//! Seed executor: processes the command list accumulated by the
//! [`Seed`](super::types::Seed) builder, inserting entities via the
//! repository layer and returning a [`SeedResult`].

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use chrono::{DateTime, Duration, SubsecRound, Utc};
use indexmap::IndexMap;
use sqlx::PgConnection;
use tracing::{debug, warn};
use tribal_db::{
    EmbeddingRepository, ItemObservationRepository, KnowledgeItemRepository, NewEmbedding,
    NewItemObservation, NewKnowledgeItem, NewKnowledgeItemRelation, NewPrincipal, NewProject,
    NewReference, PgEmbeddingRepository, PgItemObservationRepository, PgKnowledgeItemRepository,
    PgPrincipalRepository, PgProjectRepository, PgReferenceRepository, PgRelationRepository,
    PgTagRegistryRepository, PrincipalRepository, ProjectRepository, ReferenceRepository,
    RelationRepository, TagRegistryRepository,
};
use tribal_domain::{
    EmbeddingId, EpisodeId, ItemObservationId, JobId, KnowledgeItemId, PrincipalId, ProjectId,
    ReferenceId, RelationBatchId, RelationId, RelationKind,
};

use super::types::{CommittedBatch, SeedCommand, SeedItemSpec, SeedResult};

// ---------------------------------------------------------------------------
// Deterministic embedding generation
// ---------------------------------------------------------------------------

/// Assigns embedding group labels to dominant vector dimensions.
struct EmbeddingGroupAssigner {
    /// Label → (dominant dimension index, next position within group).
    groups: HashMap<String, (usize, usize)>,
    dimensions: usize,
}

impl EmbeddingGroupAssigner {
    fn new(dimensions: usize) -> Self {
        Self {
            groups: HashMap::new(),
            dimensions,
        }
    }

    /// Returns `(dimension_index, position_in_group)` for the given
    /// group label. The dimension is determined by hashing the label;
    /// the position increments on each call within the same group.
    fn assign(&mut self, group: &str) -> (usize, usize) {
        let dims = self.dimensions;
        let entry = self
            .groups
            .entry(group.to_owned())
            .or_insert_with(|| {
                let mut hasher = DefaultHasher::new();
                group.hash(&mut hasher);
                let dim_index = (hasher.finish() as usize) % dims;
                (dim_index, 0)
            });
        let position = entry.1;
        entry.1 += 1;
        (entry.0, position)
    }
}

/// Generates a deterministic embedding vector for an item.
///
/// Position 0 produces the canonical basis vector for the group's
/// dominant dimension. Subsequent positions add small perturbations
/// for deterministic search ordering within the group.
fn make_group_embedding(
    group_index: usize,
    position_in_group: usize,
    dimensions: usize,
) -> Vec<f32> {
    let mut v = vec![0.0f32; dimensions];
    let dominant = group_index % dimensions;
    v[dominant] = 1.0;
    if position_in_group > 0 {
        let noise_dim = (dominant + position_in_group) % dimensions;
        if noise_dim != dominant {
            v[noise_dim] = 0.1 / (position_in_group as f32);
        }
    }
    v
}

// ---------------------------------------------------------------------------
// Commitment scaffolding
// ---------------------------------------------------------------------------

/// Creates commitment scaffolding for a relation batch: a
/// `prompt_version` row and a completed `job` row with
/// `committed_batch_id = batch_id`.
///
/// Returns the [`JobId`] of the created job. Intended for test setup
/// where relations need to appear committed without a full pipeline
/// run.
pub async fn commit_relation_batch(
    conn: &mut PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
    batch_id: RelationBatchId,
) -> JobId {
    let content_hash = format!("{:064x}", uuid::Uuid::new_v4().as_u128());

    let prompt_version_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO prompt_versions (stage, content_hash, content) \
         VALUES ('extraction', $1, 'test') RETURNING id",
    )
    .bind(&content_hash)
    .fetch_one(&mut *conn)
    .await
    .expect("seed: insert prompt_version");

    let job_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO jobs \
         (project_id, principal_id, source_context, status, outcome, \
          committed_batch_id, extraction_prompt_version_id, \
          triage_prompt_version_id, relation_prompt_version_id) \
         VALUES ($1, $2, $3, 'completed', 'success', $4, $5, $5, $5) \
         RETURNING id",
    )
    .bind(project_id.inner())
    .bind(principal_id.inner())
    .bind(serde_json::json!({}))
    .bind(batch_id.inner())
    .bind(prompt_version_id)
    .fetch_one(&mut *conn)
    .await
    .expect("seed: insert job");

    JobId::from(job_id)
}

// ---------------------------------------------------------------------------
// Backdating helpers
// ---------------------------------------------------------------------------

async fn backdate_item(conn: &mut PgConnection, id: KnowledgeItemId, ts: DateTime<Utc>) {
    sqlx::query("UPDATE knowledge_items SET created_at = $1 WHERE id = $2")
        .bind(ts)
        .bind(id.inner())
        .execute(&mut *conn)
        .await
        .expect("seed: backdate item");
}

async fn backdate_observation(
    conn: &mut PgConnection,
    id: ItemObservationId,
    ts: DateTime<Utc>,
) {
    sqlx::query("UPDATE item_observations SET observed_at = $1 WHERE id = $2")
        .bind(ts)
        .bind(id.inner())
        .execute(&mut *conn)
        .await
        .expect("seed: backdate observation");
}

async fn backdate_relations(conn: &mut PgConnection, ids: &[RelationId], ts: DateTime<Utc>) {
    if ids.is_empty() {
        return;
    }
    let raw_ids: Vec<&uuid::Uuid> = ids.iter().map(RelationId::inner).collect();
    sqlx::query("UPDATE knowledge_item_relations SET created_at = $1 WHERE id = ANY($2)")
        .bind(ts)
        .bind(&raw_ids)
        .execute(&mut *conn)
        .await
        .expect("seed: backdate relations");
}

// ---------------------------------------------------------------------------
// Pending relation
// ---------------------------------------------------------------------------

struct PendingRelation {
    source_label: String,
    kind: RelationKind,
    target_label: String,
    principal_label: String,
}

// ---------------------------------------------------------------------------
// Execution state
// ---------------------------------------------------------------------------

struct ExecutionState {
    // Label → ID mappings
    projects: HashMap<String, ProjectId>,
    principals: HashMap<String, PrincipalId>,
    items: IndexMap<String, KnowledgeItemId>,

    // Accumulated ID mappings for SeedResult
    embeddings: HashMap<String, EmbeddingId>,
    references: HashMap<String, Vec<ReferenceId>>,
    observations: HashMap<String, Vec<ItemObservationId>>,

    // Cursor state
    current_principal: Option<String>,

    // Embedding configuration
    embedding_model: Option<String>,
    embedding_dimensions: Option<usize>,
    embedding_group_assigner: Option<EmbeddingGroupAssigner>,

    // Episode label → generated UUID
    episodes: HashMap<String, EpisodeId>,

    // Virtual clock
    base_epoch: DateTime<Utc>,
    accumulated_offset: Duration,

    // Pending relation batch
    pending_relations: Vec<PendingRelation>,

    // Committed batch labels → batch info
    committed_batches: IndexMap<String, CommittedBatch>,

    // Uncommitted relation IDs
    uncommitted_relations: Vec<RelationId>,

    // Command indices for duplicate-label diagnostics
    project_command_indices: HashMap<String, usize>,
    principal_command_indices: HashMap<String, usize>,
    item_command_indices: HashMap<String, usize>,
    batch_command_indices: HashMap<String, usize>,

    // Item label → project label (for relation scaffolding)
    item_projects: HashMap<String, String>,
}

impl ExecutionState {
    fn new() -> Self {
        Self {
            projects: HashMap::new(),
            principals: HashMap::new(),
            items: IndexMap::new(),
            embeddings: HashMap::new(),
            references: HashMap::new(),
            observations: HashMap::new(),
            current_principal: None,
            embedding_model: None,
            embedding_dimensions: None,
            embedding_group_assigner: None,
            episodes: HashMap::new(),
            base_epoch: Utc::now().trunc_subsecs(6) - Duration::hours(24),
            accumulated_offset: Duration::zero(),
            pending_relations: Vec::new(),
            committed_batches: IndexMap::new(),
            uncommitted_relations: Vec::new(),
            project_command_indices: HashMap::new(),
            principal_command_indices: HashMap::new(),
            item_command_indices: HashMap::new(),
            batch_command_indices: HashMap::new(),
            item_projects: HashMap::new(),
        }
    }

    fn virtual_time(&self) -> DateTime<Utc> {
        self.base_epoch + self.accumulated_offset
    }

    fn resolve_episode(&mut self, label: &str) -> EpisodeId {
        *self
            .episodes
            .entry(label.to_owned())
            .or_insert_with(EpisodeId::new)
    }
}

// ---------------------------------------------------------------------------
// Main executor
// ---------------------------------------------------------------------------

/// Executes a command list against the database.
pub(crate) async fn execute(commands: Vec<SeedCommand>, conn: &mut PgConnection) -> SeedResult {
    let mut state = ExecutionState::new();

    // Pre-scan validation: at least one project and one principal.
    let has_project = commands
        .iter()
        .any(|c| matches!(c, SeedCommand::CreateProject { .. }));
    let has_principal = commands
        .iter()
        .any(|c| matches!(c, SeedCommand::CreatePrincipal { .. }));

    if !has_project {
        panic!("execute() called but no projects were defined");
    }
    if !has_principal {
        panic!("execute() called but no principals were defined");
    }

    let mut i = 0;
    while i < commands.len() {
        match &commands[i] {
            SeedCommand::CreateProject {
                label,
                git_remote,
                name,
            } => {
                if let Some(&prev) = state.project_command_indices.get(label) {
                    panic!(
                        "duplicate project label '{label}' — first defined at command {prev}"
                    );
                }
                state.project_command_indices.insert(label.clone(), i);

                debug!("seed[{i}]: CreateProject label={label:?} git_remote={git_remote:?}");

                let new_project = NewProject::builder()
                    .git_remote(git_remote.clone())
                    .name(name.clone())
                    .default_branch("main".to_owned())
                    .schema_version(1)
                    .settings(serde_json::json!({}))
                    .build();

                let project = PgProjectRepository
                    .insert(&mut *conn, &new_project)
                    .await
                    .expect("seed: insert project");

                state.projects.insert(label.clone(), project.id());
                i += 1;
            }

            SeedCommand::CreatePrincipal { label, key } => {
                if let Some(&prev) = state.principal_command_indices.get(label) {
                    panic!(
                        "duplicate principal label '{label}' — first defined at command {prev}"
                    );
                }
                state.principal_command_indices.insert(label.clone(), i);

                debug!("seed[{i}]: CreatePrincipal label={label:?} key={key:?}");

                let new_principal = NewPrincipal::builder()
                    .principal_key(key.clone())
                    .build();

                let principal = PgPrincipalRepository
                    .insert(&mut *conn, &new_principal)
                    .await
                    .expect("seed: insert principal");

                state.principals.insert(label.clone(), principal.id());
                i += 1;
            }

            SeedCommand::SetEmbeddingModel { model, dimensions } => {
                let dimensions = *dimensions;

                if dimensions == 0 {
                    panic!("embedding dimensions must be greater than zero");
                }

                if let (Some(existing_model), Some(existing_dims)) =
                    (&state.embedding_model, state.embedding_dimensions)
                {
                    if existing_model == model && existing_dims == dimensions {
                        // Idempotent — no-op.
                        debug!("seed[{i}]: SetEmbeddingModel (no-op, identical values)");
                        i += 1;
                        continue;
                    }
                    panic!(
                        "set_embedding_model() called with conflicting values — \
                         was ({existing_model}, {existing_dims}), \
                         now ({model}, {dimensions})"
                    );
                }

                debug!("seed[{i}]: SetEmbeddingModel model={model:?} dimensions={dimensions}");

                state.embedding_model = Some(model.clone());
                state.embedding_dimensions = Some(dimensions);
                state.embedding_group_assigner = Some(EmbeddingGroupAssigner::new(dimensions));
                i += 1;
            }

            SeedCommand::SwitchPrincipal { label } => {
                if !state.principals.contains_key(label) {
                    let defined: Vec<_> = state.principals.keys().collect();
                    panic!(
                        "unknown principal '{label}' — defined principals: {defined:?}"
                    );
                }

                debug!("seed[{i}]: SwitchPrincipal label={label:?}");
                state.current_principal = Some(label.clone());
                i += 1;
            }

            SeedCommand::BeginProjectScope { project_label } => {
                if !state.projects.contains_key(project_label) {
                    let defined: Vec<_> = state.projects.keys().collect();
                    panic!(
                        "unknown project '{project_label}' — defined projects: {defined:?}"
                    );
                }

                debug!("seed[{i}]: BeginProjectScope project={project_label:?}");

                // Collect commands until matching EndProjectScope.
                let scope_start = i;
                let mut scope_commands = Vec::new();
                i += 1;
                while i < commands.len() {
                    if matches!(
                        &commands[i],
                        SeedCommand::EndProjectScope { project_label: p } if p == project_label
                    ) {
                        break;
                    }
                    scope_commands.push(i);
                    i += 1;
                }

                process_project_scope(
                    &commands,
                    &scope_commands,
                    project_label,
                    scope_start,
                    &mut state,
                    conn,
                )
                .await;

                debug!("seed[{i}]: EndProjectScope project={project_label:?}");

                // Skip past EndProjectScope.
                i += 1;
            }

            SeedCommand::Relate {
                source_label,
                kind,
                target_label,
            } => {
                if source_label == target_label {
                    panic!(
                        "self-relation not permitted: '{source_label}' cannot relate to itself"
                    );
                }

                if !state.items.contains_key(source_label) {
                    let defined: Vec<_> = state.items.keys().collect();
                    panic!(
                        "relation source '{source_label}' not found — defined items: {defined:?}"
                    );
                }
                if !state.items.contains_key(target_label) {
                    let defined: Vec<_> = state.items.keys().collect();
                    panic!(
                        "relation target '{target_label}' not found — defined items: {defined:?}"
                    );
                }

                let principal_label = state.current_principal.clone().expect(
                    "relate() requires an active principal — call as_principal() first",
                );

                debug!(
                    "seed[{i}]: Relate source={source_label:?} kind={kind:?} target={target_label:?}"
                );

                state.pending_relations.push(PendingRelation {
                    source_label: source_label.clone(),
                    kind: *kind,
                    target_label: target_label.clone(),
                    principal_label,
                });
                i += 1;
            }

            SeedCommand::CommitRelations { label } => {
                if state.pending_relations.is_empty() {
                    warn!("seed[{i}]: commit_relations({label:?}) called with no pending relations");
                    i += 1;
                    continue;
                }

                if let Some(&prev) = state.batch_command_indices.get(label) {
                    panic!(
                        "duplicate batch label '{label}' — first committed at command {prev}"
                    );
                }
                state.batch_command_indices.insert(label.clone(), i);

                debug!(
                    "seed[{i}]: CommitRelations batch_label={label:?} relation_count={}",
                    state.pending_relations.len()
                );

                let relation_ids =
                    flush_relations(&mut state, conn, true).await;

                let batch = state.committed_batches.get(label).expect(
                    "committed batch should have been stored by flush_relations",
                );
                let _ = batch; // Suppress unused warning; batch is already stored.

                drop(relation_ids);
                i += 1;
            }

            SeedCommand::Advance { delta } => {
                if *delta < Duration::zero() {
                    panic!(
                        "advance() requires a non-negative duration, got {delta}"
                    );
                }

                debug!(
                    "seed[{i}]: Advance delta={delta} virtual_time={:?}",
                    state.virtual_time() + *delta
                );
                state.accumulated_offset = state.accumulated_offset + *delta;
                i += 1;
            }

            // These are consumed by BeginProjectScope handler.
            SeedCommand::AddItem { .. }
            | SeedCommand::Observe { .. }
            | SeedCommand::AddReference { .. }
            | SeedCommand::AddReferenceSpec { .. }
            | SeedCommand::EndProjectScope { .. } => {
                unreachable!(
                    "seed[{i}]: command should have been consumed by scope handler"
                );
            }
        }
    }

    // Final flush: uncommitted relations.
    if !state.pending_relations.is_empty() {
        let relation_ids = flush_relations(&mut state, conn, false).await;
        state.uncommitted_relations = relation_ids;
    }

    // Build SeedResult.
    SeedResult {
        projects: state.projects,
        principals: state.principals,
        items: state.items,
        embeddings: state.embeddings,
        references: state.references,
        observations: state.observations,
        committed_batches: state.committed_batches,
        uncommitted_relations: state.uncommitted_relations,
    }
}

// ---------------------------------------------------------------------------
// Scope processing
// ---------------------------------------------------------------------------

/// Processes a project scope using the two-bucket partition:
/// 1. Items (with tag registration and backdating)
/// 2. Dependents (observations, references)
/// Then implicit embedding on scope close.
async fn process_project_scope(
    commands: &[SeedCommand],
    scope_indices: &[usize],
    project_label: &str,
    scope_start: usize,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    let project_id = state.projects[project_label];
    let vt = state.virtual_time();

    // --- Bucket 1: Items ---
    let mut scope_item_labels: Vec<String> = Vec::new();

    for &idx in scope_indices {
        if let SeedCommand::AddItem {
            label,
            principal_label,
            spec,
            ..
        } = &commands[idx]
        {
            // Validate no duplicate item label.
            if let Some(&prev) = state.item_command_indices.get(label) {
                panic!(
                    "duplicate item label '{label}' — first defined at command {prev}"
                );
            }
            state.item_command_indices.insert(label.clone(), idx);

            // Validate principal.
            let principal_label = principal_label.as_ref().unwrap_or_else(|| {
                panic!(
                    "add_item() requires an active principal — \
                     call as_principal() before for_project()"
                );
            });
            let principal_id = *state.principals.get(principal_label).unwrap_or_else(|| {
                let defined: Vec<_> = state.principals.keys().collect();
                panic!(
                    "unknown principal '{principal_label}' — defined principals: {defined:?}"
                );
            });

            // Resolve episode.
            let episode_id = spec
                .episode_label
                .as_ref()
                .map(|ep| state.resolve_episode(ep));

            debug!(
                "seed[{idx}]: AddItem label={label:?} kind={:?} project={project_label:?} principal={principal_label:?}",
                spec.kind
            );

            let new_item = NewKnowledgeItem::builder()
                .project_id(project_id)
                .principal_id(principal_id)
                .kind(spec.kind)
                .content(spec.content.clone())
                .tags(spec.tags.clone())
                .confidence(spec.confidence)
                .source_context(spec.source_context.clone())
                .episode_id(episode_id)
                .capture_commit(spec.capture_commit.clone())
                .capture_branch(spec.capture_branch.clone())
                .build();

            let item = PgKnowledgeItemRepository
                .insert(&mut *conn, &new_item)
                .await
                .expect("seed: insert item");

            let ki_id = item.id();
            state.items.insert(label.clone(), ki_id);
            state.item_projects.insert(label.clone(), project_label.to_owned());
            scope_item_labels.push(label.clone());

            // Auto-register tags.
            if !spec.tags.is_empty() {
                PgTagRegistryRepository
                    .batch_upsert(&mut *conn, &spec.tags)
                    .await
                    .expect("seed: register tags");
            }

            // Backdate created_at.
            backdate_item(conn, ki_id, vt).await;
        }
    }

    // --- Bucket 2: Dependents ---
    for &idx in scope_indices {
        match &commands[idx] {
            SeedCommand::Observe {
                item_label,
                principal_label,
                source_type,
            } => {
                let ki_id = *state.items.get(item_label).unwrap_or_else(|| {
                    let defined: Vec<_> = state.items.keys().collect();
                    panic!(
                        "observe target '{item_label}' not found — defined items: {defined:?}"
                    );
                });

                let principal_label = principal_label.as_ref().unwrap_or_else(|| {
                    panic!(
                        "observe() requires an active principal — \
                         call as_principal() before for_project()"
                    );
                });
                let principal_id =
                    *state.principals.get(principal_label).unwrap_or_else(|| {
                        let defined: Vec<_> = state.principals.keys().collect();
                        panic!(
                            "unknown principal '{principal_label}' — \
                             defined principals: {defined:?}"
                        );
                    });

                debug!("seed[{idx}]: Observe item={item_label:?} source_type={source_type:?}");

                let new_obs = NewItemObservation::builder()
                    .knowledge_item_id(ki_id)
                    .principal_id(principal_id)
                    .source_type(*source_type)
                    .build();

                let obs = PgItemObservationRepository
                    .insert(&mut *conn, &new_obs)
                    .await
                    .expect("seed: insert observation");

                let obs_id = obs.id();
                state
                    .observations
                    .entry(item_label.clone())
                    .or_default()
                    .push(obs_id);

                // Backdate observed_at.
                backdate_observation(conn, obs_id, vt).await;
            }

            SeedCommand::AddReference {
                item_label,
                project_label: ref_project_label,
                kind,
                value,
            } => {
                let ki_id = *state.items.get(item_label).unwrap_or_else(|| {
                    let defined: Vec<_> = state.items.keys().collect();
                    panic!(
                        "reference target '{item_label}' not found — \
                         defined items: {defined:?}"
                    );
                });
                let ref_project_id = state.projects[ref_project_label.as_str()];

                debug!(
                    "seed[{idx}]: AddReference item={item_label:?} kind={kind:?} value={value:?}"
                );

                let new_ref = NewReference::builder()
                    .knowledge_item_id(ki_id)
                    .kind(*kind)
                    .value(value.clone())
                    .project_id(ref_project_id)
                    .build();

                let reference = PgReferenceRepository
                    .insert(&mut *conn, &new_ref)
                    .await
                    .expect("seed: insert reference");

                state
                    .references
                    .entry(item_label.clone())
                    .or_default()
                    .push(reference.id());
            }

            SeedCommand::AddReferenceSpec {
                item_label,
                project_label: ref_project_label,
                spec,
            } => {
                let ki_id = *state.items.get(item_label).unwrap_or_else(|| {
                    let defined: Vec<_> = state.items.keys().collect();
                    panic!(
                        "reference target '{item_label}' not found — \
                         defined items: {defined:?}"
                    );
                });
                let ref_project_id = state.projects[ref_project_label.as_str()];

                debug!(
                    "seed[{idx}]: AddReferenceSpec item={item_label:?} kind={:?} value={:?}",
                    spec.kind, spec.value
                );

                let new_ref = NewReference::builder()
                    .knowledge_item_id(ki_id)
                    .kind(spec.kind)
                    .value(spec.value.clone())
                    .description(spec.description.clone())
                    .project_id(ref_project_id)
                    .commit(spec.commit.clone())
                    .branch(spec.branch.clone())
                    .build();

                let reference = PgReferenceRepository
                    .insert(&mut *conn, &new_ref)
                    .await
                    .expect("seed: insert reference");

                state
                    .references
                    .entry(item_label.clone())
                    .or_default()
                    .push(reference.id());
            }

            // Items were handled in bucket 1; skip everything else.
            _ => {}
        }
    }

    // --- Implicit embedding on scope close ---
    let embeddable: Vec<(String, SeedItemSpec)> = scope_item_labels
        .iter()
        .filter_map(|label| {
            let idx = scope_indices.iter().find(|&&idx| {
                matches!(
                    &commands[idx],
                    SeedCommand::AddItem { label: l, .. } if l == label
                )
            })?;
            if let SeedCommand::AddItem { spec, .. } = &commands[*idx] {
                if !spec.skip_embed {
                    return Some((label.clone(), spec.clone()));
                }
            }
            None
        })
        .collect();

    if !embeddable.is_empty() {
        let model = state.embedding_model.as_ref().unwrap_or_else(|| {
            panic!(
                "for_project() adds embeddable items but no embedding model set — \
                 call set_embedding_model() first or mark all items skip_embed()"
            );
        });
        let dimensions = state.embedding_dimensions.unwrap();
        let assigner = state.embedding_group_assigner.as_mut().unwrap();

        for (label, spec) in &embeddable {
            let group = spec
                .embedding_group
                .clone()
                .unwrap_or_else(|| format!("__item:{label}"));

            let (group_index, position) = assigner.assign(&group);
            let vector = make_group_embedding(group_index, position, dimensions);

            let ki_id = state.items[label.as_str()];

            debug!(
                "seed[{scope_start}]: EmbedItem label={label:?} model={model:?} group={group:?}"
            );

            let new_embedding = NewEmbedding::builder()
                .knowledge_item_id(ki_id)
                .model(model.clone())
                .dimensions(u32::try_from(dimensions).expect("dimensions fit in u32"))
                .embedding(vector)
                .build();

            let embedding = PgEmbeddingRepository
                .insert(&mut *conn, &new_embedding)
                .await
                .expect("seed: insert embedding");

            state.embeddings.insert(label.clone(), embedding.id());
        }
    }
}

// ---------------------------------------------------------------------------
// Relation flushing
// ---------------------------------------------------------------------------

/// Flushes pending relations. If `commit` is true, creates commitment
/// scaffolding and stores the batch in `committed_batches` under the
/// label from the most recent `CommitRelations` command.
///
/// Returns the relation IDs.
async fn flush_relations(
    state: &mut ExecutionState,
    conn: &mut PgConnection,
    commit: bool,
) -> Vec<RelationId> {
    let batch_id = RelationBatchId::new();
    let vt = state.virtual_time();

    // Build NewKnowledgeItemRelation structs.
    let new_relations: Vec<NewKnowledgeItemRelation> = state
        .pending_relations
        .iter()
        .map(|pr| {
            let source_id = state.items[pr.source_label.as_str()];
            let target_id = state.items[pr.target_label.as_str()];
            let principal_id = state.principals[pr.principal_label.as_str()];

            NewKnowledgeItemRelation::builder()
                .relation_batch_id(batch_id)
                .source_id(source_id)
                .target_id(target_id)
                .relation_type(pr.kind)
                .principal_id(principal_id)
                .build()
        })
        .collect();

    let inserted = PgRelationRepository
        .batch_insert(&mut *conn, &new_relations)
        .await
        .expect("seed: batch insert relations");

    let relation_ids: Vec<RelationId> = inserted.iter().map(|r| r.id()).collect();

    // Backdate all relations.
    backdate_relations(conn, &relation_ids, vt).await;

    if commit {
        // Use first relation's source item's project for scaffolding.
        let first_source = &state.pending_relations[0].source_label;
        let project_label = &state.item_projects[first_source.as_str()];
        let project_id = state.projects[project_label.as_str()];
        let principal_id =
            state.principals[state.pending_relations[0].principal_label.as_str()];

        let job_id =
            commit_relation_batch(&mut *conn, project_id, principal_id, batch_id).await;

        // Find the batch label from the most recent CommitRelations command index.
        let batch_label = state
            .batch_command_indices
            .iter()
            .max_by_key(|(_, &idx)| idx)
            .map(|(label, _)| label.clone())
            .expect("commit=true but no batch label recorded");

        state.committed_batches.insert(
            batch_label,
            CommittedBatch {
                relation_ids: relation_ids.clone(),
                job_id,
                batch_id,
            },
        );
    }

    state.pending_relations.clear();
    relation_ids
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_group_embedding_canonical_vector() {
        let v = make_group_embedding(5, 0, 768);
        assert_eq!(v.len(), 768);
        assert!((v[5] - 1.0).abs() < f32::EPSILON);
        for (i, &val) in v.iter().enumerate() {
            if i != 5 {
                assert!((val - 0.0).abs() < f32::EPSILON, "dim {i} should be 0.0");
            }
        }
    }

    #[test]
    fn test_make_group_embedding_perturbation() {
        let v = make_group_embedding(5, 1, 768);
        assert!((v[5] - 1.0).abs() < f32::EPSILON);
        // Noise at dim (5 + 1) % 768 = 6.
        assert!((v[6] - 0.1).abs() < f32::EPSILON);
    }

    #[test]
    fn test_make_group_embedding_increasing_perturbation_reduces_similarity() {
        let v0 = make_group_embedding(5, 0, 768);
        let v1 = make_group_embedding(5, 1, 768);
        let v2 = make_group_embedding(5, 2, 768);

        let cos_01 = cosine_similarity(&v0, &v1);
        let cos_02 = cosine_similarity(&v0, &v2);

        assert!(
            cos_01 > cos_02,
            "position 1 ({cos_01}) should be more similar to canonical than position 2 ({cos_02})"
        );
    }

    #[test]
    fn test_embedding_group_assigner_same_group_same_dim() {
        let mut assigner = EmbeddingGroupAssigner::new(768);
        let (dim_a, pos_a) = assigner.assign("group-a");
        let (dim_b, pos_b) = assigner.assign("group-a");
        assert_eq!(dim_a, dim_b);
        assert_eq!(pos_a, 0);
        assert_eq!(pos_b, 1);
    }

    #[test]
    fn test_embedding_group_assigner_different_groups() {
        let mut assigner = EmbeddingGroupAssigner::new(768);
        let (dim_a, _) = assigner.assign("caching");
        let (dim_b, _) = assigner.assign("design");
        // With 768 dimensions, hash collision is negligible.
        assert_ne!(
            dim_a, dim_b,
            "different groups should map to different dimensions (hash collision unlikely)"
        );
    }

    #[test]
    fn test_embedding_group_assigner_auto_position_increment() {
        let mut assigner = EmbeddingGroupAssigner::new(768);
        let (_, p0) = assigner.assign("x");
        let (_, p1) = assigner.assign("x");
        let (_, p2) = assigner.assign("x");
        assert_eq!(p0, 0);
        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        dot / (norm_a * norm_b)
    }
}
