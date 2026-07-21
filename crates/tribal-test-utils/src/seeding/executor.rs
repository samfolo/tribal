//! Seed executor: processes the command list accumulated by the
//! [`Seed`](super::types::Seed) builder, inserting entities via the
//! repository layer and returning a [`SeedResult`].
//!
//! Uses a reducer pattern: the main [`execute`] loop dispatches each
//! command to a dedicated handler function that updates the shared
//! [`ExecutionState`].

use std::collections::HashMap;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use indexmap::IndexMap;
use sqlx::PgConnection;
use tracing::{debug, warn};
use tribal_db::{
    EmbeddingProfileRepository, EmbeddingRepository, ItemObservationRepository,
    KnowledgeItemRepository, NewEmbedding, NewEmbeddingProfile, NewGitProject, NewItemObservation,
    NewKnowledgeItem, NewKnowledgeItemRelation, NewPrincipal, NewPromptVersion, NewReference,
    NewTagEmbedding, PgEmbeddingProfileRepository, PgEmbeddingRepository,
    PgItemObservationRepository, PgKnowledgeItemRepository, PgPrincipalRepository,
    PgProjectRepository, PgPromptVersionRepository, PgReferenceRepository, PgRelationRepository,
    PgTagEmbeddingRepository, PgTagRegistryRepository, PrincipalRepository, ProjectRepository,
    PromptVersionRepository, ReferenceRepository, RelationRepository, TagEmbeddingRepository,
    TagRegistryRepository,
};
use tribal_domain::{
    EmbeddingId, EmbeddingProfileId, EpisodeId, GitRemote, ItemObservationId, KnowledgeItemId,
    PrincipalId, ProjectId, PromptClass, PromptVersionId, ProviderKind, ReferenceId,
    RelationBatchId, RelationId, RelationKind,
};

use super::{
    embeddings::{EmbeddingGroupAssigner, make_group_embedding},
    repository::{PgSeedRepository, SeedRepository},
    types::{CommittedBatch, SeedCommand, SeedReferenceSpec, SeedResult},
};

// ---------------------------------------------------------------------------
// Execution state
// ---------------------------------------------------------------------------

/// Mutable state threaded through every command handler.
struct ExecutionState {
    // Label → ID mappings
    projects: HashMap<String, ProjectId>,
    principals: HashMap<String, PrincipalId>,
    prompt_versions: HashMap<String, PromptVersionId>,
    items: IndexMap<String, KnowledgeItemId>,

    // Accumulated ID mappings for SeedResult
    embeddings: HashMap<String, EmbeddingId>,
    references: HashMap<String, Vec<ReferenceId>>,
    observations: HashMap<String, Vec<ItemObservationId>>,

    // Embedding configuration
    embedding_model: Option<String>,
    embedding_dimensions: Option<usize>,
    embedding_group_assigner: Option<EmbeddingGroupAssigner>,
    // The genesis profile created when the embedding model is set; every seeded
    // embedding is keyed to it.
    embedding_profile_id: Option<EmbeddingProfileId>,

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
    prompt_version_command_indices: HashMap<String, usize>,
    item_command_indices: HashMap<String, usize>,
    batch_command_indices: HashMap<String, usize>,

    // Item label → project label (for relation scaffolding)
    item_projects: HashMap<String, String>,

    // Tag → deterministic embedding vector
    tag_embeddings: HashMap<String, Vec<f32>>,
}

impl ExecutionState {
    fn new() -> Self {
        Self {
            projects: HashMap::new(),
            principals: HashMap::new(),
            prompt_versions: HashMap::new(),
            items: IndexMap::new(),
            embeddings: HashMap::new(),
            references: HashMap::new(),
            observations: HashMap::new(),
            embedding_model: None,
            embedding_dimensions: None,
            embedding_group_assigner: None,
            embedding_profile_id: None,
            episodes: HashMap::new(),
            base_epoch: Utc::now().trunc_subsecs(6) - Duration::hours(24),
            accumulated_offset: Duration::zero(),
            pending_relations: Vec::new(),
            committed_batches: IndexMap::new(),
            uncommitted_relations: Vec::new(),
            project_command_indices: HashMap::new(),
            principal_command_indices: HashMap::new(),
            prompt_version_command_indices: HashMap::new(),
            item_command_indices: HashMap::new(),
            batch_command_indices: HashMap::new(),
            item_projects: HashMap::new(),
            tag_embeddings: HashMap::new(),
        }
    }

    fn virtual_time(&self) -> DateTime<Utc> {
        self.base_epoch + self.accumulated_offset
    }

    fn resolve_episode(&mut self, label: &str) -> EpisodeId {
        *self.episodes.entry(label.to_owned()).or_default()
    }
}

/// An accumulated relation awaiting commitment or final flush.
struct PendingRelation {
    source_label: String,
    kind: RelationKind,
    target_label: String,
    principal_label: String,
}

/// Item metadata collected during scope processing for implicit embedding.
struct PendingEmbedding {
    label: String,
    command_index: usize,
    group: Option<String>,
}

// ---------------------------------------------------------------------------
// Main dispatcher
// ---------------------------------------------------------------------------

/// Executes a command list against the database.
#[allow(clippy::too_many_lines)]
pub(crate) async fn execute(commands: Vec<SeedCommand>, conn: &mut PgConnection) -> SeedResult {
    let mut state = ExecutionState::new();

    validate_preamble(&commands);

    let mut i = 0;
    while i < commands.len() {
        match &commands[i] {
            SeedCommand::CreateProject {
                label,
                git_remote,
                name,
            } => {
                handle_create_project(i, label, git_remote, name, &mut state, conn).await;
                i += 1;
            }

            SeedCommand::CreatePrincipal { label, key } => {
                handle_create_principal(i, label, key, &mut state, conn).await;
                i += 1;
            }

            SeedCommand::SetEmbeddingModel { model, dimensions } => {
                handle_set_embedding_model(i, model, *dimensions, &mut state, conn).await;
                i += 1;
            }

            SeedCommand::CreatePromptVersion {
                label,
                stage,
                role,
                content_hash,
                content,
            } => {
                let new = NewPromptVersion::builder()
                    .stage(*stage)
                    .class(PromptClass::OneShot)
                    .role(*role)
                    .content_hash(content_hash.clone())
                    .content(content.clone())
                    .build();
                handle_create_prompt_version(i, label, &new, &mut state, conn).await;
                i += 1;
            }

            SeedCommand::SwitchPrincipal { label } => {
                handle_switch_principal(i, label, &state);
                i += 1;
            }

            SeedCommand::BeginProjectScope { project_label } => {
                let end_idx =
                    handle_project_scope(i, project_label, &commands, &mut state, conn).await;
                i = end_idx + 1;
            }

            SeedCommand::Relate {
                source_label,
                kind,
                target_label,
                principal_label,
            } => {
                handle_relate(
                    i,
                    source_label,
                    *kind,
                    target_label,
                    principal_label.as_deref(),
                    &mut state,
                );
                i += 1;
            }

            SeedCommand::CommitRelations { label } => {
                handle_commit_relations(i, label, &mut state, conn).await;
                i += 1;
            }

            SeedCommand::Advance { delta } => {
                handle_advance(i, *delta, &mut state);
                i += 1;
            }

            SeedCommand::DefineTag { tag } => {
                handle_define_tag(i, tag, conn).await;
                i += 1;
            }

            SeedCommand::DefineTagWithEmbedding { tag } => {
                handle_define_tag_with_embedding(i, tag, &mut state, conn).await;
                i += 1;
            }

            // These variants are only emitted inside BeginProjectScope..
            // EndProjectScope pairs and are consumed by
            // handle_project_scope. The builder API (Seed::for_project)
            // guarantees they never appear at the top level.
            SeedCommand::AddItem { .. }
            | SeedCommand::Observe { .. }
            | SeedCommand::AddReference { .. }
            | SeedCommand::AddReferenceSpec { .. }
            | SeedCommand::EndProjectScope { .. } => {
                unreachable!("seed[{i}]: command should have been consumed by scope handler");
            }
        }
    }

    // Final flush: uncommitted relations.
    if !state.pending_relations.is_empty() {
        let relation_ids = flush_relations(&mut state, conn, None).await;
        state.uncommitted_relations = relation_ids;
    }

    SeedResult {
        projects: state.projects,
        principals: state.principals,
        prompt_versions: state.prompt_versions,
        items: state.items,
        embeddings: state.embeddings,
        references: state.references,
        observations: state.observations,
        committed_batches: state.committed_batches,
        uncommitted_relations: state.uncommitted_relations,
        tag_embeddings: state.tag_embeddings,
    }
}

// ---------------------------------------------------------------------------
// Preamble validation
// ---------------------------------------------------------------------------

/// Validates that the command list contains at least one project and
/// one principal definition.
fn validate_preamble(commands: &[SeedCommand]) {
    let has_project = commands
        .iter()
        .any(|c| matches!(c, SeedCommand::CreateProject { .. }));
    let has_principal = commands
        .iter()
        .any(|c| matches!(c, SeedCommand::CreatePrincipal { .. }));

    assert!(has_project, "execute() called but no projects were defined");
    assert!(
        has_principal,
        "execute() called but no principals were defined"
    );
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

async fn handle_create_project(
    idx: usize,
    label: &str,
    git_remote: &GitRemote,
    name: &str,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    if let Some(&prev) = state.project_command_indices.get(label) {
        panic!("duplicate project label '{label}' — first defined at command {prev}");
    }
    state.project_command_indices.insert(label.to_owned(), idx);

    debug!("seed[{idx}]: CreateProject label={label:?} git_remote={git_remote:?}");

    let new_project = NewGitProject::builder()
        .remote(git_remote.clone())
        .name(name.to_owned())
        .default_branch("main".to_owned())
        .schema_version(1)
        .settings(serde_json::json!({}))
        .build();

    let project = PgProjectRepository
        .insert_git(&mut *conn, &new_project)
        .await
        .expect("seed: insert project");

    state.projects.insert(label.to_owned(), project.id());
}

async fn handle_create_principal(
    idx: usize,
    label: &str,
    key: &str,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    if let Some(&prev) = state.principal_command_indices.get(label) {
        panic!("duplicate principal label '{label}' — first defined at command {prev}");
    }
    state
        .principal_command_indices
        .insert(label.to_owned(), idx);

    debug!("seed[{idx}]: CreatePrincipal label={label:?} key={key:?}");

    let new_principal = NewPrincipal::builder()
        .principal_key(key.to_owned())
        .build();

    let principal = PgPrincipalRepository
        .insert(&mut *conn, &new_principal)
        .await
        .expect("seed: insert principal");

    state.principals.insert(label.to_owned(), principal.id());
}

async fn handle_set_embedding_model(
    idx: usize,
    model: &str,
    dimensions: usize,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    assert!(
        dimensions != 0,
        "embedding dimensions must be greater than zero"
    );

    if let (Some(existing_model), Some(existing_dims)) =
        (&state.embedding_model, state.embedding_dimensions)
    {
        if existing_model == model && existing_dims == dimensions {
            debug!("seed[{idx}]: SetEmbeddingModel (no-op, identical values)");
            return;
        }
        panic!(
            "set_embedding_model() called with conflicting values — \
             was ({existing_model}, {existing_dims}), now ({model}, {dimensions})"
        );
    }

    debug!("seed[{idx}]: SetEmbeddingModel model={model:?} dimensions={dimensions}");

    // Reuse the active profile when one already exists; the harness may have
    // seeded the genesis from init.embedding so its endpoint matches the live
    // embedding identity. Otherwise create a default-endpoint genesis, mirroring
    // first-boot provisioning. The seeded embeddings key against, and take their
    // dimension from, whichever profile is active.
    let profile = if let Some(existing) = PgEmbeddingProfileRepository
        .find_active(&mut *conn)
        .await
        .expect("seed: find active profile")
    {
        existing
    } else {
        let new_profile = NewEmbeddingProfile::builder()
            .provider_kind(ProviderKind::Ollama)
            .normalised_base_url(ProviderKind::DEFAULT_OLLAMA_BASE_URL.to_owned())
            .model(model.to_owned())
            .dimensions(u32::try_from(dimensions).expect("dimensions fit in u32"))
            .fingerprint_hash(format!("seed:{model}:{dimensions}"))
            .build();
        let inserted = PgEmbeddingProfileRepository
            .insert(&mut *conn, &new_profile)
            .await
            .expect("seed: insert genesis profile");
        PgEmbeddingProfileRepository
            .mark_complete(&mut *conn, inserted.id())
            .await
            .expect("seed: complete genesis profile");
        inserted
    };

    let dimensions = usize::try_from(profile.dimensions()).expect("dimensions fit in usize");
    state.embedding_model = Some(profile.model().to_owned());
    state.embedding_dimensions = Some(dimensions);
    state.embedding_group_assigner = Some(EmbeddingGroupAssigner::new(dimensions));
    state.embedding_profile_id = Some(profile.id());
}

fn handle_switch_principal(idx: usize, label: &str, state: &ExecutionState) {
    if !state.principals.contains_key(label) {
        let defined: Vec<_> = state.principals.keys().collect();
        panic!("unknown principal '{label}' — defined principals: {defined:?}");
    }

    debug!("seed[{idx}]: SwitchPrincipal label={label:?}");
}

async fn handle_create_prompt_version(
    idx: usize,
    label: &str,
    new: &NewPromptVersion,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    if let Some(&prev) = state.prompt_version_command_indices.get(label) {
        panic!("duplicate prompt version label '{label}' — first defined at command {prev}");
    }
    state
        .prompt_version_command_indices
        .insert(label.to_owned(), idx);

    debug!(
        "seed[{idx}]: CreatePromptVersion label={label:?} stage={:?} role={:?}",
        new.stage, new.role
    );

    let pv = PgPromptVersionRepository
        .upsert(&mut *conn, new)
        .await
        .expect("seed: upsert prompt version");

    state.prompt_versions.insert(label.to_owned(), pv.id());
}

fn handle_relate(
    idx: usize,
    source_label: &str,
    kind: RelationKind,
    target_label: &str,
    principal_label: Option<&str>,
    state: &mut ExecutionState,
) {
    assert!(
        source_label != target_label,
        "self-relation not permitted: '{source_label}' cannot relate to itself"
    );

    if !state.items.contains_key(source_label) {
        let defined: Vec<_> = state.items.keys().collect();
        panic!("relation source '{source_label}' not found — defined items: {defined:?}");
    }
    if !state.items.contains_key(target_label) {
        let defined: Vec<_> = state.items.keys().collect();
        panic!("relation target '{target_label}' not found — defined items: {defined:?}");
    }

    let principal_label = principal_label.unwrap_or_else(|| {
        panic!("relate() requires an active principal — call as_principal() first")
    });

    debug!("seed[{idx}]: Relate source={source_label:?} kind={kind:?} target={target_label:?}");

    state.pending_relations.push(PendingRelation {
        source_label: source_label.to_owned(),
        kind,
        target_label: target_label.to_owned(),
        principal_label: principal_label.to_owned(),
    });
}

async fn handle_commit_relations(
    idx: usize,
    label: &str,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    if state.pending_relations.is_empty() {
        warn!("seed[{idx}]: commit_relations({label:?}) called with no pending relations");
        return;
    }

    if let Some(&prev) = state.batch_command_indices.get(label) {
        panic!("duplicate batch label '{label}' — first committed at command {prev}");
    }
    state.batch_command_indices.insert(label.to_owned(), idx);

    debug!(
        "seed[{idx}]: CommitRelations batch_label={label:?} relation_count={}",
        state.pending_relations.len()
    );

    flush_relations(state, conn, Some(label)).await;
}

fn handle_advance(idx: usize, delta: Duration, state: &mut ExecutionState) {
    assert!(
        delta >= Duration::zero(),
        "advance() requires a non-negative duration, got {delta}"
    );

    debug!(
        "seed[{idx}]: Advance delta={delta} virtual_time={:?}",
        state.virtual_time() + delta
    );
    state.accumulated_offset += delta;
}

// ---------------------------------------------------------------------------
// Tag handlers
// ---------------------------------------------------------------------------

async fn handle_define_tag(idx: usize, tag: &str, conn: &mut PgConnection) {
    debug!("seed[{idx}]: DefineTag tag={tag:?}");

    PgTagRegistryRepository
        .batch_upsert(&mut *conn, &[tag.to_owned()])
        .await
        .expect("seed: register tag");
}

async fn handle_define_tag_with_embedding(
    idx: usize,
    tag: &str,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    let model = state.embedding_model.as_ref().unwrap_or_else(|| {
        panic!(
            "define_tag_with_embedding() requires an embedding model — \
             call set_embedding_model() first"
        );
    });
    let dimensions = state.embedding_dimensions.unwrap();
    let profile_id = state
        .embedding_profile_id
        .expect("seed: set_embedding_model() must run before tag embeddings");
    let assigner = state.embedding_group_assigner.as_mut().unwrap();

    debug!("seed[{idx}]: DefineTagWithEmbedding tag={tag:?} model={model:?}");

    PgTagRegistryRepository
        .batch_upsert(&mut *conn, &[tag.to_owned()])
        .await
        .expect("seed: register tag");

    let group = format!("__tag:{tag}");
    let (group_index, position) = assigner.assign(&group);
    let vector = make_group_embedding(group_index, position, dimensions);

    let new_tag_embedding = NewTagEmbedding::builder()
        .tag(tag.to_owned())
        .embedding_profile_id(profile_id)
        .model(model.clone())
        .embedding(vector.clone())
        .build();

    PgTagEmbeddingRepository
        .batch_upsert(&mut *conn, &[new_tag_embedding])
        .await
        .expect("seed: insert tag embedding");

    state.tag_embeddings.insert(tag.to_owned(), vector);
}

// ---------------------------------------------------------------------------
// Project scope processing
// ---------------------------------------------------------------------------

/// Processes a `BeginProjectScope..EndProjectScope` range using the
/// two-bucket partition (items, then dependents) with implicit
/// embedding on scope close.
///
/// Returns the index of the `EndProjectScope` command.
async fn handle_project_scope(
    begin_idx: usize,
    project_label: &str,
    commands: &[SeedCommand],
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) -> usize {
    if !state.projects.contains_key(project_label) {
        let defined: Vec<_> = state.projects.keys().collect();
        panic!("unknown project '{project_label}' — defined projects: {defined:?}");
    }

    debug!("seed[{begin_idx}]: BeginProjectScope project={project_label:?}");

    // Collect indices of commands within this scope.
    let mut scope_indices = Vec::new();
    let mut i = begin_idx + 1;
    while i < commands.len() {
        if matches!(
            &commands[i],
            SeedCommand::EndProjectScope { project_label: p } if p == project_label
        ) {
            break;
        }
        scope_indices.push(i);
        i += 1;
    }
    let end_idx = i;

    let vt = state.virtual_time();

    // --- Bucket 1: Items ---
    let pending_embeddings = process_scope_items(commands, &scope_indices, vt, state, conn).await;

    // --- Bucket 2: Dependents ---
    process_scope_dependents(commands, &scope_indices, project_label, vt, state, conn).await;

    // --- Implicit embedding on scope close ---
    process_scope_embeddings(&pending_embeddings, state, conn).await;

    debug!("seed[{end_idx}]: EndProjectScope project={project_label:?}");

    end_idx
}

/// Bucket 1: inserts all `AddItem` commands within the scope.
///
/// Returns [`PendingEmbedding`] metadata for items that need embedding
/// (those not marked `skip_embed`), used by the implicit embedding phase.
async fn process_scope_items(
    commands: &[SeedCommand],
    scope_indices: &[usize],
    vt: DateTime<Utc>,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) -> Vec<PendingEmbedding> {
    let repo = PgSeedRepository;
    let mut pending_embeddings = Vec::new();

    for &idx in scope_indices {
        let SeedCommand::AddItem {
            label,
            project_label: item_project_label,
            principal_label,
            spec,
        } = &commands[idx]
        else {
            continue;
        };

        if let Some(&prev) = state.item_command_indices.get(label) {
            panic!("duplicate item label '{label}' — first defined at command {prev}");
        }
        state.item_command_indices.insert(label.clone(), idx);

        let principal_label = principal_label.as_ref().unwrap_or_else(|| {
            panic!(
                "add_item() requires an active principal — \
                 call as_principal() before for_project()"
            );
        });
        let principal_id = *state.principals.get(principal_label).unwrap_or_else(|| {
            let defined: Vec<_> = state.principals.keys().collect();
            panic!("unknown principal '{principal_label}' — defined principals: {defined:?}");
        });

        let item_project_id = state.projects[item_project_label.as_str()];

        let episode_id = spec
            .episode_label
            .as_ref()
            .map(|ep| state.resolve_episode(ep));

        debug!(
            "seed[{idx}]: AddItem label={label:?} kind={:?} \
             project={item_project_label:?} principal={principal_label:?}",
            spec.kind
        );

        let new_item = NewKnowledgeItem::builder()
            .project_id(item_project_id)
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
        state
            .item_projects
            .insert(label.clone(), item_project_label.clone());

        if !spec.skip_embed {
            pending_embeddings.push(PendingEmbedding {
                label: label.clone(),
                command_index: idx,
                group: spec.embedding_group.clone(),
            });
        }

        if !spec.tags.is_empty() {
            PgTagRegistryRepository
                .batch_upsert(&mut *conn, &spec.tags)
                .await
                .expect("seed: register tags");
        }

        repo.backdate_item(conn, ki_id, vt).await;
    }

    pending_embeddings
}

/// Bucket 2: processes `Observe`, `AddReference`, and
/// `AddReferenceSpec` commands. Uses exhaustive matching to ensure
/// new command variants cannot be silently dropped.
async fn process_scope_dependents(
    commands: &[SeedCommand],
    scope_indices: &[usize],
    project_label: &str,
    vt: DateTime<Utc>,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    for &idx in scope_indices {
        match &commands[idx] {
            // Items handled in bucket 1.
            SeedCommand::AddItem { .. } => {}

            SeedCommand::Observe {
                item_label,
                principal_label,
                source_type,
            } => {
                handle_scope_observe(
                    idx,
                    item_label,
                    principal_label.as_deref(),
                    *source_type,
                    vt,
                    state,
                    conn,
                )
                .await;
            }

            SeedCommand::AddReference {
                item_label,
                project_label: ref_project_label,
                kind,
                value,
            } => {
                handle_scope_add_reference(
                    idx,
                    item_label,
                    ref_project_label,
                    *kind,
                    value,
                    state,
                    conn,
                )
                .await;
            }

            SeedCommand::AddReferenceSpec {
                item_label,
                project_label: ref_project_label,
                spec,
            } => {
                handle_scope_add_reference_spec(
                    idx,
                    item_label,
                    ref_project_label,
                    spec,
                    state,
                    conn,
                )
                .await;
            }

            // These variants cannot appear inside a project scope.
            // The builder API only emits AddItem, Observe,
            // AddReference, and AddReferenceSpec within
            // BeginProjectScope..EndProjectScope.
            SeedCommand::CreateProject { .. }
            | SeedCommand::CreatePrincipal { .. }
            | SeedCommand::SetEmbeddingModel { .. }
            | SeedCommand::CreatePromptVersion { .. }
            | SeedCommand::SwitchPrincipal { .. }
            | SeedCommand::BeginProjectScope { .. }
            | SeedCommand::EndProjectScope { .. }
            | SeedCommand::Relate { .. }
            | SeedCommand::CommitRelations { .. }
            | SeedCommand::Advance { .. }
            | SeedCommand::DefineTag { .. }
            | SeedCommand::DefineTagWithEmbedding { .. } => {
                unreachable!(
                    "seed[{idx}]: unexpected command inside project scope '{project_label}'"
                );
            }
        }
    }
}

async fn handle_scope_observe(
    idx: usize,
    item_label: &str,
    principal_label: Option<&str>,
    source_type: tribal_domain::SourceType,
    vt: DateTime<Utc>,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    let repo = PgSeedRepository;
    let ki_id = *state.items.get(item_label).unwrap_or_else(|| {
        let defined: Vec<_> = state.items.keys().collect();
        panic!("observe target '{item_label}' not found — defined items: {defined:?}");
    });

    let principal_label = principal_label.unwrap_or_else(|| {
        panic!(
            "observe() requires an active principal — \
             call as_principal() before for_project()"
        );
    });
    let principal_id = *state.principals.get(principal_label).unwrap_or_else(|| {
        let defined: Vec<_> = state.principals.keys().collect();
        panic!("unknown principal '{principal_label}' — defined principals: {defined:?}");
    });

    debug!("seed[{idx}]: Observe item={item_label:?} source_type={source_type:?}");

    let new_obs = NewItemObservation::builder()
        .knowledge_item_id(ki_id)
        .principal_id(principal_id)
        .source_type(source_type)
        .build();

    let obs = PgItemObservationRepository
        .insert(&mut *conn, &new_obs)
        .await
        .expect("seed: insert observation");

    let obs_id = obs.id();
    state
        .observations
        .entry(item_label.to_owned())
        .or_default()
        .push(obs_id);

    repo.backdate_observation(conn, obs_id, vt).await;
}

async fn handle_scope_add_reference(
    idx: usize,
    item_label: &str,
    ref_project_label: &str,
    kind: tribal_domain::ReferenceKind,
    value: &str,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    let ki_id = *state.items.get(item_label).unwrap_or_else(|| {
        let defined: Vec<_> = state.items.keys().collect();
        panic!("reference target '{item_label}' not found — defined items: {defined:?}");
    });
    let ref_project_id = state.projects[ref_project_label];

    debug!("seed[{idx}]: AddReference item={item_label:?} kind={kind:?} value={value:?}");

    let new_ref = NewReference::builder()
        .knowledge_item_id(ki_id)
        .kind(kind)
        .value(value.to_owned())
        .project_id(ref_project_id)
        .build();

    let reference = PgReferenceRepository
        .insert(&mut *conn, &new_ref)
        .await
        .expect("seed: insert reference");

    state
        .references
        .entry(item_label.to_owned())
        .or_default()
        .push(reference.id());
}

async fn handle_scope_add_reference_spec(
    idx: usize,
    item_label: &str,
    ref_project_label: &str,
    spec: &SeedReferenceSpec,
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    let ki_id = *state.items.get(item_label).unwrap_or_else(|| {
        let defined: Vec<_> = state.items.keys().collect();
        panic!("reference target '{item_label}' not found — defined items: {defined:?}");
    });
    let ref_project_id = state.projects[ref_project_label];

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
        .entry(item_label.to_owned())
        .or_default()
        .push(reference.id());
}

/// Implicit embedding on scope close: generates and inserts
/// deterministic embeddings for items collected during the item phase.
async fn process_scope_embeddings(
    pending: &[PendingEmbedding],
    state: &mut ExecutionState,
    conn: &mut PgConnection,
) {
    if pending.is_empty() {
        return;
    }

    let model = state.embedding_model.as_ref().unwrap_or_else(|| {
        panic!(
            "for_project() adds embeddable items but no embedding model set — \
             call set_embedding_model() first or mark all items skip_embed()"
        );
    });
    let dimensions = state.embedding_dimensions.unwrap();
    let profile_id = state
        .embedding_profile_id
        .expect("seed: set_embedding_model() must run before item embeddings");
    let assigner = state.embedding_group_assigner.as_mut().unwrap();

    for item in pending {
        let group = item
            .group
            .clone()
            .unwrap_or_else(|| format!("__item:{}", item.label));

        let (group_index, position) = assigner.assign(&group);
        let vector = make_group_embedding(group_index, position, dimensions);

        let ki_id = state.items[item.label.as_str()];

        debug!(
            "seed[{}]: EmbedItem label={:?} model={model:?} group={group:?}",
            item.command_index, item.label
        );

        let new_embedding = NewEmbedding::builder()
            .knowledge_item_id(ki_id)
            .embedding_profile_id(profile_id)
            .model(model.clone())
            .embedding(vector)
            .build();

        let embedding = PgEmbeddingRepository
            .insert(&mut *conn, &new_embedding)
            .await
            .expect("seed: insert embedding");

        state.embeddings.insert(item.label.clone(), embedding.id());
    }
}

// ---------------------------------------------------------------------------
// Relation flushing
// ---------------------------------------------------------------------------

/// Flushes pending relations. If `batch_label` is `Some`, creates
/// commitment scaffolding and stores the batch in `committed_batches`.
/// If `None`, relations are inserted without scaffolding (uncommitted).
///
/// Returns the inserted relation IDs.
async fn flush_relations(
    state: &mut ExecutionState,
    conn: &mut PgConnection,
    batch_label: Option<&str>,
) -> Vec<RelationId> {
    let repo = PgSeedRepository;
    let batch_id = RelationBatchId::new();
    let vt = state.virtual_time();

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

    let relation_ids: Vec<RelationId> = inserted
        .iter()
        .map(tribal_domain::KnowledgeItemRelation::id)
        .collect();

    repo.backdate_relations(conn, &relation_ids, vt).await;

    if let Some(label) = batch_label {
        let first_source = &state.pending_relations[0].source_label;
        let project_label = &state.item_projects[first_source.as_str()];
        let project_id = state.projects[project_label.as_str()];
        let principal_id = state.principals[state.pending_relations[0].principal_label.as_str()];

        let job_id = repo
            .commit_relation_batch(&mut *conn, project_id, principal_id, batch_id)
            .await;

        state.committed_batches.insert(
            label.to_owned(),
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
