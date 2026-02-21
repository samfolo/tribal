//! Types and builder API for the declarative seed infrastructure.
//!
//! The [`Seed`] builder accumulates commands as pure data. When
//! [`Seed::execute`] is called, the executor inserts entities via the
//! repository layer and returns a [`SeedResult`] with label-based
//! accessors for all inserted entity IDs.

use std::collections::HashMap;

use chrono::Duration;
use indexmap::IndexMap;
use sqlx::PgConnection;
use tribal_domain::{
    Confidence, EmbeddingId, ItemObservationId, JobId, KnowledgeItemId, KnowledgeKind, PrincipalId,
    ProjectId, ReferenceId, ReferenceKind, RelationBatchId, RelationId, RelationKind, SourceType,
};

use super::executor;

// ---------------------------------------------------------------------------
// Command model
// ---------------------------------------------------------------------------

/// Internal command representation. Accumulated by the builder, consumed
/// by the executor.
#[derive(Debug)]
pub(crate) enum SeedCommand {
    // Setup phase
    CreateProject {
        label: String,
        git_remote: String,
        name: String,
    },
    CreatePrincipal {
        label: String,
        key: String,
    },
    SetEmbeddingModel {
        model: String,
        dimensions: usize,
    },

    // Active state changes
    SwitchPrincipal {
        label: String,
    },

    // Project-scoped operations
    BeginProjectScope {
        project_label: String,
    },
    AddItem {
        label: String,
        project_label: String,
        principal_label: Option<String>,
        spec: SeedItemSpec,
    },
    Observe {
        item_label: String,
        principal_label: Option<String>,
        source_type: SourceType,
    },
    AddReference {
        item_label: String,
        project_label: String,
        kind: ReferenceKind,
        value: String,
    },
    AddReferenceSpec {
        item_label: String,
        project_label: String,
        spec: SeedReferenceSpec,
    },
    EndProjectScope {
        project_label: String,
    },

    // Seed-level operations
    Relate {
        source_label: String,
        kind: RelationKind,
        target_label: String,
        principal_label: Option<String>,
    },
    CommitRelations {
        label: String,
    },
    Advance {
        delta: Duration,
    },
}

// ---------------------------------------------------------------------------
// Item specification
// ---------------------------------------------------------------------------

/// Specification for a knowledge item (pure data, no I/O).
///
/// Constructed via the [`item`] free function and customised with
/// chainable setter methods.
#[derive(Debug, Clone)]
pub struct SeedItemSpec {
    pub(crate) kind: KnowledgeKind,
    pub(crate) content: String,
    pub(crate) tags: Vec<String>,
    pub(crate) confidence: Confidence,
    pub(crate) source_context: serde_json::Value,
    pub(crate) episode_label: Option<String>,
    pub(crate) capture_commit: Option<String>,
    pub(crate) capture_branch: Option<String>,
    pub(crate) embedding_group: Option<String>,
    pub(crate) skip_embed: bool,
}

impl SeedItemSpec {
    /// Sets the tags for this item.
    #[must_use]
    pub fn tags(mut self, tags: &[&str]) -> Self {
        self.tags = tags.iter().map(|&s| s.to_owned()).collect();
        self
    }

    /// Sets the confidence level for this item.
    #[must_use]
    pub fn confidence(mut self, confidence: Confidence) -> Self {
        self.confidence = confidence;
        self
    }

    /// Assigns this item to a named episode. Items sharing an episode
    /// label receive the same `episode_id` UUID.
    #[must_use]
    pub fn episode(mut self, label: impl Into<String>) -> Self {
        self.episode_label = Some(label.into());
        self
    }

    /// Sets the capture commit SHA for this item.
    #[must_use]
    pub fn capture_commit(mut self, sha: impl Into<String>) -> Self {
        self.capture_commit = Some(sha.into());
        self
    }

    /// Sets the capture branch for this item.
    #[must_use]
    pub fn capture_branch(mut self, branch: impl Into<String>) -> Self {
        self.capture_branch = Some(branch.into());
        self
    }

    /// Sets the source context JSON for this item.
    #[must_use]
    pub fn source_context(mut self, json: serde_json::Value) -> Self {
        self.source_context = json;
        self
    }

    /// Assigns this item to a named embedding group. Items in the same
    /// group receive deterministic vectors with high cosine similarity.
    #[must_use]
    pub fn embedding_group(mut self, label: impl Into<String>) -> Self {
        self.embedding_group = Some(label.into());
        self
    }

    /// Marks this item to skip embedding generation.
    #[must_use]
    pub fn skip_embed(mut self) -> Self {
        self.skip_embed = true;
        self
    }
}

/// Constructs a new item specification with the given kind and content.
///
/// Defaults: `confidence = Inferred`, `source_context = {}`,
/// `skip_embed = false`, all optional fields `None`.
pub fn item(kind: KnowledgeKind, content: impl Into<String>) -> SeedItemSpec {
    SeedItemSpec {
        kind,
        content: content.into(),
        tags: Vec::new(),
        confidence: Confidence::Inferred,
        source_context: serde_json::json!({}),
        episode_label: None,
        capture_commit: None,
        capture_branch: None,
        embedding_group: None,
        skip_embed: false,
    }
}

// ---------------------------------------------------------------------------
// Reference specification
// ---------------------------------------------------------------------------

/// Specification for a reference with extended fields (pure data, no I/O).
///
/// Constructed via the [`reference`] free function and customised with
/// chainable setter methods.
#[derive(Debug, Clone)]
pub struct SeedReferenceSpec {
    pub(crate) kind: ReferenceKind,
    pub(crate) value: String,
    pub(crate) description: Option<String>,
    pub(crate) commit: Option<String>,
    pub(crate) branch: Option<String>,
}

impl SeedReferenceSpec {
    /// Sets the description for this reference.
    #[must_use]
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Sets the commit SHA for this reference.
    #[must_use]
    pub fn commit(mut self, sha: impl Into<String>) -> Self {
        self.commit = Some(sha.into());
        self
    }

    /// Sets the branch for this reference.
    #[must_use]
    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }
}

/// Constructs a new reference specification with the given kind and value.
pub fn reference(kind: ReferenceKind, value: impl Into<String>) -> SeedReferenceSpec {
    SeedReferenceSpec {
        kind,
        value: value.into(),
        description: None,
        commit: None,
        branch: None,
    }
}

// ---------------------------------------------------------------------------
// Seed builder
// ---------------------------------------------------------------------------

/// Top-level seed builder. Accumulates commands as pure data.
///
/// Call [`execute`](Seed::execute) to insert all entities into the test
/// database and obtain a [`SeedResult`] for label-based ID access.
pub struct Seed {
    commands: Vec<SeedCommand>,
    current_principal: Option<String>,
}

impl Seed {
    /// Creates an empty seed builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
            current_principal: None,
        }
    }

    /// Registers a project with the given label and git remote URL.
    #[must_use]
    pub fn define_project(
        mut self,
        label: impl Into<String>,
        git_remote: impl Into<String>,
    ) -> Self {
        let label = label.into();
        let name = label.clone();
        self.commands.push(SeedCommand::CreateProject {
            label,
            git_remote: git_remote.into(),
            name,
        });
        self
    }

    /// Registers a principal with the given label and key.
    #[must_use]
    pub fn define_principal(mut self, label: impl Into<String>, key: impl Into<String>) -> Self {
        self.commands.push(SeedCommand::CreatePrincipal {
            label: label.into(),
            key: key.into(),
        });
        self
    }

    /// Sets the active embedding model and dimensions.
    ///
    /// Idempotent for identical values; panics at execute-time if called
    /// with conflicting values.
    #[must_use]
    pub fn set_embedding_model(mut self, model: impl Into<String>, dimensions: usize) -> Self {
        self.commands
            .push(SeedCommand::SetEmbeddingModel { model: model.into(), dimensions });
        self
    }

    /// Switches the active principal for subsequent operations.
    #[must_use]
    pub fn as_principal(mut self, label: impl Into<String>) -> Self {
        let label = label.into();
        self.current_principal = Some(label.clone());
        self.commands.push(SeedCommand::SwitchPrincipal { label });
        self
    }

    /// Clears the active principal. Subsequent `add_item` or `observe`
    /// calls without a re-set principal will panic at execute-time.
    #[must_use]
    pub fn clear_principal(mut self) -> Self {
        self.current_principal = None;
        self
    }

    /// Opens a project scope. Items, observations, and references
    /// declared within the closure are attributed to the named project.
    ///
    /// Declaration order within the closure is irrelevant — the executor
    /// uses a two-bucket partition to resolve forward references.
    #[must_use]
    pub fn for_project(
        mut self,
        label: impl Into<String>,
        f: impl FnOnce(&mut ProjectScope),
    ) -> Self {
        let label = label.into();
        self.commands.push(SeedCommand::BeginProjectScope {
            project_label: label.clone(),
        });

        let mut scope = ProjectScope {
            commands: &mut self.commands,
            project_label: label.clone(),
            current_principal: &mut self.current_principal,
        };
        f(&mut scope);

        self.commands.push(SeedCommand::EndProjectScope {
            project_label: label,
        });
        self
    }

    /// Declares a relation between two labelled items.
    ///
    /// Relations are accumulated until [`commit_relations`](Seed::commit_relations)
    /// is called.
    #[must_use]
    pub fn relate(
        mut self,
        source_label: impl Into<String>,
        kind: RelationKind,
        target_label: impl Into<String>,
    ) -> Self {
        self.commands.push(SeedCommand::Relate {
            source_label: source_label.into(),
            kind,
            target_label: target_label.into(),
            principal_label: self.current_principal.clone(),
        });
        self
    }

    /// Commits all pending relations as a named batch with scaffolding.
    ///
    /// No-op with a `WARN` trace event if no relations are pending.
    #[must_use]
    pub fn commit_relations(mut self, label: impl Into<String>) -> Self {
        self.commands.push(SeedCommand::CommitRelations {
            label: label.into(),
        });
        self
    }

    /// Advances the virtual clock by the given duration.
    #[must_use]
    pub fn advance(mut self, delta: Duration) -> Self {
        self.commands.push(SeedCommand::Advance { delta });
        self
    }

    /// Executes all accumulated commands against the database, inserting
    /// entities via the repository layer.
    ///
    /// This is the terminal method — it consumes the builder and returns
    /// a [`SeedResult`] for label-based ID access.
    ///
    /// # Panics
    ///
    /// Panics if any validation rule is violated (duplicate labels,
    /// unknown references, missing principal, etc.).
    pub async fn execute(self, conn: &mut PgConnection) -> SeedResult {
        executor::execute(self.commands, conn).await
    }
}

impl Default for Seed {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Project scope
// ---------------------------------------------------------------------------

/// Scoped context for operations within a project.
///
/// Created by [`Seed::for_project`]. All methods return `&mut Self`
/// for chaining within the closure.
pub struct ProjectScope<'a> {
    commands: &'a mut Vec<SeedCommand>,
    project_label: String,
    current_principal: &'a mut Option<String>,
}

impl ProjectScope<'_> {
    /// Adds a knowledge item to the current project.
    pub fn add_item(&mut self, label: impl Into<String>, spec: SeedItemSpec) -> &mut Self {
        self.commands.push(SeedCommand::AddItem {
            label: label.into(),
            project_label: self.project_label.clone(),
            principal_label: self.current_principal.clone(),
            spec,
        });
        self
    }

    /// Records an observation of an existing knowledge item.
    pub fn observe(&mut self, item_label: impl Into<String>, source_type: SourceType) -> &mut Self {
        self.commands.push(SeedCommand::Observe {
            item_label: item_label.into(),
            principal_label: self.current_principal.clone(),
            source_type,
        });
        self
    }

    /// Attaches a reference to a labelled item.
    pub fn add_reference(
        &mut self,
        item_label: impl Into<String>,
        kind: ReferenceKind,
        value: impl Into<String>,
    ) -> &mut Self {
        self.commands.push(SeedCommand::AddReference {
            item_label: item_label.into(),
            project_label: self.project_label.clone(),
            kind,
            value: value.into(),
        });
        self
    }

    /// Attaches a reference with extended fields to a labelled item.
    pub fn add_reference_spec(
        &mut self,
        item_label: impl Into<String>,
        spec: SeedReferenceSpec,
    ) -> &mut Self {
        self.commands.push(SeedCommand::AddReferenceSpec {
            item_label: item_label.into(),
            project_label: self.project_label.clone(),
            spec,
        });
        self
    }

    /// Switches the active principal within this scope.
    pub fn as_principal(&mut self, label: impl Into<String>) -> &mut Self {
        *self.current_principal = Some(label.into());
        self
    }

    /// Clears the active principal within this scope. Subsequent
    /// `add_item` or `observe` calls without a re-set principal will
    /// panic at execute-time.
    pub fn clear_principal(&mut self) -> &mut Self {
        *self.current_principal = None;
        self
    }
}

// ---------------------------------------------------------------------------
// Seed result
// ---------------------------------------------------------------------------

/// A committed relation batch with its associated scaffolding.
pub(crate) struct CommittedBatch {
    pub(crate) relation_ids: Vec<RelationId>,
    pub(crate) job_id: JobId,
    #[allow(dead_code)]
    pub(crate) batch_id: RelationBatchId,
}

/// Result of executing a seed specification.
///
/// Provides label-based access to all inserted entity IDs.
pub struct SeedResult {
    pub(crate) projects: HashMap<String, ProjectId>,
    pub(crate) principals: HashMap<String, PrincipalId>,
    pub(crate) items: IndexMap<String, KnowledgeItemId>,
    pub(crate) embeddings: HashMap<String, EmbeddingId>,
    pub(crate) references: HashMap<String, Vec<ReferenceId>>,
    pub(crate) observations: HashMap<String, Vec<ItemObservationId>>,
    pub(crate) committed_batches: IndexMap<String, CommittedBatch>,
    pub(crate) uncommitted_relations: Vec<RelationId>,
}

impl SeedResult {
    /// Returns the project ID for the given label.
    ///
    /// # Panics
    ///
    /// Panics if the label was not defined.
    pub fn project_id(&self, label: &str) -> ProjectId {
        *self.projects.get(label).unwrap_or_else(|| {
            let defined: Vec<_> = self.projects.keys().collect();
            panic!("unknown project label '{label}' — defined projects: {defined:?}");
        })
    }

    /// Returns the principal ID for the given label.
    ///
    /// # Panics
    ///
    /// Panics if the label was not defined.
    pub fn principal_id(&self, label: &str) -> PrincipalId {
        *self.principals.get(label).unwrap_or_else(|| {
            let defined: Vec<_> = self.principals.keys().collect();
            panic!("unknown principal label '{label}' — defined principals: {defined:?}");
        })
    }

    /// Returns the knowledge item ID for the given label.
    ///
    /// # Panics
    ///
    /// Panics if the label was not defined.
    pub fn item_id(&self, label: &str) -> KnowledgeItemId {
        *self.items.get(label).unwrap_or_else(|| {
            let defined: Vec<_> = self.items.keys().collect();
            panic!("unknown item label '{label}' — defined items: {defined:?}");
        })
    }

    /// Returns the embedding ID for the given item label.
    ///
    /// # Panics
    ///
    /// Panics if the item was not embedded or was marked `skip_embed()`.
    pub fn embedding_id(&self, item_label: &str) -> EmbeddingId {
        *self.embeddings.get(item_label).unwrap_or_else(|| {
            let embedded: Vec<_> = self.embeddings.keys().collect();
            panic!("no embedding for item '{item_label}' — embedded items: {embedded:?}");
        })
    }

    /// Returns all relation IDs from a named committed batch.
    ///
    /// # Panics
    ///
    /// Panics if the batch label was not defined.
    pub fn relation_ids(&self, batch_label: &str) -> &[RelationId] {
        &self
            .committed_batches
            .get(batch_label)
            .unwrap_or_else(|| {
                let defined: Vec<_> = self.committed_batches.keys().collect();
                panic!("unknown batch label '{batch_label}' — defined batches: {defined:?}");
            })
            .relation_ids
    }

    /// Returns the job ID created for a named `commit_relations()` call.
    ///
    /// # Panics
    ///
    /// Panics if the batch label was not defined.
    pub fn batch_job_id(&self, batch_label: &str) -> JobId {
        self.committed_batches
            .get(batch_label)
            .unwrap_or_else(|| {
                let defined: Vec<_> = self.committed_batches.keys().collect();
                panic!("unknown batch label '{batch_label}' — defined batches: {defined:?}");
            })
            .job_id
    }

    /// Returns all reference IDs for a given item label, in insertion
    /// order. Returns an empty slice if the item has no references.
    pub fn reference_ids(&self, item_label: &str) -> &[ReferenceId] {
        self.references.get(item_label).map_or(&[], Vec::as_slice)
    }

    /// Returns all observation IDs for a given item label, in insertion
    /// order. Returns an empty slice if the item has no observations.
    pub fn observation_ids(&self, item_label: &str) -> &[ItemObservationId] {
        self.observations.get(item_label).map_or(&[], Vec::as_slice)
    }

    /// Returns all item labels in insertion order.
    pub fn item_labels(&self) -> Vec<String> {
        self.items.keys().cloned().collect()
    }

    /// Returns all relation IDs that were flushed without commitment
    /// scaffolding.
    pub fn uncommitted_relation_ids(&self) -> &[RelationId] {
        &self.uncommitted_relations
    }

    /// Returns the number of committed relation batches.
    pub fn batch_count(&self) -> usize {
        self.committed_batches.len()
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed_item_spec_defaults() {
        let spec = item(KnowledgeKind::Fact, "test content");
        assert_eq!(spec.kind, KnowledgeKind::Fact);
        assert_eq!(spec.content, "test content");
        assert!(spec.tags.is_empty());
        assert_eq!(spec.confidence, Confidence::Inferred);
        assert_eq!(spec.source_context, serde_json::json!({}));
        assert!(spec.episode_label.is_none());
        assert!(spec.capture_commit.is_none());
        assert!(spec.capture_branch.is_none());
        assert!(spec.embedding_group.is_none());
        assert!(!spec.skip_embed);
    }

    #[test]
    fn test_seed_item_spec_chaining() {
        let spec = item(KnowledgeKind::Heuristic, "test")
            .tags(&["a", "b"])
            .confidence(Confidence::Verified)
            .episode("ep-1")
            .capture_commit("abc123")
            .capture_branch("main")
            .source_context(serde_json::json!({"key": "val"}))
            .embedding_group("group-a")
            .skip_embed();

        assert_eq!(spec.tags, vec!["a", "b"]);
        assert_eq!(spec.confidence, Confidence::Verified);
        assert_eq!(spec.episode_label.as_deref(), Some("ep-1"));
        assert_eq!(spec.capture_commit.as_deref(), Some("abc123"));
        assert_eq!(spec.capture_branch.as_deref(), Some("main"));
        assert_eq!(spec.source_context, serde_json::json!({"key": "val"}));
        assert_eq!(spec.embedding_group.as_deref(), Some("group-a"));
        assert!(spec.skip_embed);
    }

    #[test]
    fn test_seed_reference_spec_defaults() {
        let spec = reference(ReferenceKind::FilePath, "//src/main.rs");
        assert_eq!(spec.kind, ReferenceKind::FilePath);
        assert_eq!(spec.value, "//src/main.rs");
        assert!(spec.description.is_none());
        assert!(spec.commit.is_none());
        assert!(spec.branch.is_none());
    }

    #[test]
    fn test_seed_reference_spec_chaining() {
        let spec = reference(ReferenceKind::Url, "https://example.com")
            .description("example site")
            .commit("abc123")
            .branch("main");

        assert_eq!(spec.description.as_deref(), Some("example site"));
        assert_eq!(spec.commit.as_deref(), Some("abc123"));
        assert_eq!(spec.branch.as_deref(), Some("main"));
    }

    #[test]
    fn test_seed_builder_accumulates_commands() {
        let seed = Seed::new()
            .define_project("proj", "git@example.com:test.git")
            .define_principal("user", "user:key");

        assert_eq!(seed.commands.len(), 2);
    }

    #[test]
    fn test_seed_for_project_accumulates_scope_commands() {
        let seed = Seed::new()
            .define_project("proj", "git@example.com:test.git")
            .define_principal("user", "user:key")
            .set_embedding_model("model", 768)
            .as_principal("user")
            .for_project("proj", |store| {
                store
                    .add_item("item-1", item(KnowledgeKind::Fact, "content"))
                    .add_reference("item-1", ReferenceKind::FilePath, "//file.rs");
            });

        // define_project + define_principal + set_embedding_model +
        // switch_principal + begin_scope + add_item + add_reference +
        // end_scope = 8
        assert_eq!(seed.commands.len(), 8);
    }
}
