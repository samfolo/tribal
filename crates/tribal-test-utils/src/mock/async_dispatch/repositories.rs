//! Mock repository implementations generated via [`mock_repository!`].
//!
//! Each mock wraps one [`MockAsyncDispatchCore`] per trait method,
//! providing sequential queues, conditional matching, call history,
//! and configurable exhaustion behaviour — matching the dispatch
//! semantics of the inference provider mocks.

mod error_factories;
mod job;
mod knowledge_item;
mod principal;
mod project;
mod reference;
mod relation;
mod retrieval_feedback;
mod standing;
mod task;
mod triage_result;

pub use error_factories::{
    DbErrorFactory, a_not_found, a_pool_exhausted, a_query_failed, a_unique_violation,
};
pub use job::MockJobRepository;
pub use knowledge_item::MockKnowledgeItemRepository;
pub use principal::MockPrincipalRepository;
pub use project::MockProjectRepository;
pub use reference::MockReferenceRepository;
pub use relation::MockRelationRepository;
pub use retrieval_feedback::MockRetrievalFeedbackRepository;
pub use standing::MockStandingRepository;
pub use task::MockTaskRepository;
pub use triage_result::MockTriageResultRepository;

// ---------------------------------------------------------------------------
// mock_repository! macro
// ---------------------------------------------------------------------------

/// Generates a full mock implementation of a repository trait.
///
/// For each declared method the macro produces:
/// - a `MockAsyncDispatchCore` field in the mock struct
/// - history, call-count, and assert-exhausted accessors
/// - fluent builder methods (`on_<method>`, `on_<method>_error`,
///   `when_<method>`, `on_<method>_exhaust`)
/// - a per-method conditional builder type
/// - the `#[async_trait]` trait implementation that dispatches through
///   each core, ignoring the `&mut PgConnection` parameter
///
/// # Invocation syntax
///
/// ```ignore
/// mock_repository! {
///     MockProjectRepository for ProjectRepository, tribal_db::DbError {
///         insert(NewProject => Project)
///             (new_project: &NewProject) { new_project.clone() };
///         find_by_id(ProjectId => Project)
///             (id: ProjectId) { id };
///         list(() => Vec<Project>)
///             () { () };
///     }
/// }
/// ```
///
/// Each method line contains:
/// 1. `method_name(Req => Resp)` — dispatch core type parameters
/// 2. `(param: type, ...)` — trait method domain params (conn is implicit)
/// 3. `{ expr }` — conversion from params to the `Req` type
///
/// # Note on fully-qualified paths
///
/// The macro body uses `crate::` paths (e.g.
/// `crate::mock::async_dispatch::core::MockAsyncDispatchCore`) because
/// `macro_rules!` expansions resolve paths at the **call site**, not the
/// definition site.  Top-level `use` imports at the macro definition
/// would not be visible at expansion time.
macro_rules! mock_repository {
    (
        $MockName:ident for $Trait:ident, $Err:ty {
            $(
                $method:ident ( $Req:ty => $Resp:ty )
                    ( $( $param:ident : $param_ty:ty ),* )
                    { $convert:expr }
            );* $(;)?
        }
    ) => {
        pastey::paste! {
            // ---------------------------------------------------------------
            // Mock struct
            // ---------------------------------------------------------------

            pub struct $MockName {
                $( [<$method _core>]: crate::mock::async_dispatch::core::MockAsyncDispatchCore<$Req, $Resp, $Err>, )*
            }

            impl $MockName {
                /// Returns a new builder for configuring this mock.
                pub fn builder() -> [<$MockName Builder>] {
                    [<$MockName Builder>]::new()
                }

                $(
                    /// Returns a clone of all requests dispatched to
                    #[doc = concat!("`", stringify!($method), "`.")]
                    ///
                    /// # Panics
                    ///
                    /// Panics if the internal mutex is poisoned.
                    pub fn [<$method _history>](&self) -> Vec<$Req> {
                        self.[<$method _core>].history()
                    }

                    /// Returns the number of calls dispatched to
                    #[doc = concat!("`", stringify!($method), "`.")]
                    ///
                    /// # Panics
                    ///
                    /// Panics if the internal mutex is poisoned.
                    pub fn [<$method _call_count>](&self) -> usize {
                        self.[<$method _core>].call_count()
                    }

                    /// Panics if the sequential queue for
                    #[doc = concat!("`", stringify!($method), "`")]
                    /// has not been fully consumed.
                    ///
                    /// # Panics
                    ///
                    /// Panics if sequential entries remain unconsumed, or
                    /// if the internal mutex is poisoned.
                    pub fn [<assert_ $method _exhausted>](&self) {
                        self.[<$method _core>].assert_exhausted()
                    }
                )*
            }

            // ---------------------------------------------------------------
            // Builder
            // ---------------------------------------------------------------

            #[must_use]
            #[allow(clippy::struct_field_names)]
            pub struct [<$MockName Builder>] {
                $(
                    [<$method _queue>]: std::collections::VecDeque<
                        crate::mock::async_dispatch::core::QueueEntry<$Resp, $Err>
                    >,
                    [<$method _conditionals>]: Vec<
                        crate::mock::async_dispatch::core::ConditionalEntry<$Req, $Resp, $Err>
                    >,
                    [<$method _exhaust>]: crate::mock::async_dispatch::core::ExhaustBehaviour<$Err>,
                )*
            }

            impl [<$MockName Builder>] {
                fn new() -> Self {
                    Self {
                        $(
                            [<$method _queue>]: std::collections::VecDeque::new(),
                            [<$method _conditionals>]: Vec::new(),
                            [<$method _exhaust>]: crate::mock::async_dispatch::core::ExhaustBehaviour::Panic,
                        )*
                    }
                }

                $(
                    /// Enqueues a successful response for
                    #[doc = concat!("`", stringify!($method), "` (FIFO).")]
                    pub fn [<on_ $method>](
                        mut self,
                        response: $Resp,
                        options: Option<crate::mock::async_dispatch::core::MockProviderOptions>,
                    ) -> Self {
                        let delay = options.and_then(|o| o.delay);
                        self.[<$method _queue>].push_back(
                            crate::mock::async_dispatch::core::QueueEntry::Ok(response, delay)
                        );
                        self
                    }

                    /// Enqueues an error factory for
                    #[doc = concat!("`", stringify!($method), "`.")]
                    pub fn [<on_ $method _error>](
                        mut self,
                        factory: impl Fn() -> $Err + Send + Sync + 'static,
                        options: Option<crate::mock::async_dispatch::core::MockProviderOptions>,
                    ) -> Self {
                        let delay = options.and_then(|o| o.delay);
                        self.[<$method _queue>].push_back(
                            crate::mock::async_dispatch::core::QueueEntry::Err(Box::new(factory), delay)
                        );
                        self
                    }

                    /// Begins a conditional entry for
                    #[doc = concat!("`", stringify!($method), "`.")]
                    pub fn [<when_ $method>](
                        self,
                        matcher: impl Fn(&$Req) -> bool + Send + Sync + 'static,
                    ) -> [<$MockName Conditional $method:camel>] {
                        [<$MockName Conditional $method:camel>] {
                            parent: self,
                            matcher: Box::new(matcher),
                        }
                    }

                    /// Sets the exhaustion behaviour for
                    #[doc = concat!("`", stringify!($method), "`.")]
                    pub fn [<on_ $method _exhaust>](
                        mut self,
                        behaviour: crate::mock::async_dispatch::core::ExhaustBehaviour<$Err>,
                    ) -> Self {
                        self.[<$method _exhaust>] = behaviour;
                        self
                    }
                )*

                /// Builds the mock repository.
                pub fn build(self) -> $MockName {
                    $MockName {
                        $(
                            [<$method _core>]: crate::mock::async_dispatch::core::MockAsyncDispatchCore::new(
                                concat!(stringify!($MockName), "::", stringify!($method)),
                                self.[<$method _queue>],
                                self.[<$method _conditionals>],
                                self.[<$method _exhaust>],
                            ),
                        )*
                    }
                }
            }

            // ---------------------------------------------------------------
            // Conditional builders (one per method)
            // ---------------------------------------------------------------

            $(
                #[must_use]
                pub struct [<$MockName Conditional $method:camel>] {
                    parent: [<$MockName Builder>],
                    matcher: Box<dyn Fn(&$Req) -> bool + Send + Sync>,
                }

                impl [<$MockName Conditional $method:camel>] {
                    /// Registers a successful response for this conditional.
                    pub fn respond_with(
                        self,
                        response: $Resp,
                        options: Option<crate::mock::async_dispatch::core::MockProviderOptions>,
                    ) -> [<$MockName Builder>] {
                        let delay = options.and_then(|o| o.delay);
                        let mut parent = self.parent;
                        parent.[<$method _conditionals>].push(
                            crate::mock::async_dispatch::core::ConditionalEntry {
                                matcher: self.matcher,
                                outcome: crate::mock::async_dispatch::core::ConditionalOutcome::Ok(response, delay),
                            }
                        );
                        parent
                    }

                    /// Registers an error factory for this conditional.
                    pub fn respond_with_error(
                        self,
                        factory: impl Fn() -> $Err + Send + Sync + 'static,
                        options: Option<crate::mock::async_dispatch::core::MockProviderOptions>,
                    ) -> [<$MockName Builder>] {
                        let delay = options.and_then(|o| o.delay);
                        let mut parent = self.parent;
                        parent.[<$method _conditionals>].push(
                            crate::mock::async_dispatch::core::ConditionalEntry {
                                matcher: self.matcher,
                                outcome: crate::mock::async_dispatch::core::ConditionalOutcome::Err(
                                    Box::new(factory),
                                    delay,
                                ),
                            }
                        );
                        parent
                    }
                }
            )*

            // ---------------------------------------------------------------
            // Trait implementation
            // ---------------------------------------------------------------

            #[async_trait::async_trait]
            impl $Trait for $MockName {
                $(
                    async fn $method(
                        &self,
                        _conn: &mut sqlx::PgConnection,
                        $( $param : $param_ty, )*
                    ) -> Result<$Resp, $Err> {
                        let req = $convert;
                        let (result, delay) = self.[<$method _core>].dispatch(&req);
                        if let Some(d) = delay {
                            tokio::time::sleep(d).await;
                        }
                        result
                    }
                )*
            }
        }
    };
}

pub(crate) use mock_repository;
