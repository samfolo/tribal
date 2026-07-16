//! Typed public management-call registry.

use serde::{Deserialize, Serialize};

use super::{
    AuthenticationSettingsSnapshot, BootstrapRequest, BootstrapResult, ConfigDocument,
    ConfigFilePath, ConfigGetRequest, ConfigPatchOutcome, ConfigPatchRequest,
    ConfigPatchValidation, ConfigSchema, ConfigSetRequest, ConfigValidatePatchRequest, ConfigValue,
    ConfigWriteOutcome, DatabaseConnectionSummary, DatabaseInitialiseRequest,
    DatabaseInitialiseResult, DatabaseProbeRequest, GenesisConfigurationRequest,
    GenesisConvergenceRequest, GenesisOptions, GraphEmbeddingProfile, ManagedRuntimeStatusResult,
    ManagerShutdownResult, ManagerSnapshot, McpConfigRequest, McpConfigResult, ModelsCatalogue,
    ProbeReceipt, ProcessingProfileSetRequest, ProcessingProfileSnapshot, ProjectList,
    ProjectListRequest, ProjectRegisterRequest, ProjectRegisterResult,
    ProviderConnectionMutationReceipt, ProviderConnectionRemoveRequest,
    ProviderConnectionUpsertRequest, ProviderConnectionsCatalogue, ProviderProbeRequest,
    ProviderProbeResponse, ReadinessReport, ReindexCancelRequest, ReindexCancelResult,
    ReindexPruneRequest, ReindexPruneResult, ReindexRunRequest, ReindexRunResult, Revisioned,
    RuntimeLogsTailRequest, RuntimeLogsTailResult, RuntimeRestartResult, RuntimeStartResult,
    RuntimeStopResult, SettingsResetPreview, SettingsResetPreviewRequest, ThreadPruneRequest,
    ThreadPruneResult, TokenCreateRequest, TokenCreateResult, TokenInventory, TokenListRequest,
    TokenRevokeAllRequest, TokenRevokeAllResult, TokenRevokeRequest, TokenRevokeResult,
};

/// The request and response types owned by one management method.
pub trait ManagementCall {
    type Request;
    type Response;

    const METHOD: ManagementMethod;
}

#[cfg(feature = "schema")]
/// Schema roots projected from one registered call.
pub struct ManagementCallSchema {
    pub call_name: &'static str,
    pub method: ManagementMethod,
    pub request_name: String,
    pub request: schemars::schema::RootSchema,
    pub response_name: String,
    pub response: schemars::schema::RootSchema,
}

macro_rules! management_calls {
    (
        $(
            $variant:ident => {
                wire: $wire:literal,
                call: $call:ident,
                request: $request:ty,
                response: $response:ty,
            }
        ),+ $(,)?
    ) => {
        /// Method identity for a compatible management session.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        pub enum ManagementMethod {
            $(
                #[serde(rename = $wire)]
                $variant,
            )+
        }

        impl ManagementMethod {
            /// Every full-protocol request method.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The stable wire spelling of this method.
            #[must_use]
            pub const fn wire_name(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }
        }

        $(
            /// Type-level association for one registered management call.
            pub struct $call;

            impl ManagementCall for $call {
                type Request = $request;
                type Response = $response;

                const METHOD: ManagementMethod = ManagementMethod::$variant;
            }
        )+

        #[cfg(feature = "schema")]
        /// Every request and response schema emitted from the call registry.
        #[must_use]
        pub fn management_call_schemas() -> Vec<ManagementCallSchema> {
            use schemars::JsonSchema as _;

            vec![$(
                ManagementCallSchema {
                    call_name: stringify!($call),
                    method: ManagementMethod::$variant,
                    request_name: <$request>::schema_name(),
                    request: schemars::schema_for!($request),
                    response_name: <$response>::schema_name(),
                    response: schemars::schema_for!($response),
                }
            ),+]
        }
    };
}

management_calls! {
    ManagerSnapshot => {
        wire: "manager.snapshot",
        call: ManagerSnapshotCall,
        request: (),
        response: ManagerSnapshot,
    },
    RuntimeStart => {
        wire: "runtime.start",
        call: RuntimeStartCall,
        request: (),
        response: RuntimeStartResult,
    },
    RuntimeStop => {
        wire: "runtime.stop",
        call: RuntimeStopCall,
        request: (),
        response: RuntimeStopResult,
    },
    RuntimeRestart => {
        wire: "runtime.restart",
        call: RuntimeRestartCall,
        request: (),
        response: RuntimeRestartResult,
    },
    ManagerShutdown => {
        wire: "manager.shutdown",
        call: ManagerShutdownCall,
        request: (),
        response: ManagerShutdownResult,
    },
    ServerStatus => {
        wire: "server.status",
        call: ServerStatusCall,
        request: (),
        response: ManagedRuntimeStatusResult,
    },
    LogsTail => {
        wire: "logs.tail",
        call: LogsTailCall,
        request: RuntimeLogsTailRequest,
        response: RuntimeLogsTailResult,
    },
    TokenList => {
        wire: "token.list",
        call: TokenListCall,
        request: TokenListRequest,
        response: TokenInventory,
    },
    TokenCreate => {
        wire: "token.create",
        call: TokenCreateCall,
        request: TokenCreateRequest,
        response: TokenCreateResult,
    },
    TokenRevoke => {
        wire: "token.revoke",
        call: TokenRevokeCall,
        request: TokenRevokeRequest,
        response: TokenRevokeResult,
    },
    TokenRevokeAll => {
        wire: "token.revokeAll",
        call: TokenRevokeAllCall,
        request: TokenRevokeAllRequest,
        response: TokenRevokeAllResult,
    },
    CheckReport => {
        wire: "check.report",
        call: CheckReportCall,
        request: (),
        response: ReadinessReport,
    },
    DatabaseInitialise => {
        wire: "database.initialise",
        call: DatabaseInitialiseCall,
        request: DatabaseInitialiseRequest,
        response: DatabaseInitialiseResult,
    },
    ProjectRegister => {
        wire: "project.register",
        call: ProjectRegisterCall,
        request: ProjectRegisterRequest,
        response: ProjectRegisterResult,
    },
    ProjectList => {
        wire: "project.list",
        call: ProjectListCall,
        request: ProjectListRequest,
        response: ProjectList,
    },
    IntegrationMcpConfig => {
        wire: "integration.mcpConfig",
        call: IntegrationMcpConfigCall,
        request: McpConfigRequest,
        response: McpConfigResult,
    },
    ReindexRun => {
        wire: "reindex.run",
        call: ReindexRunCall,
        request: ReindexRunRequest,
        response: ReindexRunResult,
    },
    ReindexCancel => {
        wire: "reindex.cancel",
        call: ReindexCancelCall,
        request: ReindexCancelRequest,
        response: ReindexCancelResult,
    },
    ReindexPrune => {
        wire: "reindex.prune",
        call: ReindexPruneCall,
        request: ReindexPruneRequest,
        response: ReindexPruneResult,
    },
    ThreadsPrune => {
        wire: "threads.prune",
        call: ThreadsPruneCall,
        request: ThreadPruneRequest,
        response: ThreadPruneResult,
    },
    BootstrapRun => {
        wire: "bootstrap.run",
        call: BootstrapRunCall,
        request: BootstrapRequest,
        response: BootstrapResult,
    },
    DatabaseProbe => {
        wire: "database.probe",
        call: DatabaseProbeCall,
        request: DatabaseProbeRequest,
        response: ProbeReceipt,
    },
    DatabaseConnection => {
        wire: "database.connection",
        call: DatabaseConnectionCall,
        request: (),
        response: DatabaseConnectionSummary,
    },
    AuthenticationSettings => {
        wire: "authentication.settings",
        call: AuthenticationSettingsCall,
        request: (),
        response: AuthenticationSettingsSnapshot,
    },
    ProviderConnections => {
        wire: "providers.catalogue",
        call: ProviderConnectionsCall,
        request: (),
        response: ProviderConnectionsCatalogue,
    },
    ProviderConnectionUpsert => {
        wire: "providers.upsert",
        call: ProviderConnectionUpsertCall,
        request: ProviderConnectionUpsertRequest,
        response: ProviderConnectionMutationReceipt,
    },
    ProviderConnectionRemove => {
        wire: "providers.remove",
        call: ProviderConnectionRemoveCall,
        request: ProviderConnectionRemoveRequest,
        response: ProviderConnectionMutationReceipt,
    },
    ProviderProbe => {
        wire: "providers.probe",
        call: ProviderProbeCall,
        request: ProviderProbeRequest,
        response: ProviderProbeResponse,
    },
    ConfigGetAll => {
        wire: "config.getAll",
        call: ConfigGetAllCall,
        request: (),
        response: ConfigDocument,
    },
    ConfigPath => {
        wire: "config.path",
        call: ConfigPathCall,
        request: (),
        response: ConfigFilePath,
    },
    ConfigSchema => {
        wire: "config.schema",
        call: ConfigSchemaCall,
        request: (),
        response: ConfigSchema,
    },
    ConfigGet => {
        wire: "config.get",
        call: ConfigGetCall,
        request: ConfigGetRequest,
        response: ConfigValue,
    },
    ConfigValidatePatch => {
        wire: "config.validatePatch",
        call: ConfigValidatePatchCall,
        request: ConfigValidatePatchRequest,
        response: ConfigPatchValidation,
    },
    ConfigSet => {
        wire: "config.set",
        call: ConfigSetCall,
        request: ConfigSetRequest,
        response: ConfigWriteOutcome,
    },
    ConfigPatch => {
        wire: "config.patch",
        call: ConfigPatchCall,
        request: ConfigPatchRequest,
        response: ConfigPatchOutcome,
    },
    ModelsCatalogue => {
        wire: "models.catalogue",
        call: ModelsCatalogueCall,
        request: (),
        response: ModelsCatalogue,
    },
    ProcessingProfile => {
        wire: "processing.profile",
        call: ProcessingProfileCall,
        request: (),
        response: ProcessingProfileSnapshot,
    },
    ProcessingProfileSet => {
        wire: "processing.set",
        call: ProcessingProfileSetCall,
        request: ProcessingProfileSetRequest,
        response: ConfigPatchOutcome,
    },
    SettingsResetPreview => {
        wire: "settings.resetPreview",
        call: SettingsResetPreviewCall,
        request: SettingsResetPreviewRequest,
        response: SettingsResetPreview,
    },
    GraphGenesisOptions => {
        wire: "graph.genesisOptions",
        call: GraphGenesisOptionsCall,
        request: (),
        response: GenesisOptions,
    },
    GraphEmbeddingProfile => {
        wire: "graph.embedding_profile",
        call: GraphEmbeddingProfileCall,
        request: (),
        response: Revisioned<GraphEmbeddingProfile>,
    },
    GraphConfigureGenesis => {
        wire: "graph.configureGenesis",
        call: GraphConfigureGenesisCall,
        request: GenesisConfigurationRequest,
        response: ConfigPatchOutcome,
    },
    GraphConvergeGenesis => {
        wire: "graph.convergeGenesis",
        call: GraphConvergeGenesisCall,
        request: GenesisConvergenceRequest,
        response: ConfigPatchOutcome,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_registered_wire_names_are_unique() {
        let names: BTreeSet<_> = ManagementMethod::ALL
            .iter()
            .copied()
            .map(ManagementMethod::wire_name)
            .collect();

        assert_eq!(names.len(), ManagementMethod::ALL.len());
    }
}
