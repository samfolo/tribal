//! Public management request vocabulary.

use serde::{Deserialize, Serialize};

/// Method identity for a compatible management session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum ManagementMethod {
    #[serde(rename = "manager.snapshot")]
    ManagerSnapshot,
    #[serde(rename = "runtime.start")]
    RuntimeStart,
    #[serde(rename = "runtime.stop")]
    RuntimeStop,
    #[serde(rename = "runtime.restart")]
    RuntimeRestart,
    #[serde(rename = "manager.shutdown")]
    ManagerShutdown,
    #[serde(rename = "server.status")]
    ServerStatus,
    #[serde(rename = "logs.tail")]
    LogsTail,
    #[serde(rename = "token.list")]
    TokenList,
    #[serde(rename = "check.report")]
    CheckReport,
    #[serde(rename = "database.probe")]
    DatabaseProbe,
    #[serde(rename = "credential.probe")]
    CredentialProbe,
    #[serde(rename = "config.getAll")]
    ConfigGetAll,
    #[serde(rename = "config.path")]
    ConfigPath,
    #[serde(rename = "config.schema")]
    ConfigSchema,
    #[serde(rename = "config.get")]
    ConfigGet,
    #[serde(rename = "config.validate")]
    ConfigValidate,
    #[serde(rename = "config.set")]
    ConfigSet,
    #[serde(rename = "config.patch")]
    ConfigPatch,
    #[serde(rename = "models.catalogue")]
    ModelsCatalogue,
    #[serde(rename = "models.select")]
    ModelsSelect,
    #[serde(rename = "credential.sources")]
    CredentialSources,
    #[serde(rename = "graph.genesisOptions")]
    GraphGenesisOptions,
    #[serde(rename = "graph.embedding_profile")]
    GraphEmbeddingProfile,
    #[serde(rename = "graph.configureGenesis")]
    GraphConfigureGenesis,
    #[serde(rename = "graph.convergeGenesis")]
    GraphConvergeGenesis,
}
