use std::collections::BTreeMap;

use schemars::{schema::RootSchema, schema_for};
use tribal_wire::control::{
    CheckReport, CheckReportRequest, ClientHello, ConfigDocument, ConfigGetRequest, ConfigPath,
    ConfigSchema, ConfigSetRequest, ConfigValidateRequest, ConfigValidation, ConfigValue,
    ConfigWriteOutcome, ControlEvent, ControlNotification, ControlRequest, ControlResponse,
    CredentialProbe, CredentialProbeRequest, DatabaseProbe, DatabaseProbeRequest,
    GraphEmbeddingProfile, LogLines, LogsTailRequest, ModelsCatalogue, RestartOutcome, ServerHello,
    ServerStatus, StopOutcome, TokenList,
};

/// Legacy operator crossings retained in the public management contract.
pub fn legacy_control_schemas() -> BTreeMap<&'static str, RootSchema> {
    BTreeMap::from([
        ("ControlRequest", schema_for!(ControlRequest)),
        ("ControlResponse", schema_for!(ControlResponse)),
        ("ControlNotification", schema_for!(ControlNotification)),
        ("ClientHello", schema_for!(ClientHello)),
        ("ServerHello", schema_for!(ServerHello)),
        ("ConfigSchema", schema_for!(ConfigSchema)),
        ("ConfigGetRequest", schema_for!(ConfigGetRequest)),
        ("ConfigValue", schema_for!(ConfigValue)),
        ("ConfigDocument", schema_for!(ConfigDocument)),
        ("ConfigSetRequest", schema_for!(ConfigSetRequest)),
        ("ConfigWriteOutcome", schema_for!(ConfigWriteOutcome)),
        ("ConfigValidateRequest", schema_for!(ConfigValidateRequest)),
        ("ConfigValidation", schema_for!(ConfigValidation)),
        ("ConfigPath", schema_for!(ConfigPath)),
        ("CheckReportRequest", schema_for!(CheckReportRequest)),
        ("CheckReport", schema_for!(CheckReport)),
        ("DatabaseProbeRequest", schema_for!(DatabaseProbeRequest)),
        ("DatabaseProbe", schema_for!(DatabaseProbe)),
        (
            "CredentialProbeRequest",
            schema_for!(CredentialProbeRequest),
        ),
        ("CredentialProbe", schema_for!(CredentialProbe)),
        ("GraphEmbeddingProfile", schema_for!(GraphEmbeddingProfile)),
        ("ModelsCatalogue", schema_for!(ModelsCatalogue)),
        ("ServerStatus", schema_for!(ServerStatus)),
        ("RestartOutcome", schema_for!(RestartOutcome)),
        ("StopOutcome", schema_for!(StopOutcome)),
        ("LogsTailRequest", schema_for!(LogsTailRequest)),
        ("LogLines", schema_for!(LogLines)),
        ("TokenList", schema_for!(TokenList)),
        ("ControlEvent", schema_for!(ControlEvent)),
    ])
}
