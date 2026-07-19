//! Committed JSON Schema for the public local-management contract, checked here
//! against freshly generated output.
#![cfg(feature = "schema")]

use std::{collections::BTreeMap, fs, path::Path};

use schemars::{schema::RootSchema, schema_for};
use tribal_wire::management as m;

const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";
const CONTRACT: &str = "contracts/management.json";

#[test]
fn management_contract_matches_its_committed_schema() {
    let schemas = contract_schemas();
    let generated =
        serde_json::to_string_pretty(&schemas).expect("management schemas serialise to JSON");
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(CONTRACT);

    if std::env::var(UPDATE_ENV_VAR).is_ok() {
        fs::create_dir_all(path.parent().expect("contract path has a parent"))
            .expect("create the contract directory");
        fs::write(&path, &generated).expect("write the committed schema");
        return;
    }

    let committed = fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "committed schema missing at {}; run with {UPDATE_ENV_VAR}=1 to create it",
            path.display(),
        )
    });
    assert_eq!(
        committed,
        generated,
        "management schema drifted from {}; run with {UPDATE_ENV_VAR}=1 to update it",
        path.display(),
    );
}

#[test]
fn registered_call_catalogue_is_total() {
    let catalogue = m::management_call_schemas();
    let registered: Vec<_> = catalogue.iter().map(|call| call.method).collect();

    assert_eq!(registered, m::ManagementMethod::ALL);
}

fn contract_schemas() -> BTreeMap<String, RootSchema> {
    let mut schemas = BTreeMap::new();
    let calls = m::management_call_schemas();
    insert_schema(
        &mut schemas,
        "ManagementCallCatalogue".to_owned(),
        call_catalogue(&calls),
    );
    for call in calls {
        insert_renderable(&mut schemas, call.request_name, call.request);
        insert_renderable(&mut schemas, call.response_name, call.response);
    }

    for (name, schema) in [
        (
            "ManagementBootstrapRequest",
            schema_for!(m::ManagementBootstrapRequest),
        ),
        (
            "ManagementBootstrapResponse",
            schema_for!(m::ManagementBootstrapResponse),
        ),
        ("ManagementEvent", schema_for!(m::ManagementEvent)),
        ("ManagementMethod", schema_for!(m::ManagementMethod)),
        ("ManagerLaunchRecord", schema_for!(m::ManagerLaunchRecord)),
        (
            "ManagementResponseError",
            schema_for!(m::ManagementResponseError),
        ),
    ] {
        insert_schema(&mut schemas, name.to_owned(), schema);
    }
    schemas
}

fn call_catalogue(calls: &[m::ManagementCallSchema]) -> RootSchema {
    let calls = calls
        .iter()
        .map(|call| {
            serde_json::json!({
                "call": call.call_name,
                "method": call.method.wire_name(),
                "request": call_type(&call.request_name, &call.request),
                "response": call_type(&call.response_name, &call.response),
            })
        })
        .collect();
    let mut schema = RootSchema::default();
    schema.schema.extensions.insert(
        "x-tribal-swift-type".to_owned(),
        serde_json::Value::String("management-call-catalogue".to_owned()),
    );
    schema
        .schema
        .extensions
        .insert("x-tribal-management-calls".to_owned(), calls);
    schema
}

fn call_type(name: &str, schema: &RootSchema) -> serde_json::Value {
    let shape = serde_json::to_value(&schema.schema).expect("call schema root serialises");
    match shape.get("type").and_then(serde_json::Value::as_str) {
        Some("null") => serde_json::Value::Null,
        Some("array") => {
            let item = shape
                .pointer("/items/$ref")
                .and_then(serde_json::Value::as_str)
                .and_then(|reference| reference.rsplit('/').next())
                .expect("registered array calls name their item schema");
            serde_json::json!({ "array": item })
        }
        _ => serde_json::Value::String(name.to_owned()),
    }
}

fn insert_renderable(schemas: &mut BTreeMap<String, RootSchema>, name: String, schema: RootSchema) {
    let shape = serde_json::to_value(&schema.schema).expect("schema root serialises");
    let renderable = shape.get("properties").is_some()
        || shape.get("oneOf").is_some()
        || shape.get("anyOf").is_some()
        || shape.get("const").is_some()
        || shape.get("type").and_then(serde_json::Value::as_str) == Some("string");
    if renderable {
        insert_schema(schemas, name, schema);
    }
}

fn insert_schema(schemas: &mut BTreeMap<String, RootSchema>, name: String, schema: RootSchema) {
    if let Some(existing) = schemas.get(&name) {
        assert_eq!(
            serde_json::to_value(existing).expect("existing schema serialises"),
            serde_json::to_value(&schema).expect("registered schema serialises"),
            "registered calls disagree on schema `{name}`",
        );
        return;
    }
    schemas.insert(name, schema);
}
