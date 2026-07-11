//! Projection from config-native schema metadata to the public management DTO.

use tribal_wire::management as wire;

/// A config-native schema cannot be represented by the public contract.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigSchemaProjectionError {
    /// A fixed field escaped the config classifier's totality gate.
    #[error("configuration field '{path}' has no reload classification")]
    Unclassified { path: String },
}

/// Projects the config authority's schema without adding runtime state.
pub(crate) fn project(
    schema: tribal_config::ConfigSchema,
) -> Result<wire::ConfigSchema, ConfigSchemaProjectionError> {
    let fields = schema
        .fields
        .into_iter()
        .map(project_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(wire::ConfigSchema {
        schema: schema.schema,
        groups: schema.groups,
        fields,
    })
}

fn project_field(
    field: tribal_config::ConfigFieldMeta,
) -> Result<wire::ConfigFieldMeta, ConfigSchemaProjectionError> {
    let reload_class = match field.reload_class {
        tribal_config::ReloadClass::Hot => wire::ReloadClass::Hot,
        tribal_config::ReloadClass::GenesisOnly => wire::ReloadClass::GenesisOnly,
        tribal_config::ReloadClass::RequiresRestart => wire::ReloadClass::RequiresRestart,
        tribal_config::ReloadClass::Unclassified => {
            return Err(ConfigSchemaProjectionError::Unclassified { path: field.path });
        }
    };
    let tier = match field.tier {
        tribal_config::AudienceTier::Primary => wire::AudienceTier::Primary,
        tribal_config::AudienceTier::Standard => wire::AudienceTier::Standard,
        tribal_config::AudienceTier::Advanced => wire::AudienceTier::Advanced,
        tribal_config::AudienceTier::Hidden => wire::AudienceTier::Hidden,
        tribal_config::AudienceTier::MachineOwned => wire::AudienceTier::MachineOwned,
    };
    Ok(wire::ConfigFieldMeta {
        path: field.path,
        secret: field.secret,
        tier,
        group: field.group,
        reload_class,
        default: field.default,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_the_config_schema_projects_every_fixed_field() {
        let source = tribal_config::config_schema();
        let source_count = source.fields.len();

        let projected = project(source).expect("the total config schema projects");

        assert_eq!(projected.fields.len(), source_count);
        assert_eq!(projected.groups, tribal_config::config_schema().groups);
    }

    #[test]
    fn test_an_unclassified_field_is_refused() {
        let mut source = tribal_config::config_schema();
        let field = source.fields.first_mut().expect("schema has fixed fields");
        let path = field.path.clone();
        field.reload_class = tribal_config::ReloadClass::Unclassified;

        let result = project(source);

        assert!(matches!(
            result,
            Err(ConfigSchemaProjectionError::Unclassified { path: found }) if found == path
        ));
    }
}
