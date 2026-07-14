//! Command-line interface definition for the Tribal server.
//!
//! Re-exports all CLI types used by [`App`](crate::app::App) for argument
//! parsing and subcommand dispatch.

mod command;
mod flags;
mod styles;

pub use command::{
    BootstrapArgs, CheckArgs, Cli, Command, ConfigCommand, ConfigGetArgs, ConfigSetArgs,
    ConfigShowArgs, ConfigValidateArgs, CredentialCommand, CredentialSourcesCommand, DatabaseArgs,
    DatabaseCommand, GenesisCredentialSourceArgs, GraphCommand, InferenceStageArg, ManageArgs,
    ManagerCommand, McpConfigArgs, ModelCredentialSourceArgs, ModelsCommand, OutputArgs,
    ProjectCommand, ProjectListArgs, ProjectRegisterArgs, ReindexCommand, ReindexRunArgs,
    RuntimeCommand, ServeArgs, SetupArgs, ThreadsCommand, ThreadsPruneArgs, TokenCommand,
    TokenCreateArgs, TokenListArgs, TokenRevokeAllArgs, TokenRevokeArgs,
};
pub(crate) use flags::PersistableFlag;

#[cfg(test)]
mod projection {
    mod core {
        use clap::Parser as _;

        use super::super::{
            Cli, Command, CredentialCommand, CredentialSourcesCommand, GraphCommand,
            InferenceStageArg, ModelsCommand, RuntimeCommand,
        };

        #[test]
        fn catalogue_parses_every_core_projection() {
            for args in [
                vec!["tribal", "runtime", "start", "--json"],
                vec!["tribal", "runtime", "stop"],
                vec!["tribal", "runtime", "restart"],
                vec!["tribal", "runtime", "status"],
                vec!["tribal", "config", "show"],
                vec!["tribal", "config", "get", "server.transport"],
                vec!["tribal", "config", "set", "server.transport", "stdio"],
                vec!["tribal", "config", "validate", "server.transport", "stdio"],
                vec!["tribal", "config", "path"],
                vec!["tribal", "check", "--providers", "--json"],
                vec!["tribal", "models", "list", "--json"],
                vec!["tribal", "graph", "genesis-options", "--json"],
            ] {
                Cli::try_parse_from(args).expect("core projection parses");
            }
        }

        #[test]
        fn retired_core_flags_are_refused() {
            for args in [
                vec!["tribal", "check", "--project", "proj_invalid"],
                vec!["tribal", "check", "--token", "secret"],
                vec!["tribal", "config", "show", "--show-secrets"],
            ] {
                assert!(Cli::try_parse_from(args).is_err());
            }
        }

        #[test]
        fn discovery_arguments_project_typed_context() {
            let model = Cli::try_parse_from([
                "tribal",
                "credential",
                "sources",
                "model",
                "--model",
                "openai.default",
                "--stage",
                "extraction",
                "--provider-default",
                "--json",
            ])
            .expect("model credential query parses");
            assert!(matches!(
                model.command,
                Some(Command::Credential(CredentialCommand::Sources(
                    CredentialSourcesCommand::Model { args }
                ))) if args.stage == vec![InferenceStageArg::Extraction]
                    && args.provider_default
                    && args.output.json
            ));

            let genesis = Cli::try_parse_from([
                "tribal",
                "credential",
                "sources",
                "genesis",
                "--provider",
                "openai",
                "--model",
                "text-embedding-3-small",
                "--dimensions",
                "1536",
            ])
            .expect("genesis credential query parses");
            assert!(matches!(
                genesis.command,
                Some(Command::Credential(CredentialCommand::Sources(
                    CredentialSourcesCommand::Genesis { args }
                ))) if args.dimensions == Some(1536)
            ));
        }

        #[test]
        fn output_mode_is_bound_to_each_projection() {
            let runtime = Cli::try_parse_from(["tribal", "runtime", "status", "--json"])
                .expect("runtime status parses");
            assert!(matches!(
                runtime.command,
                Some(Command::Runtime(RuntimeCommand::Status { output })) if output.json
            ));
            let models = Cli::try_parse_from(["tribal", "models", "list", "--json"])
                .expect("models list parses");
            assert!(matches!(
                models.command,
                Some(Command::Models(ModelsCommand::List { output })) if output.json
            ));
            let graph = Cli::try_parse_from(["tribal", "graph", "genesis-options", "--json"])
                .expect("graph options parse");
            assert!(matches!(
                graph.command,
                Some(Command::Graph(GraphCommand::GenesisOptions { output })) if output.json
            ));
        }
    }
}
