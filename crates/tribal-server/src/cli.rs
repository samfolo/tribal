//! Command-line interface definition for the Tribal server.
//!
//! Re-exports all CLI types used by [`App`](crate::app::App) for argument
//! parsing and subcommand dispatch.

mod command;
mod styles;

pub use command::{
    BootstrapArgs, CheckArgs, Cli, Command, ConfigCommand, ConfigGetArgs, ConfigSetArgs,
    ConfigShowArgs, ConfigValidateArgs, CredentialCommand, CredentialSourcesCommand,
    DatabaseCommand, GenesisCredentialSourceArgs, GraphCommand, InferenceStageArg,
    IntegrationAuthArg, IntegrationCommand, IntegrationMcpConfigArgs, ManageArgs, ManagerCommand,
    ModelCredentialSourceArgs, ModelsCommand, OutputArgs, ProjectCommand, ProjectListArgs,
    ProjectRegisterArgs, ReindexCommand, ReindexPruneArgs, ReindexRunArgs, RuntimeCommand,
    ServeArgs, StageCredentialSourceArg, StageEndpointArg, StageEnvironmentCredentialArg,
    StageModelArg, ThreadsCommand, ThreadsPruneArgs, TokenCommand, TokenCreateArgs, TokenListArgs,
    TokenRevokeAllArgs, TokenRevokeArgs,
};

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

    mod administration {
        use clap::Parser as _;

        use super::super::{Cli, Command, IntegrationAuthArg, IntegrationCommand, TokenCommand};

        #[test]
        fn catalogue_parses_every_administration_projection() {
            for args in [
                vec!["tribal", "database", "initialise", "--json"],
                vec!["tribal", "project", "register", "--path", ".", "--json"],
                vec!["tribal", "project", "list", "--limit", "20", "--json"],
                vec![
                    "tribal",
                    "token",
                    "create",
                    "--persist-as-default",
                    "--json",
                ],
                vec!["tribal", "token", "list", "--json"],
                vec![
                    "tribal",
                    "token",
                    "revoke",
                    "at_550e8400-e29b-41d4-a716-446655440000",
                    "--json",
                ],
                vec!["tribal", "token", "revoke-all", "--json"],
                vec!["tribal", "integration", "mcp-config", "--json"],
                vec![
                    "tribal",
                    "bootstrap",
                    "--database-url",
                    "postgres://localhost/tribal",
                    "--model-selection",
                    "extraction=openai.gpt-4.1",
                    "--model-endpoint",
                    "extraction=provider-default",
                    "--model-credential-source",
                    "extraction=credsrc_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "--genesis-provider",
                    "ollama",
                    "--genesis-model",
                    "nomic-embed-text",
                    "--genesis-reuse-stage",
                    "extraction",
                    "--json",
                ],
            ] {
                Cli::try_parse_from(args).expect("administration projection parses");
            }
        }

        #[test]
        fn retired_administration_spellings_are_refused() {
            for args in [
                vec!["tribal", "setup"],
                vec!["tribal", "mcp-config"],
                vec![
                    "tribal",
                    "project",
                    "register",
                    "--database-url",
                    "postgres://x",
                ],
                vec!["tribal", "project", "register", "--token", "secret"],
                vec!["tribal", "project", "register", "--skip-validation"],
                vec!["tribal", "project", "register", "--transport", "stdio"],
                vec!["tribal", "token", "list", "--database-url", "postgres://x"],
                vec!["tribal", "project", "list", "--page-size", "20"],
                vec!["tribal", "token", "list", "--page-size", "20"],
                vec!["tribal", "token", "revoke", "550e8400"],
                vec![
                    "tribal",
                    "integration",
                    "mcp-config",
                    "--database-url",
                    "postgres://x",
                ],
            ] {
                assert!(Cli::try_parse_from(args).is_err());
            }
        }

        #[test]
        fn administration_arguments_preserve_typed_policy() {
            let token = Cli::try_parse_from([
                "tribal",
                "token",
                "create",
                "--principal",
                "user:sam",
                "--persist-as-default",
                "--json",
            ])
            .expect("token creation parses");
            assert!(matches!(
                token.command,
                Some(Command::Token(TokenCommand::Create { args }))
                    if args.principal.as_deref() == Some("user:sam")
                        && args.persist_as_default
                        && args.output.json
            ));

            let integration = Cli::try_parse_from([
                "tribal",
                "integration",
                "mcp-config",
                "--unscoped",
                "--auth",
                "persisted-bearer",
                "--json",
            ])
            .expect("integration rendering parses");
            assert!(matches!(
                integration.command,
                Some(Command::Integration(IntegrationCommand::McpConfig { args }))
                    if args.unscoped
                        && args.auth == IntegrationAuthArg::PersistedBearer
                        && args.output.json
            ));
        }
    }

    mod maintenance {
        use clap::Parser as _;

        use super::super::{Cli, Command, ReindexCommand, ThreadsCommand};

        #[test]
        fn catalogue_projects_preview_and_apply_modes() {
            for args in [
                vec![
                    "tribal",
                    "reindex",
                    "run",
                    "--provider",
                    "ollama",
                    "--model",
                    "nomic",
                ],
                vec![
                    "tribal",
                    "reindex",
                    "run",
                    "--provider",
                    "ollama",
                    "--model",
                    "nomic",
                    "--apply",
                    "--json",
                ],
                vec!["tribal", "reindex", "cancel", "--json"],
                vec!["tribal", "reindex", "prune"],
                vec!["tribal", "reindex", "prune", "--apply", "--json"],
                vec!["tribal", "threads", "prune", "--older-than-days", "30"],
                vec![
                    "tribal",
                    "threads",
                    "prune",
                    "--older-than-days",
                    "30",
                    "--cascade",
                    "--apply",
                    "--json",
                ],
            ] {
                Cli::try_parse_from(args).expect("maintenance projection parses");
            }
        }

        #[test]
        fn retired_maintenance_flags_are_refused() {
            for args in [
                vec![
                    "tribal",
                    "reindex",
                    "run",
                    "--provider",
                    "ollama",
                    "--model",
                    "nomic",
                    "--dry-run",
                ],
                vec![
                    "tribal",
                    "reindex",
                    "cancel",
                    "--database-url",
                    "postgres://x",
                ],
                vec![
                    "tribal",
                    "reindex",
                    "prune",
                    "--database-url",
                    "postgres://x",
                ],
                vec![
                    "tribal",
                    "threads",
                    "prune",
                    "--older-than-days",
                    "30",
                    "--dry-run",
                ],
            ] {
                assert!(Cli::try_parse_from(args).is_err());
            }
        }

        #[test]
        fn apply_is_explicit_on_mutating_projections() {
            let reindex = Cli::try_parse_from([
                "tribal",
                "reindex",
                "run",
                "--provider",
                "ollama",
                "--model",
                "nomic",
                "--apply",
            ])
            .expect("reindex apply parses");
            assert!(matches!(
                reindex.command,
                Some(Command::Reindex(ReindexCommand::Run { args })) if args.apply
            ));

            let threads = Cli::try_parse_from([
                "tribal",
                "threads",
                "prune",
                "--older-than-days",
                "30",
                "--apply",
            ])
            .expect("thread apply parses");
            assert!(matches!(
                threads.command,
                Some(Command::Threads(ThreadsCommand::Prune { args })) if args.apply
            ));
        }
    }

    mod catalogue {
        use std::collections::BTreeSet;

        use clap::CommandFactory as _;

        use super::super::Cli;

        #[test]
        fn complete_grouped_catalogue_has_no_compatibility_aliases() {
            let command = Cli::command();
            assert_eq!(
                names(&command),
                set(&[
                    "bootstrap",
                    "check",
                    "config",
                    "credential",
                    "database",
                    "graph",
                    "integration",
                    "manager",
                    "models",
                    "project",
                    "reindex",
                    "runtime",
                    "serve",
                    "threads",
                    "token",
                ])
            );
            for (group, expected) in [
                ("manager", &["run", "shutdown"][..]),
                ("runtime", &["restart", "start", "status", "stop"]),
                ("config", &["get", "path", "set", "show", "validate"]),
                ("models", &["list"]),
                ("credential", &["sources"]),
                ("graph", &["genesis-options"]),
                ("database", &["initialise"]),
                ("project", &["list", "register"]),
                ("token", &["create", "list", "revoke", "revoke-all"]),
                ("integration", &["mcp-config"]),
                ("reindex", &["cancel", "prune", "run"]),
                ("threads", &["prune"]),
            ] {
                let group = command
                    .get_subcommands()
                    .find(|candidate| candidate.get_name() == group)
                    .expect("catalogued group exists");
                assert_eq!(names(group), set(expected), "group {}", group.get_name());
            }
            let credential = command
                .get_subcommands()
                .find(|candidate| candidate.get_name() == "credential")
                .and_then(|group| {
                    group
                        .get_subcommands()
                        .find(|candidate| candidate.get_name() == "sources")
                })
                .expect("credential sources exists");
            assert_eq!(names(credential), set(&["genesis", "model"]));
            assert_no_aliases(&command);
        }

        fn names(command: &clap::Command) -> BTreeSet<&str> {
            command
                .get_subcommands()
                .map(clap::Command::get_name)
                .collect()
        }

        fn set<'a>(names: &'a [&'a str]) -> BTreeSet<&'a str> {
            names.iter().copied().collect()
        }

        fn assert_no_aliases(command: &clap::Command) {
            assert!(
                command.get_all_aliases().next().is_none(),
                "{} has a compatibility alias",
                command.get_name(),
            );
            for child in command.get_subcommands() {
                assert_no_aliases(child);
            }
        }
    }
}
