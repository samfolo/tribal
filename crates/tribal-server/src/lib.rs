//! Tribal server library — programmatic API for the server lifecycle.

mod app;
mod cli;
mod commands;
mod error;
mod git;
mod management;
mod orchestration;
mod output;
mod startup;
mod transport;

pub use app::App;
pub use error::AppError;
#[cfg(feature = "test-helpers")]
pub use management::operator_check::{CheckOptions, CheckOutput, run_async as check_async};
pub use management::{
    client::{ManagementClient, ManagementClientError},
    connector::{ManagerConnection, ManagerConnector, ManagerConnectorError},
};
pub use orchestration::{ServerHandle, start_server};
#[cfg(feature = "test-helpers")]
pub use transport::{run_http_transport, run_sse_transport};
