//! Cooperative lifetime for one management application call.

use std::{future::Future, time::Duration};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tribal_wire::management::{AdministrationFailure, ManagementError, ManagementResponseError};

pub(in crate::management) const ACTIVE_WINDOW: Duration = Duration::from_secs(45);

/// Deadline and manager shutdown state shared by one composed application call.
#[derive(Debug, Clone)]
pub(in crate::management) struct OperationContext {
    deadline: Instant,
    shutdown: CancellationToken,
}

impl OperationContext {
    pub(in crate::management) fn new(shutdown: CancellationToken) -> Self {
        Self {
            deadline: Instant::now() + ACTIVE_WINDOW,
            shutdown,
        }
    }

    /// Refuses a new effect after the call's active phase has ended.
    pub(in crate::management) fn checkpoint(&self) -> Result<(), OperationError> {
        if self.shutdown.is_cancelled() {
            Err(OperationError::ManagerShuttingDown)
        } else if Instant::now() >= self.deadline {
            Err(OperationError::DeadlineElapsed)
        } else {
            Ok(())
        }
    }

    /// Interrupts work whose partial state is absent, transactional, or recoverable.
    pub(in crate::management) async fn cancel_safe<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, OperationError> {
        self.checkpoint()?;
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => {
                Err(OperationError::ManagerShuttingDown)
            }
            () = tokio::time::sleep_until(self.deadline) => {
                Err(OperationError::DeadlineElapsed)
            }
            result = future => Ok(result),
        }
    }

    /// Waits for an admitted effect to report its terminal result.
    pub(in crate::management) async fn terminal<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, OperationError> {
        self.checkpoint()?;
        Ok(future.await)
    }
}

/// Safe interruption before a management effect begins.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(in crate::management) enum OperationError {
    #[error("management operation exceeded its active deadline")]
    DeadlineElapsed,
    #[error("manager shutdown interrupted operation")]
    ManagerShuttingDown,
}

impl OperationError {
    pub(in crate::management) fn administration_failure(self) -> AdministrationFailure {
        match self {
            Self::DeadlineElapsed => AdministrationFailure::OperationTimedOut,
            Self::ManagerShuttingDown => AdministrationFailure::ManagerShuttingDown,
        }
    }
}

pub(in crate::management) fn public_error(error: OperationError) -> ManagementResponseError {
    ManagementResponseError {
        message: match error {
            OperationError::DeadlineElapsed => "management operation exceeded its active deadline",
            OperationError::ManagerShuttingDown => "manager shutdown interrupted operation",
        }
        .to_owned(),
        error: ManagementError::Administration {
            failure: error.administration_failure(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_cancel_safe_work_obeys_the_active_deadline() {
        let operation = OperationContext::new(CancellationToken::new());
        let task =
            tokio::spawn(async move { operation.cancel_safe(std::future::pending::<()>()).await });

        tokio::time::advance(ACTIVE_WINDOW).await;
        let error = public_error(task.await.expect("operation task joins").unwrap_err());

        assert!(matches!(
            error.error,
            ManagementError::Administration {
                failure: AdministrationFailure::OperationTimedOut
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_terminal_work_finishes_after_the_active_deadline() {
        let operation = OperationContext::new(CancellationToken::new());
        let (started, admitted) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            operation
                .terminal(async {
                    let _ = started.send(());
                    tokio::time::sleep(ACTIVE_WINDOW + Duration::from_secs(1)).await;
                    7
                })
                .await
        });

        admitted.await.expect("terminal work begins");
        tokio::time::advance(ACTIVE_WINDOW + Duration::from_secs(1)).await;

        assert_eq!(task.await.expect("operation task joins").unwrap(), 7);
    }

    #[tokio::test]
    async fn test_checkpoint_observes_manager_shutdown() {
        let shutdown = CancellationToken::new();
        let operation = OperationContext::new(shutdown.clone());
        shutdown.cancel();

        let error = public_error(operation.checkpoint().unwrap_err());

        assert!(matches!(
            error.error,
            ManagementError::Administration {
                failure: AdministrationFailure::ManagerShuttingDown
            }
        ));
    }

    #[test]
    fn test_active_window_leaves_terminal_grace_before_client_timeout() {
        let database_terminal =
            Duration::from_millis(super::super::database::COMMAND_STATEMENT_TIMEOUT_MS * 2);
        let credential_terminal = super::super::credential::TERMINAL_WINDOW;
        let client =
            Duration::from_secs(tribal_wire::management::MANAGEMENT_REQUEST_TIMEOUT_SECONDS);

        assert!(ACTIVE_WINDOW + database_terminal < client);
        assert!(ACTIVE_WINDOW + credential_terminal < client);
    }
}
