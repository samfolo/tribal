//! The managed-run loop: the job-plane sibling of the driver loop.
//!
//! It claims managed runs off `run_job`, reclaims any whose lease has lapsed
//! back to the queue, and drives each claimed run through
//! [`drive_managed_run`] under a per-run metering transport. A worker with no
//! [`ManagedRuntime`] runs no job plane, so the loop returns at once.

use std::sync::Arc;

use sqlx::PgPool;
use tokio::task::JoinSet;
use tribal_inference::{GatewayMeteringClient, InferenceProvider};
use tribal_runtime_db::{ClaimedJob, PgRunJobRepository, RunJobRepository, mint_grant};

use super::{
    managed_run::{ManagedConfig, drive_managed_run},
    metering::MeteringTransport,
};
use crate::worker::Worker;

/// The dependencies the managed-run loop drives with. Absent on a worker that
/// runs no job plane; present, it binds the loop to a live gateway and the
/// runtime database.
///
/// Cheap to clone — the pool and provider are handle-shared and the metering
/// client's connection pool is shared — so each claimed run drives under its
/// own clone, wrapping the client in a grant of its own.
#[derive(Clone)]
pub struct ManagedRuntime {
    /// The job-plane database the loop claims from.
    runtime_pool: PgPool,
    /// The Platform provider each metered call brackets through.
    provider: Arc<dyn InferenceProvider>,
    /// The grantless metering client each run's teardown transport wraps.
    metering_client: GatewayMeteringClient,
    /// The timing every drive runs under.
    config: ManagedConfig,
}

impl ManagedRuntime {
    /// Binds the loop to its runtime database, metered-call provider, metering
    /// client, and drive timing.
    #[must_use]
    pub fn new(
        runtime_pool: PgPool,
        provider: Arc<dyn InferenceProvider>,
        metering_client: GatewayMeteringClient,
        config: ManagedConfig,
    ) -> Self {
        Self {
            runtime_pool,
            provider,
            metering_client,
            config,
        }
    }

    /// The job-plane pool, the sweep's other plane.
    pub(crate) fn runtime_pool(&self) -> &PgPool {
        &self.runtime_pool
    }
}

/// How many managed runs one cycle claims and drives concurrently. Each claim
/// yields at most one job, so the cycle claims up to this many before driving.
const MANAGED_CLAIM_BATCH: usize = 4;

/// The per-tenant admission ceiling a tenant with no slot of its own is
/// admitted under.
const MANAGED_TENANT_DEFAULT_CAP: i32 = 4;

/// How many lapsed leases one cycle returns to the queue.
const MANAGED_RECLAIM_LIMIT: i64 = 16;

impl Worker {
    /// Drives the job plane until cancellation: each cycle returns lapsed
    /// leases to the queue, then claims and drives a batch of runs. A worker
    /// with no [`ManagedRuntime`] returns at once.
    pub async fn run_managed_loop(self: &Arc<Self>) {
        let Some(managed) = self.managed() else {
            return;
        };
        let mut ticker = tokio::time::interval(self.config().poll_interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip the immediate first tick

        loop {
            tokio::select! {
                () = self.cancellation_token().cancelled() => return,
                _ = ticker.tick() => {}
            }

            self.reclaim_expired_managed(managed).await;
            self.drive_claimed_managed(managed).await;
        }
    }

    /// Returns lapsed managed leases to the queue, so a crashed worker's run is
    /// re-claimed and re-driven from its committed tail.
    async fn reclaim_expired_managed(&self, managed: &ManagedRuntime) {
        let Ok(mut conn) = managed.runtime_pool.acquire().await else {
            tracing::warn!("managed loop runtime pool acquire failed");
            return;
        };
        if let Err(error) = PgRunJobRepository
            .reclaim_expired(&mut conn, MANAGED_RECLAIM_LIMIT)
            .await
        {
            tracing::warn!(error = %error, "managed lease reclaim failed");
        }
    }

    /// Claims one batch of managed runs and drives them concurrently: a serial
    /// lane would stall each run behind the last's durable wait. The batch size
    /// bounds concurrency; the gateway's holds bound the money.
    async fn drive_claimed_managed(self: &Arc<Self>, managed: &ManagedRuntime) {
        let claimed = self.claim_managed_batch(managed).await;
        let mut batch = JoinSet::new();
        for job in claimed {
            let worker = Arc::clone(self);
            let managed = managed.clone();
            batch.spawn(async move { drive_one(&worker, &managed, &job).await });
        }
        // Each drive handles its own faults and returns unit, so a join error is
        // a panic or cancellation: surface it rather than lose it silently.
        while let Some(joined) = batch.join_next().await {
            if let Err(error) = joined {
                tracing::error!(error = %error, "a managed drive failed to complete");
            }
        }
    }

    /// Claims up to a batch of runs, walking tenants until the queue or the
    /// batch is spent. Each claim yields one run under per-tenant admission.
    async fn claim_managed_batch(&self, managed: &ManagedRuntime) -> Vec<ClaimedJob> {
        let Ok(mut conn) = managed.runtime_pool.acquire().await else {
            tracing::warn!("managed loop runtime pool acquire failed");
            return vec![];
        };
        let mut claimed = Vec::new();
        while claimed.len() < MANAGED_CLAIM_BATCH {
            match PgRunJobRepository
                .claim(&mut conn, MANAGED_TENANT_DEFAULT_CAP, managed.config.lease)
                .await
            {
                Ok(Some(job)) => claimed.push(job),
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(error = %error, "managed run claim failed");
                    break;
                }
            }
        }
        claimed
    }
}

/// Drives one claimed run under a metering transport bound to its own grant.
async fn drive_one(worker: &Worker, managed: &ManagedRuntime, job: &ClaimedJob) {
    let transport = MeteringTransport::new(managed.metering_client.clone(), mint_grant(job));
    match drive_managed_run(
        worker.pool(),
        &managed.runtime_pool,
        managed.provider.as_ref(),
        &transport,
        job,
        &managed.config,
    )
    .await
    {
        Ok(outcome) => {
            tracing::info!(run_key = %job.id, ?outcome, "managed run settled");
        }
        Err(error) => {
            tracing::warn!(run_key = %job.id, error = %error, "managed run failed");
        }
    }
}
