//! Redis broker operations for runtime metadata.
//!
//! Reference: Asynq v0.26.0 Redis server and scheduler metadata behavior:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
//! and runtime inspection behavior:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::broker::redis::{
    RedisClearSchedulerEntriesPlan, RedisClearSchedulerHistoryPlan, RedisClearServerStatePlan,
    RedisExecutor, RedisListSchedulerEnqueueEventsPlan, RedisListSchedulerEntriesPlan,
    RedisListServerKeysPlan, RedisListWorkerKeysPlan, RedisRecordSchedulerEnqueueEventPlan,
    RedisScriptCall, RedisWriteSchedulerEntriesPlan, RedisWriteServerStatePlan,
};
use crate::client::Clock;
use crate::{
    AdminError, ListOption, MetadataError, Pagination, SchedulerEnqueueEventInfo,
    SchedulerEntryInfo, ServerInfo, WorkerMetadataInfo,
};

use super::parse::{
    decode_scheduler_enqueue_event, decode_scheduler_entry, decode_server_info, decode_worker_info,
};
use super::{RedisBroker, RedisBrokerError, redis_broker_error_message};

impl From<RedisBrokerError> for MetadataError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    pub(in crate::broker::redis::broker) async fn eval_status_metadata(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<(), MetadataError> {
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(MetadataError::from)?;
        let status = self
            .executor
            .eval_script_status(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        if status == "OK" {
            Ok(())
        } else {
            Err(MetadataError::from(
                RedisBrokerError::UnexpectedScriptStatus {
                    script: call.script(),
                    status,
                },
            ))
        }
    }

    pub(in crate::broker::redis::broker) async fn eval_metadata_key_list(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<String>, AdminError> {
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        self.executor
            .eval_script_byte_vec(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?
            .into_iter()
            .map(|key| {
                String::from_utf8(key)
                    .map_err(|error| AdminError::Other(format!("invalid metadata key: {error}")))
            })
            .collect()
    }
}

/// Reference: Asynq v0.26.0 Redis server metadata and runtime inspection
/// behavior:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    pub async fn write_server_state(
        &mut self,
        hostname: &str,
        pid: i32,
        server_id: &str,
        server_info: impl Into<Vec<u8>>,
        workers: impl IntoIterator<Item = Vec<u8>>,
        ttl: Duration,
    ) -> Result<(), MetadataError> {
        let redis_plan = RedisWriteServerStatePlan::from_server(
            hostname,
            pid,
            server_id,
            server_info,
            workers,
            self.clock.now(),
            ttl,
        )
        .map_err(RedisBrokerError::MetadataPlan)
        .map_err(MetadataError::from)?;
        self.executor
            .zadd(
                redis_plan.all_servers_key(),
                redis_plan.expires_at(),
                redis_plan.server_info_key().as_bytes(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        self.executor
            .zadd(
                redis_plan.all_workers_key(),
                redis_plan.expires_at(),
                redis_plan.workers_key().as_bytes(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        self.eval_status_metadata(redis_plan.call()).await
    }

    pub async fn clear_server_state(
        &mut self,
        hostname: &str,
        pid: i32,
        server_id: &str,
    ) -> Result<(), MetadataError> {
        let redis_plan = RedisClearServerStatePlan::from_server(hostname, pid, server_id)
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(MetadataError::from)?;
        self.executor
            .zrem(redis_plan.all_servers_key(), redis_plan.server_info_key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        self.executor
            .zrem(redis_plan.all_workers_key(), redis_plan.workers_key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        // Reference: Asynq v0.26.0 `RDB.ClearServerState` removes both
        // metadata keys with the two-key `clearServerStateCmd` Lua script
        // after deleting the global sorted-set members.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1423-L1439>.
        self.eval_status_metadata(redis_plan.call()).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.Servers` combines live server
    /// metadata and active worker metadata without sorting.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L790-L826>.
    pub async fn servers(&mut self) -> Result<Vec<ServerInfo>, AdminError> {
        let mut servers = self.list_servers().await?;
        let workers = self.list_workers().await?;
        let server_indexes = servers
            .iter()
            .enumerate()
            .map(|(index, server)| (server.id().to_owned(), index))
            .collect::<HashMap<_, _>>();
        for worker in workers {
            if let Some(index) = server_indexes.get(worker.server_id()) {
                servers[*index]
                    .active_workers_mut()
                    .push(worker.into_admin_worker_info());
            }
        }
        Ok(servers)
    }

    /// Reference: Asynq v0.26.0 `RDB.ListServers` lists live server protobufs
    /// and skips stale, unreadable, or malformed data without sorting.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1933-L1964>.
    pub async fn list_servers(&mut self) -> Result<Vec<ServerInfo>, AdminError> {
        let plan = RedisListServerKeysPlan::from_time(self.clock.now())
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(AdminError::from)?;
        let keys = self.eval_metadata_key_list(plan.call()).await?;
        let mut servers = Vec::new();
        for key in keys {
            let Ok(Some(data)) = self.executor.get_bytes(&key).await else {
                continue;
            };
            if let Ok(server) = decode_server_info(data) {
                servers.push(server);
            }
        }
        Ok(servers)
    }

    /// Reference: Asynq v0.26.0 `RDB.ListWorkers` lists live worker hashes and
    /// skips stale, unreadable, or malformed data without sorting.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1966-L2000>.
    async fn list_workers(&mut self) -> Result<Vec<WorkerMetadataInfo>, AdminError> {
        let plan = RedisListWorkerKeysPlan::from_time(self.clock.now())
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(AdminError::from)?;
        let keys = self.eval_metadata_key_list(plan.call()).await?;
        let mut workers = Vec::new();
        for key in keys {
            let Ok(data) = self.executor.hvals_bytes(&key).await else {
                continue;
            };
            for value in data {
                if let Ok(worker) = decode_worker_info(value) {
                    workers.push(worker);
                }
            }
        }
        Ok(workers)
    }
}

/// Reference: Asynq v0.26.0 scheduler metadata write, clear, history, and
/// inspection behavior:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1449-L1474>
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2009-L2057>.
impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    pub async fn write_scheduler_entries<I>(
        &mut self,
        scheduler_id: &str,
        entries: I,
        ttl: Duration,
    ) -> Result<(), MetadataError>
    where
        I: IntoIterator<Item = (String, Vec<u8>)>,
    {
        let redis_plan = RedisWriteSchedulerEntriesPlan::from_entries(
            scheduler_id,
            entries,
            self.clock.now(),
            ttl,
        )
        .map_err(RedisBrokerError::MetadataPlan)
        .map_err(MetadataError::from)?;
        self.executor
            .zadd(
                redis_plan.all_schedulers_key(),
                redis_plan.expires_at(),
                redis_plan.scheduler_entries_key().as_bytes(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        self.eval_status_metadata(redis_plan.call()).await
    }

    pub async fn clear_scheduler_entries(
        &mut self,
        scheduler_id: &str,
    ) -> Result<(), MetadataError> {
        let redis_plan = RedisClearSchedulerEntriesPlan::from_scheduler(scheduler_id)
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(MetadataError::from)?;
        // Reference: Asynq v0.26.0 `RDB.ClearSchedulerEntries` removes the
        // scheduler entry key from `asynq:schedulers` before deleting the
        // entry list.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1463-L1474>.
        self.executor
            .zrem(redis_plan.all_schedulers_key(), redis_plan.entries_key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        self.executor
            .del(redis_plan.entries_key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        Ok(())
    }

    /// Reference: Asynq v0.26.0 `Inspector.SchedulerEntries` lists entries
    /// registered by currently running schedulers.
    /// Reference: Asynq v0.26.0 `RDB.ListSchedulerEntries` skips scheduler
    /// keys that fail `LRANGE` and entries that fail protobuf decode.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2009-L2035>.
    pub async fn scheduler_entries(&mut self) -> Result<Vec<SchedulerEntryInfo>, AdminError> {
        let plan = RedisListSchedulerEntriesPlan::from_time(self.clock.now())
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(AdminError::from)?;
        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let keys = self
            .executor
            .eval_script_byte_vec(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;

        let mut entries = Vec::new();
        for key in keys {
            let Ok(key) = String::from_utf8(key) else {
                continue;
            };
            let Ok(data) = self.executor.lrange_bytes(&key, 0, -1).await else {
                continue;
            };
            for entry in data {
                if let Ok(entry) = decode_scheduler_entry(entry) {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    /// Reference: Asynq v0.26.0 `Inspector.ListSchedulerEnqueueEvents` returns
    /// scheduler enqueue history newest first.
    /// Reference: Asynq v0.26.0 `RDB.ListSchedulerEnqueueEvents` derives the
    /// Redis history key from the supplied entry id without local validation.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2037-L2057>.
    pub async fn list_scheduler_enqueue_events(
        &mut self,
        entry_id: &str,
        pagination: Pagination,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError> {
        let plan =
            RedisListSchedulerEnqueueEventsPlan::from_entry_and_pagination(entry_id, pagination)
                .map_err(RedisBrokerError::MetadataPlan)
                .map_err(AdminError::from)?;
        let data = self
            .executor
            .zrevrange_bytes(
                plan.history_key(),
                plan.pagination().start(),
                plan.pagination().stop(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        data.into_iter()
            .map(decode_scheduler_enqueue_event)
            .collect()
    }

    /// Reference: Asynq v0.26.0 public
    /// `Inspector.ListSchedulerEnqueueEvents` accepts variadic `ListOption`
    /// values:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1036-L1051>.
    pub async fn list_scheduler_enqueue_events_with_options<I>(
        &mut self,
        entry_id: &str,
        options: I,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_scheduler_enqueue_events(entry_id, pagination)
            .await
    }

    pub async fn record_scheduler_enqueue_event(
        &mut self,
        entry_id: &str,
        event: impl Into<Vec<u8>>,
        now: SystemTime,
    ) -> Result<(), MetadataError> {
        let redis_plan = RedisRecordSchedulerEnqueueEventPlan::from_event(entry_id, event, now)
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(MetadataError::from)?;
        self.eval_status_metadata(redis_plan.call()).await
    }

    pub async fn clear_scheduler_history(&mut self, entry_id: &str) -> Result<(), MetadataError> {
        let redis_plan = RedisClearSchedulerHistoryPlan::from_entry(entry_id)
            .map_err(RedisBrokerError::MetadataPlan)
            .map_err(MetadataError::from)?;
        self.executor
            .del(redis_plan.history_key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(MetadataError::from)?;
        Ok(())
    }
}
