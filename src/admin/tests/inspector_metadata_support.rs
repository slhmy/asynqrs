use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MetadataCall {
    Servers,
    ClusterKeySlot(String),
    ClusterNodes(String),
    SchedulerEntries,
    SchedulerEvents {
        entry_id: String,
        pagination: Pagination,
    },
}

#[derive(Debug, Clone, Default)]
pub(super) struct RecordingMetadataBroker {
    pub(super) calls: Vec<MetadataCall>,
    pub(super) servers: Vec<ServerInfo>,
    pub(super) key_slot: i64,
    pub(super) cluster_nodes: Vec<ClusterNode>,
    pub(super) scheduler_entries: Vec<SchedulerEntryInfo>,
    pub(super) scheduler_events: Vec<SchedulerEnqueueEventInfo>,
    pub(super) error: Option<AdminError>,
}

#[async_trait]
impl InspectorMetadataBroker for RecordingMetadataBroker {
    async fn servers(&mut self) -> Result<Vec<ServerInfo>, AdminError> {
        self.calls.push(MetadataCall::Servers);
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.servers.clone())
    }

    async fn cluster_key_slot(&mut self, queue: &str) -> Result<i64, AdminError> {
        self.calls
            .push(MetadataCall::ClusterKeySlot(queue.to_owned()));
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.key_slot)
    }

    async fn cluster_nodes(&mut self, queue: &str) -> Result<Vec<ClusterNode>, AdminError> {
        self.calls
            .push(MetadataCall::ClusterNodes(queue.to_owned()));
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.cluster_nodes.clone())
    }

    async fn scheduler_entries(&mut self) -> Result<Vec<SchedulerEntryInfo>, AdminError> {
        self.calls.push(MetadataCall::SchedulerEntries);
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.scheduler_entries.clone())
    }

    async fn list_scheduler_enqueue_events(
        &mut self,
        entry_id: &str,
        pagination: Pagination,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError> {
        self.calls.push(MetadataCall::SchedulerEvents {
            entry_id: entry_id.to_owned(),
            pagination,
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.scheduler_events.clone())
    }
}

pub(super) fn sample_server_info(id: &str) -> ServerInfo {
    ServerInfo::new(ServerInfoParts {
        id: id.to_owned(),
        host: "host".to_owned(),
        pid: 123,
        concurrency: 10,
        queues: HashMap::new(),
        strict_priority: false,
        started: UNIX_EPOCH,
        status: "active".to_owned(),
        active_workers: Vec::new(),
    })
}

pub(super) fn sample_scheduler_entry(id: &str) -> SchedulerEntryInfo {
    SchedulerEntryInfo::new(
        id.to_owned(),
        "* * * * *".to_owned(),
        Task::new("email:send", []),
        Vec::new(),
        UNIX_EPOCH,
        None,
    )
}
