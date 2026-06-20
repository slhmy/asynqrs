use super::*;

#[test]
fn default_server_metadata_rejects_zero_concurrency() {
    let selector = QueueSelector::weighted_priority([("critical", 1)]).unwrap();

    assert_eq!(
        ServerMetadata::for_current_process_with_queue_selector(&selector, 0).unwrap_err(),
        ServerError::EmptyWorkerCount
    );
}

#[test]
fn server_metadata_validates_required_fields() {
    assert_eq!(
        ServerMetadata::new(
            " ",
            123,
            "server-id",
            b"server-info".to_vec(),
            ["worker"],
            Duration::from_secs(30)
        )
        .unwrap_err(),
        ServerError::EmptyMetadataHostname
    );
    assert_eq!(
        ServerMetadata::new(
            "host",
            123,
            "",
            b"server-info".to_vec(),
            ["worker"],
            Duration::from_secs(30)
        )
        .unwrap_err(),
        ServerError::EmptyMetadataServerId
    );
    assert_eq!(
        ServerMetadata::new(
            "host",
            123,
            "server-id",
            Vec::new(),
            ["worker"],
            Duration::ZERO
        )
        .unwrap_err(),
        ServerError::EmptyMetadataServerInfo
    );
    assert_eq!(
        ServerMetadata::new(
            "host",
            123,
            "server-id",
            b"server-info".to_vec(),
            ["worker"],
            Duration::ZERO
        )
        .unwrap_err(),
        ServerError::ZeroMetadataTtl
    );
}
