use super::*;

pub(super) async fn wait_for_state(fixture: &mut RedisFixture, task_id: &str, state: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let stored: HashMap<String, Vec<u8>> = fixture
            .connection
            .hgetall(fixture.task_key(task_id))
            .unwrap();
        if !stored.is_empty() && string_field(&stored, "state") == state {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {state}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
