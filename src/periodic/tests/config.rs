use super::*;

#[test]
fn periodic_task_config_exposes_upstream_named_fields() {
    let task = Task::new("email:welcome", b"payload".to_vec());
    let config = PeriodicTaskConfig::new(
        "@every 1m",
        task.clone(),
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .max_retries(3),
    );

    assert_eq!(config.cronspec(), "@every 1m");
    assert_eq!(config.task(), &task);
    assert_eq!(
        config.options(),
        &EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .max_retries(3)
    );
}

#[test]
fn periodic_task_config_mutable_accessors_match_upstream_public_fields() {
    let mut config = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );

    config.cronspec_mut().clear();
    config.cronspec_mut().push_str("0 * * * *");
    *config.task_mut() = Task::new("email:reminder", b"updated".to_vec());
    *config.options_mut() = config.options().clone().max_retries(7);

    assert_eq!(config.cronspec(), "0 * * * *");
    assert_eq!(config.task().type_name(), "email:reminder");
    assert_eq!(config.task().payload(), b"updated");
    assert_eq!(
        config.options(),
        &EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .max_retries(7)
    );

    config.cronspec_mut().clear();
    config.cronspec_mut().push_str("@daily");
    *config.options_mut() = EnqueueOptions::new();

    assert_eq!(config.cronspec(), "@daily");
    assert_eq!(config.options(), &EnqueueOptions::new());
}
