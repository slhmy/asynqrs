use super::*;

#[tokio::test]
async fn serve_mux_dispatches_exact_and_longest_prefix_matches() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    let image_calls = Arc::clone(&calls);
    mux.handle_fn("image", move |task: &Task, _context: &ProcessingContext| {
        image_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("image:{}", task.type_name()));
        Ok(())
    });

    let thumbnail_calls = Arc::clone(&calls);
    mux.handle_fn(
        "image:thumbnail",
        move |task: &Task, _context: &ProcessingContext| {
            thumbnail_calls
                .lock()
                .expect("calls poisoned")
                .push(format!("thumbnail:{}", task.type_name()));
            Ok(())
        },
    );

    let exact_calls = Arc::clone(&calls);
    mux.handle_fn(
        "image:thumbnail:resize",
        move |task: &Task, _context: &ProcessingContext| {
            exact_calls
                .lock()
                .expect("calls poisoned")
                .push(format!("exact:{}", task.type_name()));
            Ok(())
        },
    );

    mux.process_task(
        &Task::new("image:thumbnail:resize", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();
    mux.process_task(
        &Task::new("image:thumbnail:crop", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();
    mux.process_task(
        &Task::new("image:raw", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "exact:image:thumbnail:resize",
            "thumbnail:image:thumbnail:crop",
            "image:image:raw"
        ]
    );
    assert_eq!(
        mux.matching_pattern("image:thumbnail:small"),
        Some("image:thumbnail")
    );
}
