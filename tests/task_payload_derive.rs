#[cfg(all(feature = "macros", feature = "serde"))]
#[test]
fn task_payload_derive_ui() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/task_payload_pass.rs");
    tests.compile_fail("tests/ui/task_payload_missing_task_type.rs");
    tests.compile_fail("tests/ui/task_payload_blank_task_type.rs");
    tests.compile_fail("tests/ui/task_payload_non_string_task_type.rs");
    tests.compile_fail("tests/ui/task_payload_duplicate_task_type.rs");
    tests.compile_fail("tests/ui/serve_mux_non_payload.rs");
}
