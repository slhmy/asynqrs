#[cfg(all(feature = "macros", not(feature = "serde")))]
#[test]
fn task_payload_derive_requires_serde_feature() {
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let scratch_dir =
        std::env::temp_dir().join(format!("asynqrs-macros-only-{}", std::process::id()));

    let _ = fs::remove_dir_all(&scratch_dir);
    fs::create_dir_all(scratch_dir.join("src")).unwrap();
    fs::write(
        scratch_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "asynqrs-macros-only"
version = "0.0.0"
edition = "2024"

[dependencies]
asynqrs = {{ path = "{}", default-features = false, features = ["macros"] }}
"#,
            manifest_dir.display()
        ),
    )
    .unwrap();
    fs::copy(
        manifest_dir.join("tests/ui/task_payload_requires_serde_feature.rs"),
        scratch_dir.join("src/main.rs"),
    )
    .unwrap();

    let output = Command::new("cargo")
        .arg("check")
        .arg("--offline")
        .current_dir(&scratch_dir)
        .output()
        .unwrap();

    let _ = fs::remove_dir_all(&scratch_dir);

    assert!(
        !output.status.success(),
        "macros-only derived payload unexpectedly compiled"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("serde") && stderr.contains("encode_json_task_payload"),
        "expected serde-gated helper failure, got:\n{stderr}"
    );
}
