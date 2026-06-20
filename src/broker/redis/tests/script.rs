use super::*;

#[test]
fn script_result_mapping_documents_unique_duplicate_code() {
    assert_eq!(
        RedisScript::EnqueueUnique.result_for_code(-1),
        Some(RedisScriptResult::DuplicateTask)
    );
}
