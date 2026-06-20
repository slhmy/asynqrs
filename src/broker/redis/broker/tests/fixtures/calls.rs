use crate::broker::redis::{RedisArg, RedisScript};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::broker::redis::broker::tests) enum ExecutorCall {
    Close,
    Ping,
    Sadd {
        key: String,
        member: String,
    },
    Smembers {
        key: String,
    },
    Sismember {
        key: String,
        member: String,
    },
    Srem {
        key: String,
        member: String,
    },
    SetNxI64 {
        key: String,
        value: i64,
    },
    ZaddExisting {
        key: String,
        score: i64,
        member: String,
    },
    ZaddExistingMany {
        key: String,
        score: i64,
        members: Vec<String>,
    },
    Zadd {
        key: String,
        score: i64,
        member: Vec<u8>,
    },
    LrangeBytes {
        key: String,
        start: usize,
        stop: isize,
    },
    ZrevrangeBytes {
        key: String,
        start: isize,
        stop: isize,
    },
    Zrem {
        key: String,
        member: String,
    },
    Del {
        key: String,
    },
    GetBytes {
        key: String,
    },
    HgetBytes {
        key: String,
        field: String,
    },
    HvalsBytes {
        key: String,
    },
    Zscore {
        key: String,
        member: String,
    },
    HsetBytes {
        key: String,
        field: String,
        value: Vec<u8>,
    },
    Publish {
        channel: String,
        payload: String,
    },
    ClusterKeySlot {
        key: String,
    },
    ClusterSlots,
    EvalScriptInt {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptBytes {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptByteVec {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptStatus {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptValue {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
}
