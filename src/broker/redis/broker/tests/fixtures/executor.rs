use super::*;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use crate::broker::redis::executor::RedisSlotRange;
use crate::broker::redis::{RedisDequeueCall, RedisExecutor, RedisScriptCall};

#[derive(Debug)]
pub(in crate::broker::redis::broker::tests) struct FakeExecutor {
    pub(in crate::broker::redis::broker::tests) calls: Vec<ExecutorCall>,
    pub(in crate::broker::redis::broker::tests) script_int_results: Vec<i64>,
    pub(in crate::broker::redis::broker::tests) script_bytes_results: Vec<Option<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) script_byte_vec_results: Vec<Vec<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) script_status_results: Vec<String>,
    pub(in crate::broker::redis::broker::tests) script_value_results: Vec<redis::Value>,
    pub(in crate::broker::redis::broker::tests) smembers_results: Vec<Vec<String>>,
    pub(in crate::broker::redis::broker::tests) sismember_results: Vec<bool>,
    pub(in crate::broker::redis::broker::tests) set_nx_results: Vec<bool>,
    pub(in crate::broker::redis::broker::tests) zadd_existing_results: Vec<usize>,
    pub(in crate::broker::redis::broker::tests) zscore_results: Vec<f64>,
    pub(in crate::broker::redis::broker::tests) lrange_bytes_results: Vec<Vec<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) zrevrange_bytes_results: Vec<Vec<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) get_bytes_results: Vec<Option<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) get_bytes_result_results:
        Vec<Result<Option<Vec<u8>>, RedisExecutorError>>,
    pub(in crate::broker::redis::broker::tests) hget_bytes_results: Vec<Option<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) hvals_bytes_results: Vec<Vec<Vec<u8>>>,
    pub(in crate::broker::redis::broker::tests) hvals_bytes_result_results:
        Vec<Result<Vec<Vec<u8>>, RedisExecutorError>>,
    pub(in crate::broker::redis::broker::tests) sadd_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) smembers_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) srem_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) set_nx_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) zadd_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) zscore_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) hset_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) publish_results: Vec<usize>,
    pub(in crate::broker::redis::broker::tests) cluster_key_slot_results: Vec<i64>,
    pub(in crate::broker::redis::broker::tests) cluster_slots_results: Vec<Vec<RedisSlotRange>>,
    pub(in crate::broker::redis::broker::tests) ping_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) publish_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) cluster_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) zrem_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) del_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) script_error: Option<RedisExecutorError>,
    pub(in crate::broker::redis::broker::tests) call_log: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl Default for FakeExecutor {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            script_int_results: vec![1],
            script_bytes_results: Vec::new(),
            script_byte_vec_results: Vec::new(),
            script_status_results: vec!["OK".to_owned()],
            script_value_results: Vec::new(),
            smembers_results: Vec::new(),
            sismember_results: vec![true],
            set_nx_results: vec![true],
            zadd_existing_results: vec![1],
            zscore_results: vec![1_700_000_120.0],
            lrange_bytes_results: Vec::new(),
            zrevrange_bytes_results: Vec::new(),
            get_bytes_results: Vec::new(),
            get_bytes_result_results: Vec::new(),
            hget_bytes_results: Vec::new(),
            hvals_bytes_results: Vec::new(),
            hvals_bytes_result_results: Vec::new(),
            sadd_error: None,
            smembers_error: None,
            srem_error: None,
            set_nx_error: None,
            zadd_error: None,
            zscore_error: None,
            hset_error: None,
            publish_results: vec![1],
            cluster_key_slot_results: vec![12182],
            cluster_slots_results: Vec::new(),
            ping_error: None,
            publish_error: None,
            cluster_error: None,
            zrem_error: None,
            del_error: None,
            script_error: None,
            call_log: None,
        }
    }
}

#[async_trait]
impl RedisExecutor for FakeExecutor {
    fn close(&mut self) -> Result<(), RedisExecutorError> {
        self.calls.push(ExecutorCall::Close);
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RedisExecutorError> {
        self.calls.push(ExecutorCall::Ping);
        if let Some(error) = self.ping_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        self.calls.push(ExecutorCall::Sadd {
            key: key.to_owned(),
            member: member.to_owned(),
        });
        if let Some(error) = self.sadd_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn smembers(&mut self, key: &str) -> Result<Vec<String>, RedisExecutorError> {
        self.calls.push(ExecutorCall::Smembers {
            key: key.to_owned(),
        });
        if let Some(error) = self.smembers_error.clone() {
            return Err(error);
        }
        Ok(self.smembers_results.remove(0))
    }

    async fn sismember(&mut self, key: &str, member: &str) -> Result<bool, RedisExecutorError> {
        if let Some(call_log) = &self.call_log {
            call_log.lock().unwrap().push("sismember");
        }
        self.calls.push(ExecutorCall::Sismember {
            key: key.to_owned(),
            member: member.to_owned(),
        });
        if let Some(error) = self.smembers_error.clone() {
            return Err(error);
        }
        Ok(self.sismember_results.remove(0))
    }

    async fn srem(&mut self, key: &str, member: &str) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::Srem {
            key: key.to_owned(),
            member: member.to_owned(),
        });
        if let Some(error) = self.srem_error.clone() {
            return Err(error);
        }
        Ok(1)
    }

    async fn set_nx_i64(&mut self, key: &str, value: i64) -> Result<bool, RedisExecutorError> {
        self.calls.push(ExecutorCall::SetNxI64 {
            key: key.to_owned(),
            value,
        });
        if let Some(error) = self.set_nx_error.clone() {
            return Err(error);
        }
        Ok(self.set_nx_results.remove(0))
    }

    async fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::ZaddExisting {
            key: key.to_owned(),
            score,
            member: member.to_owned(),
        });
        if let Some(error) = self.zadd_error.clone() {
            return Err(error);
        }
        Ok(self.zadd_existing_results.remove(0))
    }

    async fn zadd_existing_many(
        &mut self,
        key: &str,
        score: i64,
        members: &[String],
    ) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::ZaddExistingMany {
            key: key.to_owned(),
            score,
            members: members.to_vec(),
        });
        if let Some(error) = self.zadd_error.clone() {
            return Err(error);
        }
        Ok(self.zadd_existing_results.remove(0))
    }

    async fn zadd(
        &mut self,
        key: &str,
        score: i64,
        member: &[u8],
    ) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::Zadd {
            key: key.to_owned(),
            score,
            member: member.to_vec(),
        });
        if let Some(error) = self.zadd_error.clone() {
            return Err(error);
        }
        Ok(1)
    }

    async fn lrange_bytes(
        &mut self,
        key: &str,
        start: usize,
        stop: isize,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::LrangeBytes {
            key: key.to_owned(),
            start,
            stop,
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.lrange_bytes_results.remove(0))
    }

    async fn zrevrange_bytes(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::ZrevrangeBytes {
            key: key.to_owned(),
            start,
            stop,
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.zrevrange_bytes_results.remove(0))
    }

    async fn zrem(&mut self, key: &str, member: &str) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::Zrem {
            key: key.to_owned(),
            member: member.to_owned(),
        });
        if let Some(error) = self.zrem_error.clone() {
            return Err(error);
        }
        Ok(1)
    }

    async fn del(&mut self, key: &str) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::Del {
            key: key.to_owned(),
        });
        if let Some(error) = self.del_error.clone() {
            return Err(error);
        }
        Ok(1)
    }

    async fn hget_bytes(
        &mut self,
        key: &str,
        field: &str,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::HgetBytes {
            key: key.to_owned(),
            field: field.to_owned(),
        });
        if let Some(error) = self.hset_error.clone() {
            return Err(error);
        }
        Ok(self.hget_bytes_results.remove(0))
    }

    async fn get_bytes(&mut self, key: &str) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::GetBytes {
            key: key.to_owned(),
        });
        if let Some(error) = self.hset_error.clone() {
            return Err(error);
        }
        if !self.get_bytes_result_results.is_empty() {
            return self.get_bytes_result_results.remove(0);
        }
        Ok(self.get_bytes_results.remove(0))
    }

    async fn hvals_bytes(&mut self, key: &str) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::HvalsBytes {
            key: key.to_owned(),
        });
        if let Some(error) = self.hset_error.clone() {
            return Err(error);
        }
        if !self.hvals_bytes_result_results.is_empty() {
            return self.hvals_bytes_result_results.remove(0);
        }
        Ok(self.hvals_bytes_results.remove(0))
    }

    async fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptInt {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_int_results.remove(0))
    }

    async fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptBytes {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_bytes_results.remove(0))
    }

    async fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptByteVec {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_byte_vec_results.remove(0))
    }

    async fn eval_script_status(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<String, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptStatus {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_status_results.remove(0))
    }

    async fn eval_script_value(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<redis::Value, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptValue {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_value_results.remove(0))
    }

    async fn zscore(&mut self, key: &str, member: &str) -> Result<f64, RedisExecutorError> {
        self.calls.push(ExecutorCall::Zscore {
            key: key.to_owned(),
            member: member.to_owned(),
        });
        if let Some(error) = self.zscore_error.clone() {
            return Err(error);
        }
        Ok(self.zscore_results.remove(0))
    }

    async fn hset_bytes(
        &mut self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::HsetBytes {
            key: key.to_owned(),
            field: field.to_owned(),
            value: value.to_vec(),
        });
        if let Some(error) = self.hset_error.clone() {
            return Err(error);
        }
        Ok(1)
    }

    async fn publish(&mut self, channel: &str, payload: &str) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::Publish {
            channel: channel.to_owned(),
            payload: payload.to_owned(),
        });
        if let Some(error) = self.publish_error.clone() {
            return Err(error);
        }
        Ok(self.publish_results.remove(0))
    }

    async fn cluster_key_slot(&mut self, key: &str) -> Result<i64, RedisExecutorError> {
        self.calls.push(ExecutorCall::ClusterKeySlot {
            key: key.to_owned(),
        });
        if let Some(error) = self.cluster_error.clone() {
            return Err(error);
        }
        Ok(self.cluster_key_slot_results.remove(0))
    }

    async fn cluster_slots(&mut self) -> Result<Vec<RedisSlotRange>, RedisExecutorError> {
        self.calls.push(ExecutorCall::ClusterSlots);
        if let Some(error) = self.cluster_error.clone() {
            return Err(error);
        }
        Ok(self.cluster_slots_results.remove(0))
    }
}
