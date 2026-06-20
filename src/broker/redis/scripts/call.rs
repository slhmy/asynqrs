use crate::broker::redis::{RedisArg, RedisScript, RedisScriptCall};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisScriptCallError {
    #[error("{} script expected {expected} keys, got {actual}", script.name())]
    WrongKeyCount {
        script: RedisScript,
        expected: usize,
        actual: usize,
    },
    #[error("{} script expected {expected} args, got {actual}", script.name())]
    WrongArgCount {
        script: RedisScript,
        expected: usize,
        actual: usize,
    },
}

impl RedisScript {
    pub fn validate_call(
        self,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<(), RedisScriptCallError> {
        let spec = self.spec();
        if keys.len() != spec.key_count() {
            if matches!(self, Self::HistoricalQueueStats) {
                if keys.is_empty() || keys.len() % 2 != 0 {
                    return Err(RedisScriptCallError::WrongKeyCount {
                        script: self,
                        expected: spec.key_count(),
                        actual: keys.len(),
                    });
                }
            } else {
                return Err(RedisScriptCallError::WrongKeyCount {
                    script: self,
                    expected: spec.key_count(),
                    actual: keys.len(),
                });
            }
        }
        if !spec.matches_arg_count(args.len()) {
            return Err(RedisScriptCallError::WrongArgCount {
                script: self,
                expected: spec.min_arg_count(),
                actual: args.len(),
            });
        }
        Ok(())
    }
}

impl RedisScriptCall {
    pub fn validate(&self) -> Result<(), RedisScriptCallError> {
        self.script().validate_call(self.keys(), self.args())
    }
}
