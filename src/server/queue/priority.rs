/// Converts a Rust integer into an Asynq queue priority.
///
/// Reference: Asynq v0.26.0 `Config.Queues` uses signed `int` values and
/// ignores zero or negative queue priorities:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L151-L170>.
pub trait QueuePriority {
    fn into_queue_priority(self) -> Option<usize>;
}

macro_rules! impl_unsigned_queue_priority {
    ($($ty:ty),* $(,)?) => {
        $(
            impl QueuePriority for $ty {
                fn into_queue_priority(self) -> Option<usize> {
                    usize::try_from(self).ok().filter(|priority| *priority > 0)
                }
            }
        )*
    };
}

macro_rules! impl_signed_queue_priority {
    ($($ty:ty),* $(,)?) => {
        $(
            impl QueuePriority for $ty {
                fn into_queue_priority(self) -> Option<usize> {
                    if self <= 0 {
                        None
                    } else {
                        usize::try_from(self).ok()
                    }
                }
            }
        )*
    };
}

impl_unsigned_queue_priority!(u8, u16, u32, u64, u128, usize);
impl_signed_queue_priority!(i8, i16, i32, i64, i128, isize);
