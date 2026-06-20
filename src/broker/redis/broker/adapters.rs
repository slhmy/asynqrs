//! Trait adapters that let `RedisBroker` satisfy crate workflow boundaries.
//!
//! Redis command behavior lives in `operations`. This module only bridges those
//! inherent methods to the traits consumed by clients, server workers,
//! schedulers, and aggregation runners.

mod aggregation;
mod client;
mod inspector;
mod metadata;
mod scheduler;
mod worker;
