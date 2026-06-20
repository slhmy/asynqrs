//! Internal compatibility constants shared by implementation modules.

/// Maximum nanosecond duration representable by upstream-compatible duration
/// fields.
///
/// Reference: Go `time.Duration` is an `int64` nanosecond count, and Asynq
/// stores several duration fields using that representation before converting
/// them for Redis/protobuf boundaries.
/// <https://go.dev/src/time/time.go>.
pub(crate) const MAX_DURATION_NANOS_U128: u128 = i64::MAX as u128;

/// Signed form of [`MAX_DURATION_NANOS_U128`] for duration parsers that need to
/// model the negative lower bound.
pub(crate) const MAX_DURATION_NANOS_I128: i128 = i64::MAX as i128;

/// Absolute magnitude of the minimum signed duration value.
pub(crate) const MIN_DURATION_ABS_NANOS_I128: i128 = MAX_DURATION_NANOS_I128 + 1;
