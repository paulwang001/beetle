//! luffa exit codes
//!
//! Exit code constants intended to be passed to
//! `std::process::exit()`

/// Alias for the numeric type that holds luffa exit codes.
pub type LuffaExitCode = i32;

/// Successful exit
pub const OK: LuffaExitCode = 0;

/// Generic error exit
pub const ERROR: LuffaExitCode = 1;

/// Cannot acquire a resource lock
pub const LOCKED: LuffaExitCode = 2;
