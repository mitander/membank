//! Fuzzing coordinator for bug discovery and system hardening.
//!
//! Coordinates specialized fuzzing modules targeting storage, query, network,
//! and serialization subsystems. Uses deterministic simulation framework
//! to enable reproducible bug discovery with precise failure reproduction.
//!
//! Design rationale: Modular fuzzing architecture enables focused testing
//! of individual subsystems while maintaining integration test coverage.
//! Deterministic execution ensures bugs can be reliably reproduced and
//! validated after fixes.

const fuzz_main = @import("fuzz/main.zig");

// Re-export main function for build system compatibility
pub const main = fuzz_main.main;
