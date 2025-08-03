//! KausalDB fuzzing entry point.
//!
//! Coordinates modular fuzzing system with specialized modules for different
//! components. Provides backward compatibility while delegating to focused modules.

const fuzz_main = @import("fuzz/main.zig");

// Re-export main function for build system compatibility
pub const main = fuzz_main.main;
