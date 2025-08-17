//! Test Configuration Module
//!
//! Provides centralized control over test logging levels and behavior.
//! This module eliminates the need for conditional logging throughout the codebase
//! by using Zig's built-in std.testing.log_level control.
//!
//! Usage:
//! ```zig
//! const test_config = @import("test_config.zig");
//!
//! test "my clean test" {
//!     test_config.enable_quiet_mode(); // Only errors
//!     // ... test code with minimal log noise
//! }
//! ```

const std = @import("std");
const build_options = @import("build_options");

/// Automatically configure test output based on --debug flag
/// This should be called at the start of test files for consistent behavior
pub fn auto_configure_debug() void {
    if (build_options.debug_tests) {
        enable_debug_mode();
    } else {
        enable_standard_mode();
    }
}

/// Set log level to only show errors during tests.
/// Use this for tests that expect failures or inject corruption
/// where warning/info logs would create noise.
pub fn enable_quiet_mode() void {
    std.testing.log_level = .err;
}

/// Set log level to show warnings and errors.
/// Use this for most functional tests where you want to see
/// important issues but suppress debug chatter.
pub fn enable_standard_mode() void {
    std.testing.log_level = .warn;
}

/// Set log level to show info, warnings, and errors.
/// Use this for integration tests where you want to see
/// system state transitions and major operations.
pub fn enable_verbose_mode() void {
    std.testing.log_level = .info;
}

/// Set log level to show all logging output.
/// Use this for debugging test failures or developing new tests
/// where you need maximum visibility.
pub fn enable_debug_mode() void {
    std.testing.log_level = .debug;
}

/// Set log level for corruption/fault injection tests.
/// These tests intentionally cause failures and should suppress
/// expected warning output while showing actual errors.
pub fn enable_corruption_test_mode() void {
    std.testing.log_level = .err;
}

/// Set log level for performance/stress tests.
/// These tests may generate high-volume logging that would
/// obscure performance measurements and test results.
pub fn enable_performance_test_mode() void {
    std.testing.log_level = .err;
}

/// Set log level for memory safety tests.
/// These tests validate memory management and should show
/// warnings about leaks while suppressing debug noise.
pub fn enable_memory_test_mode() void {
    std.testing.log_level = .warn;
}

/// Set log level for recovery/simulation tests.
/// These tests simulate system failures and should show
/// recovery progress while suppressing expected failure noise.
pub fn enable_recovery_test_mode() void {
    std.testing.log_level = .info;
}

/// Configuration for different test categories based on file path patterns.
/// This can be used by test harnesses to automatically set appropriate
/// log levels based on test file location.
pub const TestCategory = enum {
    unit, // tests/ - standard mode
    integration, // tests/integration/ - verbose mode
    stress, // tests/stress/ - performance mode
    defensive, // tests/defensive/ - corruption mode
    fault_injection, // tests/fault_injection/ - corruption mode
    recovery, // tests/recovery/ - recovery mode
    performance, // tests/performance/ - performance mode
    memory, // tests/memory/ - memory mode

    /// Apply the appropriate log level for this test category
    pub fn apply(self: TestCategory) void {
        switch (self) {
            .unit => enable_standard_mode(),
            .integration => enable_verbose_mode(),
            .stress => enable_performance_test_mode(),
            .defensive => enable_corruption_test_mode(),
            .fault_injection => enable_corruption_test_mode(),
            .recovery => enable_recovery_test_mode(),
            .performance => enable_performance_test_mode(),
            .memory => enable_memory_test_mode(),
        }
    }
};

/// Automatically detect test category from source location and apply
/// appropriate log level. Call this at the start of test files for
/// automatic configuration.
pub fn auto_configure(src: std.builtin.SourceLocation) void {
    const file_path = src.file;

    if (std.mem.indexOf(u8, file_path, "tests/integration/") != null) {
        TestCategory.integration.apply();
    } else if (std.mem.indexOf(u8, file_path, "tests/stress/") != null) {
        TestCategory.stress.apply();
    } else if (std.mem.indexOf(u8, file_path, "tests/defensive/") != null) {
        TestCategory.defensive.apply();
    } else if (std.mem.indexOf(u8, file_path, "tests/fault_injection/") != null or
        std.mem.indexOf(u8, file_path, "fault_injection") != null)
    {
        TestCategory.fault_injection.apply();
    } else if (std.mem.indexOf(u8, file_path, "tests/recovery/") != null) {
        TestCategory.recovery.apply();
    } else if (std.mem.indexOf(u8, file_path, "tests/performance/") != null or
        std.mem.indexOf(u8, file_path, "benchmark") != null)
    {
        TestCategory.performance.apply();
    } else if (std.mem.indexOf(u8, file_path, "tests/memory/") != null or
        std.mem.indexOf(u8, file_path, "arena") != null)
    {
        TestCategory.memory.apply();
    } else {
        // Default for unit tests and unclassified tests
        TestCategory.unit.apply();
    }
}

/// Test helper to temporarily change log level for a specific operation.
/// Restores the previous level after the operation completes.
pub fn with_log_level(comptime level: std.log.Level, operation: anytype) void {
    const old_level = std.testing.log_level;
    defer std.testing.log_level = old_level;

    std.testing.log_level = level;
    operation();
}

/// Check if debug output is enabled (from --debug flag)
pub fn is_debug() bool {
    return build_options.debug_tests;
}

/// Print to stdout only if debug mode is enabled
/// This allows demo and diagnostic tests to be quiet by default
pub fn debug_print(comptime fmt: []const u8, args: anytype) void {
    if (is_debug()) {
        std.debug.print(fmt, args);
    }
}

/// Test utilities for common logging scenarios
pub const TestUtils = struct {
    /// Run a test operation with quiet logging (errors only)
    pub fn run_quiet(operation: anytype) void {
        with_log_level(.err, operation);
    }

    /// Run a test operation with verbose logging (info and above)
    pub fn run_verbose(operation: anytype) void {
        with_log_level(.info, operation);
    }

    /// Run a test operation with debug logging (all levels)
    pub fn run_debug(operation: anytype) void {
        with_log_level(.debug, operation);
    }
};

// Test the module functionality
test "log level control" {
    // Test that we can set different log levels
    enable_quiet_mode();
    enable_standard_mode();
    enable_verbose_mode();
    enable_debug_mode();

    // Test category-based configuration
    TestCategory.defensive.apply();
    TestCategory.integration.apply();

    // Test auto-configuration (won't actually detect this file's category
    // since we're not in a tests/ directory, but exercises the logic)
    auto_configure(@src());
}

test "test utils functionality" {
    // Test temporary log level changes
    enable_standard_mode(); // Start with standard

    TestUtils.run_quiet(struct {
        fn op() void {
            // This would run with .err level
        }
    }.op);

    // Level should be restored to standard after the operation
}
