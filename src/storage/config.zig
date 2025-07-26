//! Storage engine configuration validation and management.
//!
//! Provides type-safe configuration with validation to prevent common
//! operational issues like OOM crashes from oversized memtables or
//! excessive flush overhead from undersized memtables.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;

/// Default maximum memory size for memtable before flushing to SSTable (128MB).
/// Size chosen to balance memory usage with flush frequency - smaller values
/// cause excessive I/O overhead, larger values risk OOM with large blocks.
pub const DEFAULT_MEMTABLE_MAX_SIZE: u64 = 128 * 1024 * 1024;

/// Configuration validation errors.
pub const ConfigError = error{
    /// Memtable max size too small (must be at least 1MB for testing, 16MB+ recommended for production)
    MemtableMaxSizeTooSmall,
    /// Memtable max size too large (must be at most 1GB to prevent OOM)
    MemtableMaxSizeTooLarge,
};

/// Configuration options for the storage engine.
pub const Config = struct {
    /// Maximum memory size for memtable before flushing to SSTable.
    /// Prevents unpredictable memory usage and potential OOM crashes with large blocks.
    memtable_max_size: u64 = DEFAULT_MEMTABLE_MAX_SIZE,

    /// Validate configuration parameters for operational safety.
    /// Production deployments should use 16MB+ for memtable_max_size to avoid
    /// excessive flush overhead, but testing allows 1MB+ for faster iteration.
    pub fn validate(self: Config) ConfigError!void {
        if (self.memtable_max_size < 1024 * 1024) {
            return ConfigError.MemtableMaxSizeTooSmall;
        }
        if (self.memtable_max_size > 1024 * 1024 * 1024) {
            return ConfigError.MemtableMaxSizeTooLarge;
        }
    }

    /// Create a minimal configuration suitable for testing environments.
    /// Uses 1MB memtable to enable fast test iteration without excessive memory usage.
    pub fn minimal_for_testing() Config {
        return Config{
            .memtable_max_size = 1024 * 1024, // 1MB
        };
    }

    /// Create a production-optimized configuration.
    /// Uses 256MB memtable to balance memory usage with flush frequency for production workloads.
    pub fn production_optimized() Config {
        return Config{
            .memtable_max_size = 256 * 1024 * 1024, // 256MB
        };
    }
};

// Tests
const testing = std.testing;

test "config validation accepts default configuration" {
    const config = Config{};
    try config.validate();
}

test "config validation accepts minimum size for testing" {
    const config = Config{
        .memtable_max_size = 1024 * 1024, // 1MB - minimum allowed
    };
    try config.validate();
}

test "config validation accepts maximum size" {
    const config = Config{
        .memtable_max_size = 1024 * 1024 * 1024, // 1GB - maximum allowed
    };
    try config.validate();
}

test "config validation rejects undersized memtable" {
    const config = Config{
        .memtable_max_size = 512 * 1024, // 512KB - too small
    };
    const result = config.validate();
    try testing.expectError(ConfigError.MemtableMaxSizeTooSmall, result);
}

test "config validation rejects oversized memtable" {
    const config = Config{
        .memtable_max_size = 2 * 1024 * 1024 * 1024, // 2GB - too large
    };
    const result = config.validate();
    try testing.expectError(ConfigError.MemtableMaxSizeTooLarge, result);
}

test "minimal testing config is valid and optimized for fast iteration" {
    const config = Config.minimal_for_testing();
    try config.validate();
    try testing.expectEqual(@as(u64, 1024 * 1024), config.memtable_max_size);
}

test "production config is valid and optimized for throughput" {
    const config = Config.production_optimized();
    try config.validate();
    try testing.expectEqual(@as(u64, 256 * 1024 * 1024), config.memtable_max_size);
}

test "config validation boundary conditions" {

    // Test just below minimum (should fail)
    {
        const config = Config{ .memtable_max_size = 1024 * 1024 - 1 };
        try testing.expectError(ConfigError.MemtableMaxSizeTooSmall, config.validate());
    }

    // Test just above maximum (should fail)
    {
        const config = Config{ .memtable_max_size = 1024 * 1024 * 1024 + 1 };
        try testing.expectError(ConfigError.MemtableMaxSizeTooLarge, config.validate());
    }
}
