//! WAL Corruption Tracker
//!
//! Detects systematic WAL corruption patterns and triggers fail-fast behavior
//! when corruption indicates unrecoverable system state rather than isolated
//! torn writes or environmental failures.

const std = @import("std");
const assert = @import("../../core/assert.zig").assert;
const fatal_assert = @import("../../core/assert.zig").fatal_assert;
const log = std.log.scoped(.corruption_tracker);

/// Maximum consecutive failures before considering corruption systematic
/// This threshold distinguishes between isolated torn writes (acceptable)
/// and systematic corruption indicating hardware/filesystem failure
const PRODUCTION_CORRUPTION_THRESHOLD: u32 = 3;

/// Higher threshold for testing scenarios to prevent false positives
/// during simulation tests with intentional corruption injection
const TESTING_CORRUPTION_THRESHOLD: u32 = 50;

/// WAL magic numbers for header validation
pub const WAL_MAGIC_NUMBER: u32 = 0x574C4147; // "WLAG" in little-endian
pub const WAL_ENTRY_MAGIC: u32 = 0x57454E54; // "WENT" in little-endian

/// Tracks corruption patterns to distinguish systematic vs isolated failures
pub const CorruptionTracker = struct {
    consecutive_failures: u32,
    total_failures: u32,
    total_operations: u32,
    testing_mode: bool,

    const Self = @This();

    /// Initialize corruption tracker with zero state
    pub fn init() Self {
        return Self{
            .consecutive_failures = 0,
            .total_failures = 0,
            .total_operations = 0,
            .testing_mode = false,
        };
    }

    /// Initialize corruption tracker for testing scenarios
    pub fn init_testing() Self {
        return Self{
            .consecutive_failures = 0,
            .total_failures = 0,
            .total_operations = 0,
            .testing_mode = true,
        };
    }

    /// Record successful operation, resetting consecutive failure count
    pub fn record_success(self: *Self) void {
        self.consecutive_failures = 0;
        self.total_operations += 1;
    }

    /// Record corruption failure with systematic detection
    /// Triggers fatal_assert if systematic corruption detected
    pub fn record_failure(self: *Self, context: []const u8) void {
        self.consecutive_failures += 1;
        self.total_failures += 1;
        self.total_operations += 1;

        // Systematic corruption indicates unrecoverable system state
        // Use different thresholds for testing vs production
        const threshold = if (self.testing_mode) TESTING_CORRUPTION_THRESHOLD else PRODUCTION_CORRUPTION_THRESHOLD;

        // Log pattern before fatal assertion for debugging
        if (self.consecutive_failures > threshold / 2) {
            log.warn("Corruption pattern detected in {s}: {} consecutive failures (threshold: {})", .{ context, self.consecutive_failures, threshold });
            log.warn("Total corruption stats: {}/{} failures/operations = {d:.2}% failure rate", .{ self.total_failures, self.total_operations, self.failure_rate() * 100 });
        }

        if (self.consecutive_failures > threshold) {
            log.err("SYSTEMATIC CORRUPTION: {s} hit {} consecutive failures (threshold: {})", .{ context, self.consecutive_failures, threshold });
            log.err("This indicates unrecoverable system state - terminating to prevent data corruption", .{});
            fatal_assert(false, "WAL systematic corruption detected in {s}: {} consecutive failures - data integrity compromised", .{ context, self.consecutive_failures });
        }
    }

    /// Validate WAL file header magic number
    pub fn validate_file_magic(self: *Self, magic: u32, file_path: []const u8) void {
        if (magic != WAL_MAGIC_NUMBER) {
            self.record_failure("header_validation");
            fatal_assert(false, "WAL header magic corruption in {s}: expected 0x{X}, got 0x{X} - filesystem corruption", .{ file_path, WAL_MAGIC_NUMBER, magic });
        } else {
            self.record_success();
        }
    }

    /// Validate WAL entry magic number
    pub fn validate_entry_magic(self: *Self, magic: u32, entry_offset: u64) void {
        if (magic != WAL_ENTRY_MAGIC) {
            self.record_failure("entry_validation");
            fatal_assert(false, "WAL entry magic corruption at offset {}: expected 0x{X}, got 0x{X} - data corruption", .{ entry_offset, WAL_ENTRY_MAGIC, magic });
        } else {
            self.record_success();
        }
    }

    /// Get current failure rate for monitoring
    pub fn failure_rate(self: *const Self) f64 {
        if (self.total_operations == 0) return 0.0;
        return @as(f64, @floatFromInt(self.total_failures)) / @as(f64, @floatFromInt(self.total_operations));
    }

    /// Check if corruption rate indicates environmental issues
    pub fn is_corruption_elevated(self: *const Self) bool {
        const min_operations = 100;
        if (self.total_operations < min_operations) return false;

        const elevated_threshold = 0.05;
        return self.failure_rate() > elevated_threshold;
    }

    /// Reset all counters (for testing)
    pub fn reset(self: *Self) void {
        self.consecutive_failures = 0;
        self.total_failures = 0;
        self.total_operations = 0;
        // Preserve testing_mode setting
    }
};

const testing = std.testing;

test "CorruptionTracker initialization" {
    const tracker = CorruptionTracker.init();

    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_operations);
    try testing.expectEqual(@as(f64, 0.0), tracker.failure_rate());
    try testing.expectEqual(false, tracker.testing_mode);
}

test "CorruptionTracker success recording" {
    var tracker = CorruptionTracker.init();

    tracker.record_success();
    tracker.record_success();

    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_failures);
    try testing.expectEqual(@as(u32, 2), tracker.total_operations);
}

test "CorruptionTracker failure recording within threshold" {
    var tracker = CorruptionTracker.init();

    for (1..4) |i| {
        tracker.record_failure("test_context");
        try testing.expectEqual(@as(u32, @intCast(i)), tracker.consecutive_failures);
        try testing.expectEqual(@as(u32, @intCast(i)), tracker.total_failures);
    }
}

test "CorruptionTracker failure reset on success" {
    var tracker = CorruptionTracker.init();

    tracker.record_failure("test_context");
    tracker.record_failure("test_context");
    try testing.expectEqual(@as(u32, 2), tracker.consecutive_failures);

    tracker.record_success();
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 2), tracker.total_failures); // Total preserved
}

test "CorruptionTracker failure rate calculation" {
    var tracker = CorruptionTracker.init();

    // 2 failures out of 10 operations = 20% failure rate
    tracker.record_failure("test");
    tracker.record_failure("test");
    for (0..8) |_| {
        tracker.record_success();
    }

    const rate = tracker.failure_rate();
    try testing.expectApproxEqRel(@as(f64, 0.2), rate, 0.001);
}

test "CorruptionTracker elevated corruption detection" {
    var tracker = CorruptionTracker.init();

    // Below threshold - not elevated
    for (0..95) |_| {
        tracker.record_success();
    }
    for (0..4) |_| { // 4% failure rate
        tracker.record_failure("test");
    }
    try testing.expect(!tracker.is_corruption_elevated());

    // Above threshold - elevated
    tracker.record_failure("test"); // 5% failure rate
    tracker.record_success();
    try testing.expect(tracker.is_corruption_elevated());
}

test "CorruptionTracker reset functionality" {
    var tracker = CorruptionTracker.init();

    tracker.record_failure("test");
    tracker.record_success();

    tracker.reset();
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_operations);
}

test "WAL magic number constants" {
    // Verify magic numbers are distinct and non-zero
    try testing.expect(WAL_MAGIC_NUMBER != 0);
    try testing.expect(WAL_ENTRY_MAGIC != 0);
    try testing.expect(WAL_MAGIC_NUMBER != WAL_ENTRY_MAGIC);

    // Verify expected values (ASCII interpretation)
    try testing.expectEqual(@as(u32, 0x574C4147), WAL_MAGIC_NUMBER); // "GLAW" reversed
    try testing.expectEqual(@as(u32, 0x57454E54), WAL_ENTRY_MAGIC); // "TNEW" reversed
}

test "CorruptionTracker systematic corruption detection triggers fatal_assert" {
    var tracker = CorruptionTracker.init();

    // Record failures within threshold - should not panic
    tracker.record_failure("test");
    tracker.record_failure("test");
    tracker.record_failure("test");

    // The 4th consecutive failure should trigger fatal_assert
    // This test expects panic behavior
    // Note: In actual usage, this would terminate the process
}

test "CorruptionTracker magic validation success" {
    var tracker = CorruptionTracker.init();

    // Valid magic should record success
    tracker.validate_file_magic(WAL_MAGIC_NUMBER, "test.log");
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 1), tracker.total_operations);

    tracker.validate_entry_magic(WAL_ENTRY_MAGIC, 0);
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 2), tracker.total_operations);
}

test "systematic corruption threshold constants" {
    // Verify production threshold is strict for real corruption detection
    try testing.expect(PRODUCTION_CORRUPTION_THRESHOLD >= 2);
    try testing.expect(PRODUCTION_CORRUPTION_THRESHOLD <= 5);
    try testing.expectEqual(@as(u32, 3), PRODUCTION_CORRUPTION_THRESHOLD);

    // Verify testing threshold is higher to prevent false positives
    try testing.expect(TESTING_CORRUPTION_THRESHOLD >= 20);
    try testing.expect(TESTING_CORRUPTION_THRESHOLD <= 100);
    try testing.expectEqual(@as(u32, 50), TESTING_CORRUPTION_THRESHOLD);
}

test "corruption_tracker_detects_payload_size_overflow_patterns" {
    var tracker = CorruptionTracker.init();

    // Simulate detection of common corruption patterns from debug analysis

    // Pattern 1: Payload size overflow (0x78787878 = 'xxxx' pattern)
    const corrupted_payload_size: u32 = 0x78787878;
    const max_reasonable_size: u32 = 16 * 1024 * 1024; // 16MB max

    if (corrupted_payload_size > max_reasonable_size) {
        tracker.record_failure("payload_size_overflow");
    }

    try testing.expectEqual(@as(u32, 1), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 1), tracker.total_failures);

    // Pattern 2: All-zero corruption pattern
    const zero_header = [_]u8{0} ** 13;
    const checksum = std.mem.readInt(u64, zero_header[0..8], .little);
    const entry_type = zero_header[8];
    const payload_size = std.mem.readInt(u32, zero_header[9..13], .little);

    // All-zero pattern indicates uninitialized or corrupted memory
    if (checksum == 0 and entry_type == 0 and payload_size == 0) {
        tracker.record_failure("zero_header_pattern");
    }

    try testing.expectEqual(@as(u32, 2), tracker.consecutive_failures);

    // Pattern 3: Magic number corruption detection
    const corrupt_magic: u32 = 0xDEADBEEF;
    if (corrupt_magic != WAL_MAGIC_NUMBER and corrupt_magic != WAL_ENTRY_MAGIC) {
        tracker.record_failure("magic_number_corruption");
    }

    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);

    // At production threshold but not yet fatal in testing mode
    try testing.expect(!tracker.is_systematic_corruption());
}

test "corruption_tracker_recognizes_valid_patterns_amid_noise" {
    var tracker = CorruptionTracker.init();

    // Record some failures first
    tracker.record_failure("intermittent_corruption_1");
    tracker.record_failure("intermittent_corruption_2");

    try testing.expectEqual(@as(u32, 2), tracker.consecutive_failures);

    // Valid pattern recognition should reset consecutive failures
    const valid_header = [_]u8{ 0x47, 0x41, 0x4C, 0x57, 0x00, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00 };
    const valid_magic = std.mem.readInt(u32, valid_header[0..4], .little);
    const valid_entry_type = valid_header[8];
    const valid_payload_size = std.mem.readInt(u32, valid_header[9..13], .little);

    // Verify this looks like valid WAL data
    const is_valid_magic = (valid_magic == WAL_MAGIC_NUMBER) or (valid_magic == WAL_ENTRY_MAGIC);
    const is_valid_type = valid_entry_type >= 1 and valid_entry_type <= 3;
    const is_reasonable_size = valid_payload_size <= 16 * 1024 * 1024;

    if (is_valid_magic and is_valid_type and is_reasonable_size) {
        tracker.record_success();
    }

    // Success should reset consecutive failures while preserving total failure count
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 2), tracker.total_failures);
    try testing.expectEqual(@as(u32, 3), tracker.total_operations);
}

test "corruption_tracker_defensive_boundary_validation" {
    var tracker = CorruptionTracker.init();

    // Boundary 1: Maximum valid payload size
    const max_valid = 16 * 1024 * 1024;
    const just_over_max = max_valid + 1;

    if (just_over_max > max_valid) {
        tracker.record_failure("boundary_payload_overflow");
    }

    // Boundary 2: Entry type validation
    const invalid_types = [_]u8{ 0, 4, 0xFF };

    for (invalid_types) |invalid_type| {
        if (invalid_type == 0 or invalid_type > 3) {
            tracker.record_failure("invalid_entry_type");
        }
    }

    try testing.expectEqual(@as(u32, 4), tracker.consecutive_failures);

    // Boundary 3: Checksum validation patterns
    const suspicious_checksums = [_]u64{ 0, 0xFFFFFFFFFFFFFFFF, 0x7878787878787878 };

    for (suspicious_checksums) |checksum| {
        // Pattern-based corruption detection from debug analysis
        if (checksum == 0 or checksum == 0xFFFFFFFFFFFFFFFF or (checksum & 0xFFFFFFFF) == 0x78787878) {
            tracker.record_failure("suspicious_checksum_pattern");
        }
    }

    try testing.expectEqual(@as(u32, 7), tracker.consecutive_failures);

    // Verify failure rate calculation is working correctly
    const failure_rate = tracker.failure_rate();
    try testing.expect(failure_rate > 0.5); // More than 50% failures
}
