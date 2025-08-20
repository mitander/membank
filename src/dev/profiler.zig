//! Memory profiler for RSS measurement and tracking.
//!
//! Provides platform-specific RSS (Resident Set Size) measurement
//! capabilities for performance monitoring and memory usage analysis.
//! Used by benchmarks and profiling tests to track memory efficiency.

const builtin = @import("builtin");
const std = @import("std");
const build_options = @import("build_options");

/// Memory profiler for tracking RSS usage patterns
pub const MemoryProfiler = struct {
    initial_rss_bytes: u64,
    peak_rss_bytes: u64,

    const Self = @This();

    /// Initialize a new memory profiler instance
    pub fn init() Self {
        return Self{
            .initial_rss_bytes = 0,
            .peak_rss_bytes = 0,
        };
    }

    /// Start profiling by recording initial RSS baseline
    pub fn start_profiling(self: *Self) void {
        self.initial_rss_bytes = query_current_rss_memory();
        self.peak_rss_bytes = self.initial_rss_bytes;
    }

    /// Sample current memory usage and update peak if necessary
    pub fn sample_memory(self: *Self) void {
        const current_rss = query_current_rss_memory();
        if (current_rss > self.peak_rss_bytes) {
            self.peak_rss_bytes = current_rss;
        }
    }

    /// Calculate total memory growth since profiling started
    pub fn calculate_memory_growth(self: *const Self) u64 {
        if (self.peak_rss_bytes >= self.initial_rss_bytes) {
            return self.peak_rss_bytes - self.initial_rss_bytes;
        }
        return 0;
    }

    /// Check if memory usage is within efficiency thresholds
    /// Automatically adjusts limits based on build configuration (sanitizers, etc.)
    pub fn is_memory_efficient(self: *const Self, operations: u64) bool {
        // Base thresholds for production builds
        const BASE_PEAK_MEMORY_BYTES = 100 * 1024 * 1024; // 100MB for 10K operations
        const BASE_MEMORY_GROWTH_PER_OP = 1024; // 1KB average growth per operation

        // Apply tier-based multipliers for different build configurations
        const memory_multiplier: f64 = if (build_options.sanitizers_active) 100.0 else 1.0;

        const max_peak_memory = @as(u64, @intFromFloat(@as(f64, @floatFromInt(BASE_PEAK_MEMORY_BYTES)) * memory_multiplier));
        const max_growth_per_op = @as(u64, @intFromFloat(@as(f64, @floatFromInt(BASE_MEMORY_GROWTH_PER_OP)) * memory_multiplier));

        const growth_bytes = self.calculate_memory_growth();
        const peak_efficient = self.peak_rss_bytes <= max_peak_memory;
        const growth_per_op = if (operations > 0) growth_bytes / operations else 0;
        const growth_efficient = growth_per_op <= max_growth_per_op;

        // Debug output for sanitizer threshold debugging - only in debug mode or when explicitly requested
        if ((builtin.mode == .Debug and build_options.debug_tests) or
            (build_options.debug_tests and (!peak_efficient or !growth_efficient)))
        {
            std.debug.print("\n[MEMORY_EFFICIENCY] sanitizers_active={any}, multiplier={d:.1}\n", .{ build_options.sanitizers_active, memory_multiplier });
            std.debug.print("[MEMORY_EFFICIENCY] peak_rss={d}MB, max_allowed={d}MB, peak_ok={any}\n", .{ self.peak_rss_bytes / (1024 * 1024), max_peak_memory / (1024 * 1024), peak_efficient });
            std.debug.print("[MEMORY_EFFICIENCY] growth_per_op={d}bytes, max_allowed={d}bytes, growth_ok={any}\n", .{ growth_per_op, max_growth_per_op, growth_efficient });
        }

        return peak_efficient and growth_efficient;
    }
};

/// Query current RSS memory usage across platforms
pub fn query_current_rss_memory() u64 {
    switch (builtin.os.tag) {
        .linux => return query_rss_linux() catch 0,
        .macos => return query_rss_macos() catch 0,
        .windows => return query_rss_windows() catch 0,
        else => return 0,
    }
}

// Linux RSS memory via multiple methods (robust for containers)
fn query_rss_linux() !u64 {
    // Method 1: Try /proc/self/status (most common)
    if (query_rss_proc_status()) |rss| {
        if (rss > 0) return rss;
    }

    // Method 2: Try /proc/self/statm (alternative in containers)
    if (query_rss_proc_statm()) |rss| {
        if (rss > 0) return rss;
    }

    return 0;
}

fn query_rss_proc_status() ?u64 {
    const file = std.fs.openFileAbsolute("/proc/self/status", .{}) catch return null;
    defer file.close();

    var buf: [4096]u8 = undefined;
    const bytes_read = file.readAll(&buf) catch return null;
    const content = buf[0..bytes_read];

    // Search for "VmRSS:" line in /proc/self/status
    var lines = std.mem.splitSequence(u8, content, "\n");
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "VmRSS:")) {
            // Parse: "VmRSS:    1234 kB"
            var parts = std.mem.splitSequence(u8, line, " ");
            _ = parts.next(); // Skip "VmRSS:"
            while (parts.next()) |part| {
                if (part.len > 0 and std.ascii.isDigit(part[0])) {
                    const kb = std.fmt.parseInt(u64, part, 10) catch return null;
                    return kb * 1024; // Convert KB to bytes
                }
            }
        }
    }
    return null;
}

fn query_rss_proc_statm() ?u64 {
    const file = std.fs.openFileAbsolute("/proc/self/statm", .{}) catch return null;
    defer file.close();

    var buf: [256]u8 = undefined;
    const bytes_read = file.readAll(&buf) catch return null;
    const content = buf[0..bytes_read];

    // /proc/self/statm format: size resident shared text lib data dt
    var parts = std.mem.splitSequence(u8, std.mem.trim(u8, content, " \n\t"), " ");
    _ = parts.next(); // Skip size (first field)

    if (parts.next()) |resident_pages_str| {
        const resident_pages = std.fmt.parseInt(u64, resident_pages_str, 10) catch return null;
        // Convert pages to bytes (typically 4KB pages on most systems)
        const page_size = 4096; // Standard page size on Linux
        return resident_pages * page_size;
    }
    return null;
}

// macOS RSS memory via mach task_info
fn query_rss_macos() !u64 {
    const c = @cImport({
        @cInclude("mach/mach.h");
        @cInclude("mach/task.h");
        @cInclude("mach/mach_init.h");
    });

    var info: c.mach_task_basic_info_data_t = undefined;
    var count: c.mach_msg_type_number_t = c.MACH_TASK_BASIC_INFO_COUNT;

    const result = c.task_info(c.mach_task_self(), c.MACH_TASK_BASIC_INFO, @ptrCast(&info), &count);

    if (result != c.KERN_SUCCESS) return 0;

    return info.resident_size;
}

// Windows RSS memory via GetProcessMemoryInfo
fn query_rss_windows() !u64 {
    // For Zig 0.13+, this would use Windows API calls
    // For now, return 0 as fallback (Windows support can be added later)
    return 0;
}
