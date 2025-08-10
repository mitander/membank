//! Enum-based state machines for type-safe state management.
//!
//! Design rationale: Provides compile-time and runtime validation of state
//! transitions to prevent invalid operations. State machines use enums to make
//! invalid states unrepresentable and validate all transitions explicitly.
//!
//! All state machines follow the pattern of can_transition_to() for validation
//! and transition() for safe state changes with debug logging and fatal assertions
//! for invalid transitions in debug builds.

const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx.zig");
const custom_assert = @import("assert.zig");
const fatal_assert = custom_assert.fatal_assert;
const assert_fmt = custom_assert.assert_fmt;

/// File state for VFS operations with transition validation.
/// Prevents invalid file operations by encoding valid states in the type system.
pub const FileState = enum {
    closed,
    open_read,
    open_write,
    open_read_write,
    deleted,

    /// Validate if transition to next state is allowed.
    /// Used for compile-time verification of transition logic.
    pub fn can_transition_to(self: FileState, next: FileState) bool {
        return switch (self) {
            .closed => switch (next) {
                .open_read, .open_write, .open_read_write, .deleted => true,
                else => false,
            },
            .open_read => switch (next) {
                .closed, .deleted => true,
                else => false,
            },
            .open_write => switch (next) {
                .closed, .deleted => true,
                else => false,
            },
            .open_read_write => switch (next) {
                .closed, .deleted => true,
                else => false,
            },
            .deleted => false, // Terminal state
        };
    }

    /// Perform validated state transition with debug logging.
    /// Triggers fatal assertion in debug builds for invalid transitions.
    pub fn transition(self: *FileState, next: FileState) void {
        if (builtin.mode == .Debug) {
            if (!self.can_transition_to(next)) {
                fatal_assert(false, "Invalid file state transition: {} -> {}", .{ self.*, next });
            }
            std.log.debug("File state transition: {} -> {}", .{ self.*, next });
        } else {
            // Release builds still validate critical transitions
            fatal_assert(self.* != .deleted, "File operation on deleted file", .{});
        }
        self.* = next;
    }

    /// Check if file can be read in current state.
    pub fn can_read(self: FileState) bool {
        return switch (self) {
            .open_read, .open_read_write => true,
            else => false,
        };
    }

    /// Check if file can be written in current state.
    pub fn can_write(self: FileState) bool {
        return switch (self) {
            .open_write, .open_read_write => true,
            else => false,
        };
    }

    /// Assert that file can be read, panic if not.
    pub fn assert_can_read(self: FileState) void {
        fatal_assert(self.can_read(), "Cannot read file in state: {}", .{self});
    }

    /// Assert that file can be written, panic if not.
    pub fn assert_can_write(self: FileState) void {
        fatal_assert(self.can_write(), "Cannot write file in state: {}", .{self});
    }

    /// Check if file is open in any mode.
    pub fn is_open(self: FileState) bool {
        return switch (self) {
            .open_read, .open_write, .open_read_write => true,
            else => false,
        };
    }
};

/// Connection state for server connection lifecycle management.
/// Enforces valid connection protocol state transitions.
pub const ConnectionState = enum {
    idle,
    reading_header,
    reading_payload,
    processing,
    writing_response,
    closing,
    closed,

    /// Validate if transition to next state is allowed.
    pub fn can_transition_to(self: ConnectionState, next: ConnectionState) bool {
        return switch (self) {
            .idle => next == .reading_header or next == .closing,
            .reading_header => next == .reading_payload or next == .closing,
            .reading_payload => next == .processing or next == .closing,
            .processing => next == .writing_response or next == .closing,
            .writing_response => next == .idle or next == .closing,
            .closing => next == .closed,
            .closed => false, // Terminal state
        };
    }

    /// Perform validated state transition with debug logging.
    pub fn transition(self: *ConnectionState, next: ConnectionState) void {
        if (builtin.mode == .Debug) {
            if (!self.can_transition_to(next)) {
                fatal_assert(false, "Invalid connection state transition: {} -> {}", .{ self.*, next });
            }
            std.log.debug("Connection state transition: {} -> {}", .{ self.*, next });
        }
        self.* = next;
    }

    /// Check if connection can receive data.
    pub fn can_receive(self: ConnectionState) bool {
        return switch (self) {
            .reading_header, .reading_payload => true,
            else => false,
        };
    }

    /// Check if connection can send data.
    pub fn can_send(self: ConnectionState) bool {
        return switch (self) {
            .writing_response => true,
            else => false,
        };
    }

    /// Assert that connection can receive data.
    pub fn assert_can_receive(self: ConnectionState) void {
        fatal_assert(self.can_receive(), "Cannot receive in state: {}", .{self});
    }

    /// Assert that connection can send data.
    pub fn assert_can_send(self: ConnectionState) void {
        fatal_assert(self.can_send(), "Cannot send in state: {}", .{self});
    }

    /// Check if connection is active (not closed/closing).
    pub fn is_active(self: ConnectionState) bool {
        return switch (self) {
            .closed, .closing => false,
            else => true,
        };
    }
};

/// Storage engine state for LSM-Tree lifecycle management.
/// Coordinates memtable and SSTable subsystem states.
pub const StorageState = enum {
    uninitialized,
    initialized,
    running,
    compacting,
    flushing,
    stopping,
    stopped,

    /// Validate if transition to next state is allowed.
    pub fn can_transition_to(self: StorageState, next: StorageState) bool {
        return switch (self) {
            .uninitialized => next == .initialized,
            .initialized => next == .running or next == .stopped,
            .running => next == .compacting or next == .flushing or next == .stopping,
            .compacting => next == .running or next == .stopping,
            .flushing => next == .running or next == .stopping,
            .stopping => next == .stopped,
            .stopped => false, // Terminal state
        };
    }

    /// Perform validated state transition.
    pub fn transition(self: *StorageState, next: StorageState) void {
        if (builtin.mode == .Debug) {
            if (!self.can_transition_to(next)) {
                fatal_assert(false, "Invalid storage state transition: {} -> {}", .{ self.*, next });
            }
            std.log.debug("Storage state transition: {} -> {}", .{ self.*, next });
        }
        self.* = next;
    }

    /// Check if storage engine can accept writes.
    pub fn can_write(self: StorageState) bool {
        return switch (self) {
            .running, .compacting => true,
            else => false,
        };
    }

    /// Check if storage engine can perform reads.
    pub fn can_read(self: StorageState) bool {
        return switch (self) {
            .running, .compacting, .flushing => true,
            else => false,
        };
    }

    /// Assert that storage can accept writes.
    pub fn assert_can_write(self: StorageState) void {
        fatal_assert(self.can_write(), "Cannot write in storage state: {}", .{self});
    }

    /// Assert that storage can perform reads.
    pub fn assert_can_read(self: StorageState) void {
        fatal_assert(self.can_read(), "Cannot read in storage state: {}", .{self});
    }
};

/// Generic state machine validation for custom enum states.
/// Provides compile-time validation that state machines have proper transition methods.
pub fn validate_state_machine(comptime StateEnum: type) void {
    const info = @typeInfo(StateEnum);
    switch (info) {
        .@"enum" => {
            // Verify required methods exist
            if (!@hasDecl(StateEnum, "can_transition_to")) {
                @compileError(@typeName(StateEnum) ++ " state machine must implement can_transition_to() method");
            }
            if (!@hasDecl(StateEnum, "transition")) {
                @compileError(@typeName(StateEnum) ++ " state machine must implement transition() method");
            }

            // Validate that no state can transition to itself unless explicitly allowed
            inline for (info.@"enum".fields) |field| {
                const state_value: StateEnum = @enumFromInt(field.value);
                comptime {
                    // This helps catch infinite loops in state machines
                    if (@hasDecl(StateEnum, "ALLOW_SELF_TRANSITIONS")) {
                        // Some state machines may explicitly allow self-transitions
                    } else {
                        // Most state machines should not allow self-transitions
                        if (state_value.can_transition_to(state_value)) {
                            @compileError(@typeName(StateEnum) ++ " state " ++ field.name ++ " allows self-transition (add ALLOW_SELF_TRANSITIONS if intended)");
                        }
                    }
                }
            }
        },
        else => @compileError("validate_state_machine only works with enum types"),
    }
}

/// State machine debug tracer for development builds.
/// Tracks state transition history for debugging invalid sequences.
pub fn StateMachineTracerType(comptime StateEnum: type) type {
    return struct {
        const Tracer = StateMachineTracerType(StateEnum);
        history: if (builtin.mode == .Debug) [32]StateEnum else void,
        history_len: if (builtin.mode == .Debug) usize else void,
        creation_time: if (builtin.mode == .Debug) i64 else void,

        pub fn init() Tracer {
            return Tracer{
                .history = if (builtin.mode == .Debug) [_]StateEnum{undefined} ** 32 else {},
                .history_len = if (builtin.mode == .Debug) 0 else {},
                .creation_time = if (builtin.mode == .Debug) std.time.milliTimestamp() else {},
            };
        }

        /// Record state transition with timestamp and validation.
        /// Maintains circular buffer of recent transitions for debugging.
        pub fn record_transition(self: *Tracer, from: StateEnum, to: StateEnum) void {
            if (builtin.mode == .Debug) {
                if (self.history_len < self.history.len) {
                    self.history[self.history_len] = to;
                    self.history_len += 1;
                } else {
                    // Circular buffer - overwrite oldest entry
                    stdx.copy_left(StateEnum, self.history[0 .. self.history.len - 1], self.history[1..]);
                    self.history[self.history.len - 1] = to;
                }

                std.log.debug("State transition recorded: {} -> {} (history: {})", .{
                    from,
                    to,
                    self.history_len,
                });
            }
        }

        /// Dump complete state transition history for debugging.
        /// Outputs chronological list of state changes with indices.
        pub fn dump_history(self: *const Tracer) void {
            if (builtin.mode == .Debug) {
                std.log.debug("State machine history ({} transitions):", .{self.history_len});
                const count = @min(self.history_len, self.history.len);
                for (self.history[0..count], 0..) |state, i| {
                    std.log.debug("  {}: {}", .{ i, state });
                }
            }
        }
    };
}

// Convenience type alias
pub fn StateMachineTracer(comptime StateEnum: type) type {
    return StateMachineTracerType(StateEnum);
}

/// Compile-time validation for state machine exhaustiveness.
/// Ensures all enum variants are handled in transition logic.
pub fn validate_transition_exhaustiveness(comptime StateEnum: type) void {
    const info = @typeInfo(StateEnum);
    switch (info) {
        .@"enum" => |enum_info| {
            // Create test instance to verify can_transition_to handles all states
            inline for (enum_info.fields) |from_field| {
                const from_state: StateEnum = @enumFromInt(from_field.value);
                inline for (enum_info.fields) |to_field| {
                    const to_state: StateEnum = @enumFromInt(to_field.value);
                    // This forces compilation of can_transition_to for all combinations
                    _ = from_state.can_transition_to(to_state);
                }
            }
        },
        else => @compileError("validate_transition_exhaustiveness only works with enum types"),
    }
}

// Compile-time validation of our state machines
comptime {
    validate_state_machine(FileState);
    validate_state_machine(ConnectionState);
    validate_state_machine(StorageState);

    validate_transition_exhaustiveness(FileState);
    validate_transition_exhaustiveness(ConnectionState);
    validate_transition_exhaustiveness(StorageState);
}

// Tests

test "FileState transition validation" {
    var state = FileState.closed;

    // Valid transitions
    state.transition(.open_read);
    try std.testing.expect(state == .open_read);

    state.transition(.closed);
    try std.testing.expect(state == .closed);

    state.transition(.open_write);
    try std.testing.expect(state == .open_write);

    // Can always transition to deleted
    state.transition(.deleted);
    try std.testing.expect(state == .deleted);
}

test "FileState operation validation" {
    const read_state = FileState.open_read;
    const write_state = FileState.open_write;
    const closed_state = FileState.closed;

    // Read validation
    try std.testing.expect(read_state.can_read());
    try std.testing.expect(!write_state.can_read());
    try std.testing.expect(!closed_state.can_read());

    // Write validation
    try std.testing.expect(!read_state.can_write());
    try std.testing.expect(write_state.can_write());
    try std.testing.expect(!closed_state.can_write());

    // Open validation
    try std.testing.expect(read_state.is_open());
    try std.testing.expect(write_state.is_open());
    try std.testing.expect(!closed_state.is_open());
}

test "ConnectionState lifecycle" {
    var state = ConnectionState.idle;

    // Normal request flow
    state.transition(.reading_header);
    try std.testing.expect(state.can_receive());

    state.transition(.reading_payload);
    try std.testing.expect(state.can_receive());

    state.transition(.processing);
    try std.testing.expect(!state.can_receive());
    try std.testing.expect(!state.can_send());

    state.transition(.writing_response);
    try std.testing.expect(state.can_send());

    state.transition(.idle);
    try std.testing.expect(!state.can_send());

    // Cleanup flow
    state.transition(.closing);
    state.transition(.closed);
    try std.testing.expect(!state.is_active());
}

test "StorageState operation coordination" {
    var state = StorageState.uninitialized;

    // Startup sequence
    state.transition(.initialized);
    state.transition(.running);

    try std.testing.expect(state.can_read());
    try std.testing.expect(state.can_write());

    // Background operations
    state.transition(.compacting);
    try std.testing.expect(state.can_read());
    try std.testing.expect(state.can_write()); // Compaction doesn't block writes

    state.transition(.running);

    state.transition(.flushing);
    try std.testing.expect(state.can_read());
    try std.testing.expect(!state.can_write()); // Flushing blocks writes

    // Shutdown sequence
    state.transition(.stopping);
    try std.testing.expect(!state.can_write());

    state.transition(.stopped);
    try std.testing.expect(!state.can_read());
    try std.testing.expect(!state.can_write());
}

test "state machine tracer in debug mode" {
    if (builtin.mode != .Debug) return;

    var tracer = StateMachineTracer(FileState).init();
    var state = FileState.closed;

    // Record some transitions
    const old_state = state;
    state.transition(.open_read);
    tracer.record_transition(old_state, state);

    const old_state2 = state;
    state.transition(.closed);
    tracer.record_transition(old_state2, state);

    // Dump history for debugging
    tracer.dump_history();
}

test "state machine prevents invalid transitions" {
    var file_state = FileState.deleted;

    // Deleted files cannot transition to any other state
    try std.testing.expect(!file_state.can_transition_to(.open_read));
    try std.testing.expect(!file_state.can_transition_to(.closed));

    var conn_state = ConnectionState.closed;

    // Closed connections cannot transition
    try std.testing.expect(!conn_state.can_transition_to(.idle));
    try std.testing.expect(!conn_state.can_transition_to(.reading_header));
}

test "state machine compile-time validation" {
    // Verify our state machines pass validation
    comptime {
        validate_state_machine(FileState);
        validate_state_machine(ConnectionState);
        validate_state_machine(StorageState);
    }
}

test "exhaustive transition validation" {
    // This test ensures can_transition_to handles all enum combinations
    comptime {
        validate_transition_exhaustiveness(FileState);
        validate_transition_exhaustiveness(ConnectionState);
        validate_transition_exhaustiveness(StorageState);
    }
}

test "connection state operation validation" {
    const reading = ConnectionState.reading_header;
    const processing = ConnectionState.processing;
    const writing = ConnectionState.writing_response;
    const closed = ConnectionState.closed;

    // Receive validation
    try std.testing.expect(reading.can_receive());
    try std.testing.expect(!processing.can_receive());
    try std.testing.expect(!writing.can_receive());

    // Send validation
    try std.testing.expect(!reading.can_send());
    try std.testing.expect(!processing.can_send());
    try std.testing.expect(writing.can_send());

    // Active validation
    try std.testing.expect(reading.is_active());
    try std.testing.expect(processing.is_active());
    try std.testing.expect(writing.is_active());
    try std.testing.expect(!closed.is_active());
}

test "storage state coordination patterns" {
    var state = StorageState.running;

    // Background operations don't interfere with each other
    try std.testing.expect(state.can_transition_to(.compacting));
    try std.testing.expect(state.can_transition_to(.flushing));

    // But they can't run simultaneously
    state.transition(.compacting);
    try std.testing.expect(!state.can_transition_to(.flushing));

    // Emergency stop always works
    try std.testing.expect(state.can_transition_to(.stopping));
}
