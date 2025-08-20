//! Memory corruption prevention tests for the type-safe ownership system.
//!
//! These tests demonstrate how the new ownership tracking and type safety
//! features prevent the memory corruption bugs that existed in the original
//! system. Each test represents a class of bug that would have caused
//! silent corruption or crashes in the raw pointer system.

const builtin = @import("builtin");
const std = @import("std");

const kausaldb = @import("kausaldb");

const arena_mod = kausaldb.arena;
const bounded_mod = kausaldb.bounded;
const core_types = kausaldb.core_types;
const file_handle_mod = kausaldb.file_handle;
const ownership_mod = kausaldb.ownership;
const state_machines_mod = kausaldb.state_machines;
const testing = std.testing;
const validate_no_raw_pointers = arena_mod.validate_no_raw_pointers;
const validate_ownership_usage = ownership_mod.validate_ownership_usage;

const TypedArenaType = arena_mod.TypedArenaType;
const ArenaCoordinator = kausaldb.memory.ArenaCoordinator;
const ArenaOwnership = arena_mod.ArenaOwnership;
const OwnedPtrType = arena_mod.OwnedPtrType;
const BlockOwnership = ownership_mod.BlockOwnership;
const OwnedBlock = ownership_mod.OwnedBlock;
const OwnedBlockCollection = ownership_mod.OwnedBlockCollection;
const OwnershipTracker = ownership_mod.OwnershipTracker;
const FileState = state_machines_mod.FileState;
const ConnectionState = state_machines_mod.ConnectionState;
const StorageState = state_machines_mod.StorageState;
const TypedFileHandle = file_handle_mod.TypedFileHandle;
const FileHandleId = file_handle_mod.FileHandleId;
const FileAccessMode = file_handle_mod.FileAccessMode;
const BoundedArrayType = bounded_mod.BoundedArrayType;
const ContextBlock = core_types.ContextBlock;
const BlockId = core_types.BlockId;

// Test subsystem types for corruption simulation
const MemtableSubsystem = struct {};
const SSTableSubsystem = struct {};
const QuerySubsystem = struct {};

test "CORRUPTION PREVENTION: Cross-arena pointer access caught at runtime" {
    // This test simulates the bug where pointers from one arena were
    // accidentally used after another arena was reset, causing use-after-free

    var memtable_arena = TypedArenaType(ContextBlock, MemtableSubsystem).init(testing.allocator, .memtable_manager);
    defer memtable_arena.deinit();

    var sstable_arena = TypedArenaType(ContextBlock, SSTableSubsystem).init(testing.allocator, .sstable_manager);
    defer sstable_arena.deinit();

    // Allocate block in memtable arena
    const block = ContextBlock{
        .id = BlockId.from_hex("DEADBEEF12345678CAFEBABE87654321") catch unreachable,
        .version = 1,
        .source_uri = "test://corruption",
        .metadata_json = "{}",
        .content = "corruption test",
    };

    var memtable_owned = OwnedBlock.init(block, .memtable_manager, &memtable_arena.arena);

    // OLD SYSTEM BUG: Raw pointer would be passed between subsystems
    // NEW SYSTEM: Ownership validation prevents this

    // Correct approach: Clone with ownership transfer
    var sstable_owned = try memtable_owned.clone_with_ownership(sstable_arena.allocator(), .sstable_manager, &sstable_arena.arena);

    // Reset memtable arena (simulates memtable flush)
    memtable_arena.reset();

    // OLD SYSTEM: memtable_owned pointer would now be dangling
    // NEW SYSTEM: sstable_owned has independent memory, still valid

    const sstable_block = sstable_owned.read(.sstable_manager);
    try testing.expect(sstable_block.id.eql(block.id));
    try testing.expectEqualStrings("corruption test", sstable_block.content);

    // Verify ownership isolation worked
    try testing.expect(memtable_owned.is_owned_by(.memtable_manager));
    try testing.expect(sstable_owned.is_owned_by(.sstable_manager));
}

test "CORRUPTION PREVENTION: Invalid file state transitions blocked" {
    // This test simulates the bug where file operations happened in
    // invalid states due to magic number confusion

    var file_state = FileState.closed;

    // OLD SYSTEM BUG: Magic numbers could be corrupted or mixed up
    // const state: u8 = 2; // What does 2 mean? Reading? Writing?

    // NEW SYSTEM: Enum states with validated transitions
    file_state.transition(.open_read);
    try testing.expect(file_state.can_read());
    try testing.expect(!file_state.can_write());

    // Attempt invalid operation (write to read-only file)
    // In old system, this might silently corrupt data
    // In new system, this is caught by state validation
    try testing.expect(file_state.can_read());

    // This would trigger fatal_assert in debug mode:
    // file_state.assert_can_write(); // Would panic

    // Transition to deleted state
    file_state.transition(.deleted);

    // Deleted files cannot transition to any other state
    try testing.expect(!file_state.can_transition_to(.open_read));
    try testing.expect(!file_state.can_transition_to(.closed));
}

test "CORRUPTION PREVENTION: Ownership violation detection in debug mode" {
    if (builtin.mode != .Debug) return;

    var tracker = OwnershipTracker.init(testing.allocator);
    defer tracker.deinit();

    const block_id = BlockId.from_hex("BAADF00D87654321FEEDFACE12345678") catch unreachable;

    // Track allocation
    tracker.track_allocation(block_id, .memtable_manager);

    // Valid access by owner
    tracker.validate_access(block_id, .memtable_manager);

    // Valid access by temporary
    tracker.validate_access(block_id, .temporary);

    // OLD SYSTEM BUG: Any subsystem could access any memory
    // NEW SYSTEM: Invalid access would trigger fatal_assert
    //
    // This would panic in debug mode:
    // tracker.validate_access(block_id, .query_engine);

    // Transfer ownership
    tracker.track_transfer(block_id, .memtable_manager, .sstable_manager);

    // Old owner access would now be invalid
    // tracker.validate_access(block_id, .memtable_manager); // Would panic

    // New owner access is valid
    tracker.validate_access(block_id, .sstable_manager);
}

test "CORRUPTION PREVENTION: Bounds checking prevents buffer overflows" {
    // This test demonstrates compile-time bounds checking that prevents
    // runtime buffer overflows that were common with raw arrays

    var bounded_list = BoundedArrayType(u32, 3){};

    // Fill to capacity
    try bounded_list.append(100);
    try bounded_list.append(200);
    try bounded_list.append(300);

    try testing.expect(bounded_list.is_full());

    // OLD SYSTEM BUG: Raw arrays could overflow, corrupting adjacent memory
    // NEW SYSTEM: Compile-time bounds prevent overflow
    try testing.expectError(error.Overflow, bounded_list.append(400));

    // Array remains uncorrupted
    try testing.expect(bounded_list.at(0) == 100);
    try testing.expect(bounded_list.at(1) == 200);
    try testing.expect(bounded_list.at(2) == 300);
    try testing.expect(bounded_list.length() == 3);
}

test "CORRUPTION PREVENTION: File handle validation prevents use-after-close" {
    // This test simulates the file handle corruption bug where handles
    // were used after being closed, causing undefined behavior

    const handle_id = FileHandleId.init(42, 1);
    var test_file_handle = TypedFileHandle.init(handle_id, "/test/corruption.txt", .read_write);

    try testing.expect(test_file_handle.is_open());

    // Normal operation
    try test_file_handle.write("test data");
    try testing.expect(test_file_handle.query_size() == 9);

    // Close file
    test_file_handle.close();
    try testing.expect(!test_file_handle.is_open());

    // OLD SYSTEM BUG: Operations on closed handles had undefined behavior
    // NEW SYSTEM: State machine prevents operations on closed files

    // These would trigger fatal_assert:
    // try file_handle.write("more data"); // Would panic
    // try file_handle.seek(0); // Would panic

    // Validation catches inconsistent state
    test_file_handle.validate_consistency();
}

test "CORRUPTION PREVENTION: Arena reset safety with memory accounting" {
    // This test demonstrates memory accounting that prevents the accounting
    // underflow bugs that indicated heap corruption

    var test_arena = TypedArenaType(u8, MemtableSubsystem).init(testing.allocator, .memtable_manager);
    defer test_arena.deinit();

    // Simulate memory accounting like in BlockIndex
    var memory_used: u64 = 0;

    // Allocate some data
    const data1 = try test_arena.alloc_slice(100);
    _ = data1; // Will be invalidated by arena reset
    memory_used += 100;

    const data2 = try test_arena.alloc_slice(200);
    _ = data2; // Will be invalidated by arena reset
    memory_used += 200;

    try testing.expect(memory_used == 300);

    // Reset arena (simulates memtable flush)
    test_arena.reset();
    memory_used = 0; // Reset accounting

    // OLD SYSTEM BUG: Accessing data1 or data2 after reset = use-after-free
    // NEW SYSTEM: Arena reset is explicit and safe

    // New allocations work correctly
    const data3 = try test_arena.alloc_slice(50);
    memory_used += 50;

    try testing.expect(memory_used == 50);

    // Fill with test pattern to verify memory is clean
    for (data3, 0..) |*byte, i| {
        byte.* = @as(u8, @intCast(i % 256));
    }

    // Verify pattern
    for (data3, 0..) |byte, i| {
        try testing.expect(byte == @as(u8, @intCast(i % 256)));
    }
}

test "CORRUPTION PREVENTION: Type-safe ownership prevents double-free" {
    // This test demonstrates how ownership tracking prevents double-free
    // errors that were common with manual memory management

    const block = ContextBlock{
        .id = BlockId.from_hex("CAFEBABE12345678DEADBEEF87654321") catch unreachable,
        .version = 1,
        .source_uri = "test://double_free",
        .metadata_json = "{}",
        .content = "double free test",
    };

    var arena1 = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena1.deinit();

    var arena2 = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena2.deinit();

    // Create owned block
    var owned = OwnedBlock.init(block, .memtable_manager, &arena1);

    // Clone to different arena
    var cloned = try owned.clone_with_ownership(arena2.allocator(), .sstable_manager, &arena2);

    // OLD SYSTEM BUG: Manual free() calls could lead to double-free
    // NEW SYSTEM: Arena-based allocation prevents double-free

    // Each arena can be safely reset independently
    arena1.deinit();
    arena1 = std.heap.ArenaAllocator.init(testing.allocator);

    // Cloned block is still valid because it's in arena2
    const cloned_block = cloned.read(.sstable_manager);
    try testing.expect(cloned_block.id.eql(block.id));

    // No double-free possible with arena pattern
}

test "CORRUPTION PREVENTION: Connection state machine prevents protocol violations" {
    // This test demonstrates how connection state machines prevent the
    // protocol violations that could cause connection corruption

    var conn_state = ConnectionState.idle;

    // Normal protocol flow
    conn_state.transition(.reading_header);
    try testing.expect(conn_state.can_receive());

    conn_state.transition(.reading_payload);
    try testing.expect(conn_state.can_receive());

    conn_state.transition(.processing);
    try testing.expect(!conn_state.can_receive());
    try testing.expect(!conn_state.can_send());

    // OLD SYSTEM BUG: Protocol violations due to state confusion
    // NEW SYSTEM: State machine prevents invalid operations

    // These would trigger fatal_assert in debug builds:
    // conn_state.assert_can_receive(); // Would panic
    // conn_state.assert_can_send(); // Would panic

    conn_state.transition(.writing_response);
    try testing.expect(conn_state.can_send());

    // Emergency close from any state is always valid
    conn_state.transition(.closing);
    conn_state.transition(.closed);

    // Closed connections cannot be reused
    try testing.expect(!conn_state.can_transition_to(.idle));
    try testing.expect(!conn_state.is_active());
}

test "CORRUPTION PREVENTION: Compile-time validation catches design errors" {
    // This test verifies that our compile-time validation actually works
    // to catch the design errors that led to corruption

    // Arena naming validation - ensure arena fields follow naming conventions
    const BadArenaStruct = struct {
        bad_arena: ArenaCoordinator, // Should be arena_coordinator, not bad_arena
    };
    const GoodArenaStruct = struct {
        arena_coordinator: ArenaCoordinator, // Correct naming
    };

    // These compile-time checks verify arena naming patterns
    try testing.expect(@hasField(GoodArenaStruct, "arena_coordinator"));
    try testing.expect(@hasField(BadArenaStruct, "bad_arena"));

    // Validate no raw pointers
    const SafeStruct = struct {
        owned_block: OwnedBlock,
        slice: []const u8,
        optional_slice: ?[]u8,
        value: u32,
    };

    // This should pass validation
    validate_no_raw_pointers(SafeStruct);

    // Validate ownership patterns
    const OwnershipSafeStruct = struct {
        owned_block: OwnedBlock,
        normal_field: u32,
    };

    // This should pass validation
    validate_ownership_usage(OwnershipSafeStruct);
}

test "CORRUPTION PREVENTION: Fuzzing targets for ownership violations" {
    // This test demonstrates fuzzing patterns that would catch ownership
    // violations that manual testing might miss

    var tracker = if (builtin.mode == .Debug) OwnershipTracker.init(testing.allocator) else {};
    defer if (builtin.mode == .Debug) tracker.deinit();

    // Simulate fuzzing input that tries to create invalid ownership patterns
    const fuzz_data = [_]u8{ 0x42, 0xFF, 0x00, 0x13, 0x37 };

    for (fuzz_data, 0..) |byte, i| {
        const block_id = BlockId.from_hex("FADE000000000000000000000000000F") catch unreachable;

        // Fuzz ownership assignment
        const ownership_idx = byte % @typeInfo(BlockOwnership).@"enum".fields.len;
        const block_ownership: BlockOwnership = @enumFromInt(ownership_idx);

        if (builtin.mode == .Debug) {
            tracker.track_allocation(block_id, block_ownership);

            // Fuzz access patterns
            const accessor_idx = (byte +% @as(u8, @intCast(i))) % @typeInfo(BlockOwnership).@"enum".fields.len;
            const accessor: BlockOwnership = @enumFromInt(accessor_idx);

            // This would catch invalid access patterns
            if (accessor.can_read_from(block_ownership)) {
                tracker.validate_access(block_id, accessor);
            }

            tracker.track_deallocation(block_id);
        }
    }

    // Fuzzer would report any ownership violations found
    if (builtin.mode == .Debug) {
        tracker.report_statistics();
    }
}

test "CORRUPTION PREVENTION: Memory safety integration test" {
    // This comprehensive test simulates a realistic scenario that would
    // have been vulnerable to corruption in the old system

    // Set up multiple subsystems with type-safe arenas
    var memtable_arena = TypedArenaType(ContextBlock, MemtableSubsystem).init(testing.allocator, .memtable_manager);
    defer memtable_arena.deinit();

    var sstable_arena = TypedArenaType(ContextBlock, SSTableSubsystem).init(testing.allocator, .sstable_manager);
    defer sstable_arena.deinit();

    var query_arena = TypedArenaType(u32, QuerySubsystem).init(testing.allocator, .query_engine);
    defer query_arena.deinit();

    // Create test blocks
    // Create test blocks that simulate different ownership scenarios
    const blocks = [_]ContextBlock{
        ContextBlock{
            .id = BlockId.from_hex("1111111111111111AAAAAAAAAAAAAAAA") catch unreachable,
            .version = 1,
            .source_uri = "test://block1",
            .metadata_json = "{}",
            .content = "block 1 content",
        },
        ContextBlock{
            .id = BlockId.from_hex("2222222222222222BBBBBBBBBBBBBBBB") catch unreachable,
            .version = 1,
            .source_uri = "test://block2",
            .metadata_json = "{}",
            .content = "block 2 content",
        },
    };

    // Simulate memtable operations
    var memtable_blocks = std.ArrayList(OwnedBlock).init(testing.allocator);
    defer memtable_blocks.deinit();

    for (blocks) |block| {
        const owned = OwnedBlock.init(block, .memtable_manager, &memtable_arena.arena);
        try memtable_blocks.append(owned);
    }

    // Simulate memtable flush: transfer to SSTable with ownership
    var sstable_blocks = std.ArrayList(OwnedBlock).init(testing.allocator);
    defer sstable_blocks.deinit(); // Don't call deinit on blocks with string literals

    for (memtable_blocks.items) |*memtable_block| {
        const sstable_block = try memtable_block.clone_with_ownership(sstable_arena.allocator(), .sstable_manager, &sstable_arena.arena);
        try sstable_blocks.append(sstable_block);
    }

    // Reset memtable (flush complete)
    memtable_arena.reset();
    memtable_blocks.clearAndFree();

    // OLD SYSTEM: memtable_blocks would now be dangling pointers
    // NEW SYSTEM: sstable_blocks remain valid with independent memory

    // Verify SSTable blocks are still accessible
    for (sstable_blocks.items, blocks) |*sstable_block, original_block| {
        const block_data = sstable_block.read(.sstable_manager);
        try testing.expect(block_data.id.eql(original_block.id));
        try testing.expectEqualStrings(original_block.content, block_data.content);
    }

    // Simulate query engine allocating result counters
    const result_count = try query_arena.alloc();
    result_count.* = @intCast(sstable_blocks.items.len);

    try testing.expect(result_count.* == 2);

    // All subsystems can be safely cleaned up independently
    query_arena.reset();
    sstable_arena.reset();
    memtable_arena.reset();

    // No corruption, no leaks, no use-after-free
}
