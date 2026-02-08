const std = @import("std");
const value_mod = @import("value.zig");
const row_storage = @import("row_storage.zig");

const Row = value_mod.Row;
const RowStorage = row_storage.RowStorage;

/// ArrayList-backed row storage (in-memory implementation)
pub const ArrayListStorage = struct {
    rows: std.ArrayList(Row),

    const vtable = RowStorage.VTable{
        .scan = scan,
        .scanMut = scanMut,
        .len = getLen,
        .append = appendRow,
        .orderedRemove = remove,
        .clearRetainingCapacity = clear,
        .deinit = deinitStorage,
    };

    /// Initialize empty ArrayList storage
    pub fn init() ArrayListStorage {
        return .{ .rows = .{} };
    }

    /// Get the abstract RowStorage interface for this storage
    pub fn storage(self: *const ArrayListStorage) RowStorage {
        return .{
            .ptr = @ptrCast(@constCast(self)),
            .vtable = &vtable,
        };
    }

    fn scan(ptr: *anyopaque) []const Row {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        return self.rows.items;
    }

    fn scanMut(ptr: *anyopaque) []Row {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        return self.rows.items;
    }

    fn getLen(ptr: *anyopaque) usize {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        return self.rows.items.len;
    }

    fn appendRow(ptr: *anyopaque, allocator: std.mem.Allocator, row: Row) error{OutOfMemory}!void {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        try self.rows.append(allocator, row);
    }

    fn remove(ptr: *anyopaque, index: usize) Row {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        return self.rows.orderedRemove(index);
    }

    fn clear(ptr: *anyopaque) void {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        self.rows.clearRetainingCapacity();
    }

    fn deinitStorage(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ArrayListStorage = @ptrCast(@alignCast(ptr));
        self.rows.deinit(allocator);
    }
};

// Unit tests
test "ArrayListStorage: init and len" {
    const allocator = std.testing.allocator;
    var storage = ArrayListStorage.init();
    defer storage.rows.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), storage.storage().len());
}

test "ArrayListStorage: append and scan" {
    const allocator = std.testing.allocator;
    var storage = ArrayListStorage.init();
    defer storage.rows.deinit(allocator);

    var row1: Row = undefined;
    row1.values = try allocator.alloc(value_mod.Value, 1);
    defer allocator.free(row1.values);
    row1.values[0] = .{ .integer = 42 };

    try storage.storage().append(allocator, row1);

    const rows = storage.storage().scan();
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 42), rows[0].values[0].integer);
}

test "ArrayListStorage: orderedRemove" {
    const allocator = std.testing.allocator;
    var storage = ArrayListStorage.init();
    defer storage.rows.deinit(allocator);

    // Append two rows
    var row1: Row = undefined;
    row1.values = try allocator.alloc(value_mod.Value, 1);
    defer allocator.free(row1.values);
    row1.values[0] = .{ .integer = 1 };

    var row2: Row = undefined;
    row2.values = try allocator.alloc(value_mod.Value, 1);
    defer allocator.free(row2.values);
    row2.values[0] = .{ .integer = 2 };

    try storage.storage().append(allocator, row1);
    try storage.storage().append(allocator, row2);

    // Remove first row
    const removed = storage.storage().orderedRemove(0);
    try std.testing.expectEqual(@as(i64, 1), removed.values[0].integer);

    // Verify remaining row
    const rows = storage.storage().scan();
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 2), rows[0].values[0].integer);
}

test "ArrayListStorage: clearRetainingCapacity" {
    const allocator = std.testing.allocator;
    var storage = ArrayListStorage.init();
    defer storage.rows.deinit(allocator);

    var row: Row = undefined;
    row.values = try allocator.alloc(value_mod.Value, 1);
    defer allocator.free(row.values);
    row.values[0] = .{ .integer = 123 };

    try storage.storage().append(allocator, row);
    try std.testing.expectEqual(@as(usize, 1), storage.storage().len());

    storage.storage().clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), storage.storage().len());
}
