const std = @import("std");
const value_mod = @import("value.zig");

const Row = value_mod.Row;

/// Abstract interface for row storage backends (ArrayList, B-Tree, etc.)
/// Follows the vtable pattern similar to std.mem.Allocator
pub const RowStorage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Iterate all rows (returns immutable slice; caller must NOT free)
        scan: *const fn (ptr: *anyopaque) []const Row,

        /// Iterate all rows with mutable access (for UPDATE, ALTER TABLE)
        scanMut: *const fn (ptr: *anyopaque) []Row,

        /// Number of rows
        len: *const fn (ptr: *anyopaque) usize,

        /// Append a row
        append: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, row: Row) error{OutOfMemory}!void,

        /// Remove row at index (ordered, shifts subsequent elements down)
        /// Returns the removed row
        orderedRemove: *const fn (ptr: *anyopaque, index: usize) Row,

        /// Clear all rows (retaining capacity for subsequent inserts)
        clearRetainingCapacity: *const fn (ptr: *anyopaque) void,

        /// Release all storage resources
        deinit: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    pub fn scan(self: RowStorage) []const Row {
        return self.vtable.scan(self.ptr);
    }

    pub fn scanMut(self: RowStorage) []Row {
        return self.vtable.scanMut(self.ptr);
    }

    pub fn len(self: RowStorage) usize {
        return self.vtable.len(self.ptr);
    }

    pub fn append(self: RowStorage, allocator: std.mem.Allocator, row: Row) error{OutOfMemory}!void {
        return self.vtable.append(self.ptr, allocator, row);
    }

    pub fn orderedRemove(self: RowStorage, index: usize) Row {
        return self.vtable.orderedRemove(self.ptr, index);
    }

    pub fn clearRetainingCapacity(self: RowStorage) void {
        return self.vtable.clearRetainingCapacity(self.ptr);
    }

    pub fn deinit(self: RowStorage, allocator: std.mem.Allocator) void {
        return self.vtable.deinit(self.ptr, allocator);
    }
};
