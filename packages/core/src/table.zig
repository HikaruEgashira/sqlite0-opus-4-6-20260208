const std = @import("std");
const value_mod = @import("value.zig");
const parser = @import("parser");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Column = value_mod.Column;
const dupeStr = value_mod.dupeStr;
const compareValuesOrder = value_mod.compareValuesOrder;
const likeMatch = value_mod.likeMatch;
const WhereClause = parser.WhereClause;
const CompOp = parser.CompOp;

pub const Table = struct {
    name: []const u8,
    columns: []const Column,
    rows: std.ArrayList(Row),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, columns: []const Column) Table {
        return .{
            .name = name,
            .columns = columns,
            .rows = .{},
            .allocator = allocator,
        };
    }

    pub fn insertRow(self: *Table, raw_values: []const []const u8) !void {
        var values = try self.allocator.alloc(Value, raw_values.len);
        for (raw_values, 0..) |raw, i| {
            if (std.mem.eql(u8, raw, "NULL")) {
                values[i] = .null_val;
            } else if (raw.len >= 2 and raw[0] == '\'') {
                values[i] = .{ .text = try dupeStr(self.allocator, raw[1 .. raw.len - 1]) };
            } else {
                const num = std.fmt.parseInt(i64, raw, 10) catch {
                    values[i] = .{ .text = try dupeStr(self.allocator, raw) };
                    continue;
                };
                values[i] = .{ .integer = num };
            }
        }
        try self.rows.append(self.allocator, .{ .values = values });
    }

    pub fn findColumnIndex(self: *const Table, col_name: []const u8) ?usize {
        for (self.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col_name, col.name)) return i;
        }
        return null;
    }

    pub fn matchesSingleCondition(self: *const Table, row: Row, column: []const u8, op: CompOp, value: []const u8) bool {
        const col_idx = self.findColumnIndex(column) orelse return false;
        const val = row.values[col_idx];
        // Handle IS NULL / IS NOT NULL
        if (op == .is_null) return val == .null_val;
        if (op == .is_not_null) return val != .null_val;
        // in_subquery should be resolved before reaching here
        if (op == .in_subquery) return false;
        // Handle LIKE pattern matching
        if (op == .like) {
            if (val != .text) return false;
            const pattern_val = parseRawValue(value);
            if (pattern_val != .text) return false;
            // Call likeMatch
            return likeMatch(val.text, pattern_val.text) catch false;
        }
        const where_val = parseRawValue(value);
        return compareValues(val, where_val, op);
    }

    pub fn matchesWhere(self: *const Table, row: Row, where: WhereClause) bool {
        var result = self.matchesSingleCondition(row, where.column, where.op, where.value);

        for (where.extra, 0..) |cond, i| {
            const cond_result = self.matchesSingleCondition(row, cond.column, cond.op, cond.value);
            switch (where.connectors[i]) {
                .and_op => result = result and cond_result,
                .or_op => result = result or cond_result,
            }
        }

        return result;
    }

    pub fn parseRawValue(raw: []const u8) Value {
        if (raw.len >= 2 and raw[0] == '\'') {
            return .{ .text = raw[1 .. raw.len - 1] };
        }
        const num = std.fmt.parseInt(i64, raw, 10) catch {
            return .{ .text = raw };
        };
        return .{ .integer = num };
    }

    pub fn compareValues(a: Value, b: Value, op: CompOp) bool {
        // NULL comparisons: any comparison involving NULL returns false (SQL standard)
        if (a == .null_val or b == .null_val) return false;
        // IS NULL / IS NOT NULL / in_subquery / like handled elsewhere
        if (op == .is_null or op == .is_not_null or op == .in_subquery or op == .like) return false;

        // Integer vs Integer
        if (a == .integer and b == .integer) {
            return switch (op) {
                .eq => a.integer == b.integer,
                .ne => a.integer != b.integer,
                .lt => a.integer < b.integer,
                .le => a.integer <= b.integer,
                .gt => a.integer > b.integer,
                .ge => a.integer >= b.integer,
                .is_null, .is_not_null, .in_subquery, .like => unreachable,
            };
        }
        // Text vs Text
        if (a == .text and b == .text) {
            const ord = std.mem.order(u8, a.text, b.text);
            return switch (op) {
                .eq => ord == .eq,
                .ne => ord != .eq,
                .lt => ord == .lt,
                .le => ord == .lt or ord == .eq,
                .gt => ord == .gt,
                .ge => ord == .gt or ord == .eq,
                .is_null, .is_not_null, .in_subquery, .like => unreachable,
            };
        }
        // Mismatched types: try numeric coercion (e.g., text "150.0" vs integer 150)
        if (a == .text and b == .integer) {
            const f_a = std.fmt.parseFloat(f64, a.text) catch return switch (op) {
                .ne => true,
                else => false,
            };
            const f_b: f64 = @floatFromInt(b.integer);
            return switch (op) {
                .eq => f_a == f_b,
                .ne => f_a != f_b,
                .lt => f_a < f_b,
                .le => f_a <= f_b,
                .gt => f_a > f_b,
                .ge => f_a >= f_b,
                .is_null, .is_not_null, .in_subquery, .like => false,
            };
        }
        if (a == .integer and b == .text) {
            const f_a: f64 = @floatFromInt(a.integer);
            const f_b = std.fmt.parseFloat(f64, b.text) catch return switch (op) {
                .ne => true,
                else => false,
            };
            return switch (op) {
                .eq => f_a == f_b,
                .ne => f_a != f_b,
                .lt => f_a < f_b,
                .le => f_a <= f_b,
                .gt => f_a > f_b,
                .ge => f_a >= f_b,
                .is_null, .is_not_null, .in_subquery, .like => false,
            };
        }
        return switch (op) {
            .ne => true,
            else => false,
        };
    }

    pub fn freeRow(self: *Table, row: Row) void {
        for (row.values) |val| {
            switch (val) {
                .text => |t| self.allocator.free(t),
                else => {},
            }
        }
        self.allocator.free(row.values);
    }

    pub fn deinit(self: *Table) void {
        for (self.rows.items) |row| {
            self.freeRow(row);
        }
        self.rows.deinit(self.allocator);
        for (self.columns) |col| {
            self.allocator.free(col.name);
        }
        self.allocator.free(self.columns);
        self.allocator.free(self.name);
    }
};
