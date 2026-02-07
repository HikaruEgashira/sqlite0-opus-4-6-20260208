const std = @import("std");
const tokenizer = @import("tokenizer");
const parser = @import("parser");

pub const Tokenizer = tokenizer.Tokenizer;
pub const Parser = parser.Parser;
pub const Statement = parser.Statement;
pub const WhereClause = parser.WhereClause;
pub const OrderByClause = parser.OrderByClause;
pub const CompOp = parser.CompOp;

pub const Value = union(enum) {
    integer: i64,
    text: []const u8,
    null_val: void,
};

pub const Row = struct {
    values: []Value,
};

pub const Column = struct {
    name: []const u8,
    col_type: []const u8,
    is_primary_key: bool,
};

fn dupeStr(allocator: std.mem.Allocator, s: []const u8) ![]const u8 {
    const copy = try allocator.alloc(u8, s.len);
    @memcpy(copy, s);
    return copy;
}

fn compareRowsByValue(a: Value, b: Value) std.math.Order {
    if (a == .integer and b == .integer) {
        if (a.integer < b.integer) return .lt;
        if (a.integer > b.integer) return .gt;
        return .eq;
    }
    if (a == .text and b == .text) {
        return std.mem.order(u8, a.text, b.text);
    }
    return .eq;
}

pub const RowComparator = struct {
    col_idx: usize,
    is_desc: bool,

    pub fn compare(ctx: @This(), a: Row, b: Row) bool {
        const ord = compareRowsByValue(a.values[ctx.col_idx], b.values[ctx.col_idx]);
        return if (ctx.is_desc) ord == .gt else ord == .lt;
    }
};

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
            if (raw.len >= 2 and raw[0] == '\'') {
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

    pub fn matchesWhere(self: *const Table, row: Row, where: WhereClause) bool {
        const col_idx = self.findColumnIndex(where.column) orelse return false;
        const val = row.values[col_idx];

        // Parse the where value
        const where_val = parseRawValue(where.value);

        return compareValues(val, where_val, where.op);
    }

    fn parseRawValue(raw: []const u8) Value {
        if (raw.len >= 2 and raw[0] == '\'') {
            return .{ .text = raw[1 .. raw.len - 1] };
        }
        const num = std.fmt.parseInt(i64, raw, 10) catch {
            return .{ .text = raw };
        };
        return .{ .integer = num };
    }

    fn compareValues(a: Value, b: Value, op: CompOp) bool {
        // Integer vs Integer
        if (a == .integer and b == .integer) {
            return switch (op) {
                .eq => a.integer == b.integer,
                .ne => a.integer != b.integer,
                .lt => a.integer < b.integer,
                .le => a.integer <= b.integer,
                .gt => a.integer > b.integer,
                .ge => a.integer >= b.integer,
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
            };
        }
        // Mismatched types: only eq/ne are meaningful
        return switch (op) {
            .ne => true,
            else => false,
        };
    }

    fn freeRow(self: *Table, row: Row) void {
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

pub const Database = struct {
    tables: std.StringHashMap(Table),
    allocator: std.mem.Allocator,
    // Temporary storage for projected rows (freed on next execute)
    projected_rows: ?[]Row = null,
    projected_values: ?[][]Value = null,

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{
            .tables = std.StringHashMap(Table).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Database) void {
        self.freeProjected();
        var it = self.tables.valueIterator();
        while (it.next()) |table| {
            table.deinit();
        }
        self.tables.deinit();
    }

    fn freeProjected(self: *Database) void {
        if (self.projected_values) |pv| {
            for (pv) |v| {
                self.allocator.free(v);
            }
            self.allocator.free(pv);
            self.projected_values = null;
        }
        if (self.projected_rows) |pr| {
            self.allocator.free(pr);
            self.projected_rows = null;
        }
    }

    pub fn execute(self: *Database, sql: []const u8) !ExecuteResult {
        self.freeProjected();
        var tok = Tokenizer.init(sql);
        const tokens = try tok.tokenize(self.allocator);
        defer self.allocator.free(tokens);

        var p = Parser.init(self.allocator, tokens);
        const stmt = try p.parse();

        switch (stmt) {
            .create_table => |ct| {
                defer self.allocator.free(ct.columns);
                const table_name = try dupeStr(self.allocator, ct.table_name);
                var columns = try self.allocator.alloc(Column, ct.columns.len);
                for (ct.columns, 0..) |col, i| {
                    columns[i] = .{
                        .name = try dupeStr(self.allocator, col.name),
                        .col_type = col.col_type,
                        .is_primary_key = col.is_primary_key,
                    };
                }
                var table = Table.init(self.allocator, table_name, columns);
                _ = &table;
                try self.tables.put(table_name, table);
                return .ok;
            },
            .insert => |ins| {
                defer self.allocator.free(ins.values);
                if (self.tables.getPtr(ins.table_name)) |table| {
                    try table.insertRow(ins.values);
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
            .select_stmt => |sel| {
                defer self.allocator.free(sel.columns);
                if (self.tables.getPtr(sel.table_name)) |table| {
                    // Collect matching rows (with optional WHERE filter)
                    var matching_rows: std.ArrayList(Row) = .{};
                    defer matching_rows.deinit(self.allocator);
                    for (table.rows.items) |row| {
                        if (sel.where) |where| {
                            if (!table.matchesWhere(row, where)) continue;
                        }
                        try matching_rows.append(self.allocator, row);
                    }

                    // Apply ORDER BY if specified
                    if (sel.order_by) |order_by| {
                        const col_idx = table.findColumnIndex(order_by.column) orelse {
                            return .{ .err = "column not found" };
                        };
                        const ctx = RowComparator{ .col_idx = col_idx, .is_desc = order_by.is_desc };
                        std.mem.sort(Row, matching_rows.items, ctx, RowComparator.compare);
                    }

                    // Apply LIMIT and OFFSET
                    const offset_val = sel.offset orelse 0;
                    const remaining = if (offset_val < matching_rows.items.len)
                        matching_rows.items.len - offset_val
                    else
                        0;
                    const final_count = if (sel.limit) |limit_val|
                        @min(limit_val, remaining)
                    else
                        remaining;

                    if (sel.columns.len == 0) {
                        // SELECT * — copy matched rows to projected storage
                        const proj_rows = try self.allocator.alloc(Row, final_count);
                        if (final_count > 0) {
                            @memcpy(proj_rows, matching_rows.items[offset_val .. offset_val + final_count]);
                        }
                        // No per-row value arrays to track for SELECT *
                        self.projected_rows = proj_rows;
                        return .{ .rows = proj_rows };
                    }
                    // Column projection: resolve column indices
                    var col_indices = try self.allocator.alloc(usize, sel.columns.len);
                    defer self.allocator.free(col_indices);
                    for (sel.columns, 0..) |sel_col, i| {
                        var found = false;
                        for (table.columns, 0..) |tbl_col, j| {
                            if (std.mem.eql(u8, sel_col, tbl_col.name)) {
                                col_indices[i] = j;
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            return .{ .err = "column not found" };
                        }
                    }
                    // Build projected rows
                    var proj_values = try self.allocator.alloc([]Value, final_count);
                    var proj_rows = try self.allocator.alloc(Row, final_count);
                    for (0..final_count) |i| {
                        const src_idx = offset_val + i;
                        const row = matching_rows.items[src_idx];
                        var values = try self.allocator.alloc(Value, sel.columns.len);
                        for (col_indices, 0..) |src_col_idx, dst_idx| {
                            values[dst_idx] = row.values[src_col_idx];
                        }
                        proj_values[i] = values;
                        proj_rows[i] = .{ .values = values };
                    }
                    self.projected_rows = proj_rows;
                    self.projected_values = proj_values;
                    return .{ .rows = proj_rows };
                }
                return .{ .err = "table not found" };
            },
            .delete => |del| {
                if (self.tables.getPtr(del.table_name)) |table| {
                    if (del.where) |where| {
                        // Delete matching rows (iterate backwards to avoid index issues)
                        var i: usize = table.rows.items.len;
                        while (i > 0) {
                            i -= 1;
                            if (table.matchesWhere(table.rows.items[i], where)) {
                                table.freeRow(table.rows.items[i]);
                                _ = table.rows.orderedRemove(i);
                            }
                        }
                    } else {
                        // DELETE without WHERE: delete all rows
                        for (table.rows.items) |row| {
                            table.freeRow(row);
                        }
                        table.rows.clearRetainingCapacity();
                    }
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
            .update => |upd| {
                if (self.tables.getPtr(upd.table_name)) |table| {
                    const set_col_idx = table.findColumnIndex(upd.set_column) orelse {
                        return .{ .err = "column not found" };
                    };
                    for (table.rows.items) |*row| {
                        const matches = if (upd.where) |where| table.matchesWhere(row.*, where) else true;
                        if (matches) {
                            // Free old text value if needed
                            switch (row.values[set_col_idx]) {
                                .text => |t| self.allocator.free(t),
                                else => {},
                            }
                            // Set new value
                            if (upd.set_value.len >= 2 and upd.set_value[0] == '\'') {
                                row.values[set_col_idx] = .{ .text = try dupeStr(self.allocator, upd.set_value[1 .. upd.set_value.len - 1]) };
                            } else {
                                const num = std.fmt.parseInt(i64, upd.set_value, 10) catch {
                                    row.values[set_col_idx] = .{ .text = try dupeStr(self.allocator, upd.set_value) };
                                    continue;
                                };
                                row.values[set_col_idx] = .{ .integer = num };
                            }
                        }
                    }
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
            .drop_table => |dt| {
                if (self.tables.fetchRemove(dt.table_name)) |entry| {
                    var table = entry.value;
                    table.deinit();
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
        }
    }
};

pub const ExecuteResult = union(enum) {
    ok: void,
    rows: []const Row,
    err: []const u8,
};

test "create table and insert" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    const r1 = try db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
    try std.testing.expectEqual(ExecuteResult.ok, r1);

    const r2 = try db.execute("INSERT INTO users VALUES (1, 'alice');");
    try std.testing.expectEqual(ExecuteResult.ok, r2);

    const r3 = try db.execute("SELECT * FROM users;");
    switch (r3) {
        .rows => |rows| {
            try std.testing.expectEqual(@as(usize, 1), rows.len);
            try std.testing.expectEqual(Value{ .integer = 1 }, rows[0].values[0]);
            switch (rows[0].values[1]) {
                .text => |t| try std.testing.expectEqualStrings("alice", t),
                else => return error.UnexpectedToken,
            }
        },
        else => return error.UnexpectedToken,
    }
}
