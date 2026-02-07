const std = @import("std");
const tokenizer = @import("tokenizer");
const parser = @import("parser");

pub const Tokenizer = tokenizer.Tokenizer;
pub const Parser = parser.Parser;
pub const Statement = parser.Statement;
pub const WhereClause = parser.WhereClause;
pub const CompOp = parser.CompOp;
pub const OrderByClause = parser.OrderByClause;
pub const SortOrder = parser.SortOrder;
pub const SelectExpr = parser.SelectExpr;
pub const AggFunc = parser.AggFunc;

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

fn compareValuesOrder(a: Value, b: Value) std.math.Order {
    // Integer vs Integer
    if (a == .integer and b == .integer) {
        return std.math.order(a.integer, b.integer);
    }
    // Text vs Text
    if (a == .text and b == .text) {
        return std.mem.order(u8, a.text, b.text);
    }
    // Integer < Text (SQLite3 type affinity: integers sort before text)
    if (a == .integer and b == .text) return .lt;
    if (a == .text and b == .integer) return .gt;
    // null_val sorts before everything
    if (a == .null_val and b == .null_val) return .eq;
    if (a == .null_val) return .lt;
    if (b == .null_val) return .gt;
    return .eq;
}

pub const Database = struct {
    tables: std.StringHashMap(Table),
    allocator: std.mem.Allocator,
    // Temporary storage for projected rows (freed on next execute)
    projected_rows: ?[]Row = null,
    projected_values: ?[][]Value = null,
    projected_texts: ?[][]const u8 = null, // Text values allocated by aggregate functions

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
        if (self.projected_texts) |pt| {
            for (pt) |t| {
                self.allocator.free(t);
            }
            self.allocator.free(pt);
            self.projected_texts = null;
        }
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

    fn computeAgg(self: *Database, func: AggFunc, arg: []const u8, tbl: *const Table, rows: []const Row) !Value {
        if (func == .count) {
            if (std.mem.eql(u8, arg, "*")) {
                return .{ .integer = @intCast(rows.len) };
            }
            const col_idx = tbl.findColumnIndex(arg) orelse return error.UnexpectedToken;
            var cnt: i64 = 0;
            for (rows) |row| {
                if (row.values[col_idx] != .null_val) cnt += 1;
            }
            return .{ .integer = cnt };
        }

        const col_idx = tbl.findColumnIndex(arg) orelse return error.UnexpectedToken;

        switch (func) {
            .sum => {
                var total: i64 = 0;
                for (rows) |row| {
                    if (row.values[col_idx] == .integer) {
                        total += row.values[col_idx].integer;
                    }
                }
                return .{ .integer = total };
            },
            .avg => {
                var total: f64 = 0;
                var cnt: usize = 0;
                for (rows) |row| {
                    if (row.values[col_idx] == .integer) {
                        total += @floatFromInt(row.values[col_idx].integer);
                        cnt += 1;
                    }
                }
                if (cnt == 0) return .{ .integer = 0 };
                const avg = total / @as(f64, @floatFromInt(cnt));
                // Format as SQLite3-compatible float string
                var buf: [64]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{avg}) catch return error.OutOfMemory;
                // Ensure decimal point (SQLite3 always outputs float for AVG)
                const has_dot = blk: {
                    for (slice) |ch| {
                        if (ch == '.') break :blk true;
                    }
                    break :blk false;
                };
                if (has_dot) {
                    return .{ .text = try dupeStr(self.allocator, slice) };
                } else {
                    // Append ".0"
                    var dot_buf: [66]u8 = undefined;
                    @memcpy(dot_buf[0..slice.len], slice);
                    dot_buf[slice.len] = '.';
                    dot_buf[slice.len + 1] = '0';
                    return .{ .text = try dupeStr(self.allocator, dot_buf[0 .. slice.len + 2]) };
                }
            },
            .min => {
                if (rows.len == 0) return .null_val;
                var min_val = rows[0].values[col_idx];
                for (rows[1..]) |row| {
                    const v = row.values[col_idx];
                    if (compareValuesOrder(v, min_val) == .lt) {
                        min_val = v;
                    }
                }
                return min_val;
            },
            .max => {
                if (rows.len == 0) return .null_val;
                var max_val = rows[0].values[col_idx];
                for (rows[1..]) |row| {
                    const v = row.values[col_idx];
                    if (compareValuesOrder(v, max_val) == .gt) {
                        max_val = v;
                    }
                }
                return max_val;
            },
            else => return .null_val,
        }
    }

    fn collectAggTexts(self: *Database, sel_exprs: []const SelectExpr, all_values: []const []Value) !void {
        var texts: std.ArrayList([]const u8) = .{};
        for (all_values) |values| {
            for (sel_exprs, 0..) |expr, i| {
                // Only track text values from aggregate expressions (e.g., AVG)
                if (expr == .aggregate and values[i] == .text) {
                    texts.append(self.allocator, values[i].text) catch {};
                }
            }
        }
        if (texts.items.len > 0) {
            self.projected_texts = texts.toOwnedSlice(self.allocator) catch null;
        } else {
            texts.deinit(self.allocator);
        }
    }

    fn executeAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row) !ExecuteResult {
        if (sel.group_by) |gb_col| {
            return self.executeGroupByAggregate(tbl, sel, rows, gb_col);
        }

        var values = try self.allocator.alloc(Value, sel.select_exprs.len);
        for (sel.select_exprs, 0..) |expr, i| {
            switch (expr) {
                .aggregate => |agg| {
                    values[i] = try self.computeAgg(agg.func, agg.arg, tbl, rows);
                },
                .column => |col_name| {
                    const col_idx = tbl.findColumnIndex(col_name) orelse return .{ .err = "column not found" };
                    if (rows.len > 0) {
                        values[i] = rows[0].values[col_idx];
                    } else {
                        values[i] = .null_val;
                    }
                },
            }
        }
        var proj_values = try self.allocator.alloc([]Value, 1);
        proj_values[0] = values;
        var proj_rows = try self.allocator.alloc(Row, 1);
        proj_rows[0] = .{ .values = values };
        self.projected_rows = proj_rows;
        self.projected_values = proj_values;
        // Track allocated text values (e.g., AVG results)
        try self.collectAggTexts(sel.select_exprs, proj_values);
        return .{ .rows = proj_rows };
    }

    fn executeGroupByAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row, gb_col: []const u8) !ExecuteResult {
        const gb_idx = tbl.findColumnIndex(gb_col) orelse return .{ .err = "column not found" };

        var group_keys: std.ArrayList(Value) = .{};
        defer group_keys.deinit(self.allocator);
        var group_rows: std.ArrayList(std.ArrayList(Row)) = .{};
        defer {
            for (group_rows.items) |*gr| {
                gr.deinit(self.allocator);
            }
            group_rows.deinit(self.allocator);
        }

        for (rows) |row| {
            const key = row.values[gb_idx];
            var found_idx: ?usize = null;
            for (group_keys.items, 0..) |gk, gi| {
                if (compareValuesOrder(key, gk) == .eq) {
                    found_idx = gi;
                    break;
                }
            }
            if (found_idx) |idx| {
                try group_rows.items[idx].append(self.allocator, row);
            } else {
                try group_keys.append(self.allocator, key);
                var new_list: std.ArrayList(Row) = .{};
                try new_list.append(self.allocator, row);
                try group_rows.append(self.allocator, new_list);
            }
        }

        const n_groups = group_keys.items.len;
        var proj_values = try self.allocator.alloc([]Value, n_groups);
        var proj_rows = try self.allocator.alloc(Row, n_groups);

        for (0..n_groups) |gi| {
            var values = try self.allocator.alloc(Value, sel.select_exprs.len);
            const grp = group_rows.items[gi].items;
            for (sel.select_exprs, 0..) |expr, ei| {
                switch (expr) {
                    .aggregate => |agg| {
                        values[ei] = try self.computeAgg(agg.func, agg.arg, tbl, grp);
                    },
                    .column => |col_name| {
                        const col_idx = tbl.findColumnIndex(col_name) orelse return .{ .err = "column not found" };
                        values[ei] = grp[0].values[col_idx];
                    },
                }
            }
            proj_values[gi] = values;
            proj_rows[gi] = .{ .values = values };
        }

        // Sort groups by group key to match sqlite3 output order
        // Find the index of the group-by column in select_exprs
        var gb_expr_idx: ?usize = null;
        for (sel.select_exprs, 0..) |expr, ei| {
            switch (expr) {
                .column => |col_name| {
                    if (std.mem.eql(u8, col_name, gb_col)) {
                        gb_expr_idx = ei;
                        break;
                    }
                },
                else => {},
            }
        }
        if (gb_expr_idx) |si| {
            const GBSortCtx = struct {
                idx: usize,
                fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                    return compareValuesOrder(a.values[ctx.idx], b.values[ctx.idx]) == .lt;
                }
            };
            const ctx = GBSortCtx{ .idx = si };
            std.mem.sort(Row, proj_rows, ctx, GBSortCtx.lessThan);
            std.mem.sort([]Value, proj_values, ctx, struct {
                fn lessThan(c: GBSortCtx, a: []Value, b: []Value) bool {
                    return compareValuesOrder(a[c.idx], b[c.idx]) == .lt;
                }
            }.lessThan);
        }

        self.projected_rows = proj_rows;
        self.projected_values = proj_values;
        try self.collectAggTexts(sel.select_exprs, proj_values);
        return .{ .rows = proj_rows };
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
                defer self.allocator.free(sel.select_exprs);
                if (self.tables.get(sel.table_name)) |table| {
                    // Collect matching rows (with optional WHERE filter)
                    var matching_rows: std.ArrayList(Row) = .{};
                    defer matching_rows.deinit(self.allocator);
                    for (table.rows.items) |row| {
                        if (sel.where) |where| {
                            if (!table.matchesWhere(row, where)) continue;
                        }
                        try matching_rows.append(self.allocator, row);
                    }

                    // Check if this is an aggregate query
                    const has_agg = blk: {
                        for (sel.select_exprs) |expr| {
                            if (expr == .aggregate) break :blk true;
                        }
                        break :blk false;
                    };

                    if (has_agg) {
                        return self.executeAggregate(&table, sel, matching_rows.items);
                    }

                    // Apply ORDER BY
                    if (sel.order_by) |order_by| {
                        const sort_col_idx = table.findColumnIndex(order_by.column) orelse {
                            return .{ .err = "column not found" };
                        };
                        const desc = order_by.order == .desc;
                        const SortCtx = struct {
                            sort_col_idx: usize,
                            is_desc: bool,
                            fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                                const va = a.values[ctx.sort_col_idx];
                                const vb = b.values[ctx.sort_col_idx];
                                const cmp = compareValuesOrder(va, vb);
                                if (ctx.is_desc) {
                                    return cmp == .gt;
                                }
                                return cmp == .lt;
                            }
                        };
                        std.mem.sort(Row, matching_rows.items, SortCtx{ .sort_col_idx = sort_col_idx, .is_desc = desc }, SortCtx.lessThan);
                    }

                    // Apply LIMIT/OFFSET
                    const total = matching_rows.items.len;
                    var start: usize = 0;
                    var end_val: usize = total;
                    if (sel.offset) |off| {
                        if (off > 0) {
                            start = @min(@as(usize, @intCast(off)), total);
                        }
                    }
                    if (sel.limit) |lim| {
                        if (lim >= 0) {
                            end_val = @min(start + @as(usize, @intCast(lim)), total);
                        }
                    }
                    const result_rows = matching_rows.items[start..end_val];

                    if (sel.columns.len == 0 and sel.select_exprs.len == 0) {
                        // SELECT *
                        const proj_rows = try self.allocator.alloc(Row, result_rows.len);
                        @memcpy(proj_rows, result_rows);
                        self.projected_rows = proj_rows;
                        return .{ .rows = proj_rows };
                    }
                    // Column projection
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
                    const row_count = result_rows.len;
                    var proj_values = try self.allocator.alloc([]Value, row_count);
                    var proj_rows = try self.allocator.alloc(Row, row_count);
                    for (result_rows, 0..) |row, ri| {
                        var values = try self.allocator.alloc(Value, sel.columns.len);
                        for (col_indices, 0..) |src_idx, dst_idx| {
                            values[dst_idx] = row.values[src_idx];
                        }
                        proj_values[ri] = values;
                        proj_rows[ri] = .{ .values = values };
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
