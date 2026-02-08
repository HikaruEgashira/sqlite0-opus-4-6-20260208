const std = @import("std");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");
const parser = @import("parser");
const convert_mod = @import("convert.zig");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Column = value_mod.Column;
const Table = table_mod.Table;
const Statement = parser.Statement;
const dupeStr = value_mod.dupeStr;

const root = @import("root.zig");
const Database = root.Database;
const ExecuteResult = root.ExecuteResult;

pub fn executePragma(self: *Database, pragma: Statement.Pragma) !ExecuteResult {
    if (std.ascii.eqlIgnoreCase(pragma.name, "table_info")) {
        const table_name = pragma.arg orelse return .{ .err = "PRAGMA table_info requires a table name" };
        const table = self.tables.getPtr(table_name) orelse return .{ .err = "no such table" };

        const col_count = table.columns.len;
        var result_rows = self.allocator.alloc(Row, col_count) catch return .{ .err = "out of memory" };
        var result_values = self.allocator.alloc([]Value, col_count) catch return .{ .err = "out of memory" };
        var texts: std.ArrayList([]const u8) = .{};

        for (table.columns, 0..) |col, i| {
            var vals = self.allocator.alloc(Value, 6) catch return .{ .err = "out of memory" };
            vals[0] = .{ .integer = @intCast(i) };
            const name_t = dupeStr(self.allocator, col.name) catch return .{ .err = "out of memory" };
            vals[1] = .{ .text = name_t };
            texts.append(self.allocator, name_t) catch {};
            const type_t = dupeStr(self.allocator, col.col_type) catch return .{ .err = "out of memory" };
            vals[2] = .{ .text = type_t };
            texts.append(self.allocator, type_t) catch {};
            vals[3] = .{ .integer = if (col.not_null) 1 else 0 };
            if (col.default_value) |dv| {
                const dv_t = dupeStr(self.allocator, dv) catch return .{ .err = "out of memory" };
                vals[4] = .{ .text = dv_t };
                texts.append(self.allocator, dv_t) catch {};
            } else {
                vals[4] = .null_val;
            }
            vals[5] = .{ .integer = if (col.is_primary_key) 1 else 0 };
            result_values[i] = vals;
            result_rows[i] = .{ .values = vals };
        }

        var col_names = self.allocator.alloc([]const u8, 6) catch return .{ .err = "out of memory" };
        col_names[0] = dupeStr(self.allocator, "cid") catch return .{ .err = "out of memory" };
        col_names[1] = dupeStr(self.allocator, "name") catch return .{ .err = "out of memory" };
        col_names[2] = dupeStr(self.allocator, "type") catch return .{ .err = "out of memory" };
        col_names[3] = dupeStr(self.allocator, "notnull") catch return .{ .err = "out of memory" };
        col_names[4] = dupeStr(self.allocator, "dflt_value") catch return .{ .err = "out of memory" };
        col_names[5] = dupeStr(self.allocator, "pk") catch return .{ .err = "out of memory" };
        self.projected_column_names = col_names;
        self.projected_rows = result_rows;
        self.projected_values = result_values;
        self.projected_texts = texts.toOwnedSlice(self.allocator) catch null;

        return .{ .rows = result_rows };
    } else if (std.ascii.eqlIgnoreCase(pragma.name, "table_list")) {
        var count: usize = 0;
        var tit = self.tables.iterator();
        while (tit.next()) |_| count += 1;

        var result_rows = self.allocator.alloc(Row, count) catch return .{ .err = "out of memory" };
        var result_values = self.allocator.alloc([]Value, count) catch return .{ .err = "out of memory" };
        var texts: std.ArrayList([]const u8) = .{};

        var it = self.tables.iterator();
        var idx: usize = 0;
        while (it.next()) |entry| {
            var vals = self.allocator.alloc(Value, 3) catch return .{ .err = "out of memory" };
            const t0 = dupeStr(self.allocator, "main") catch return .{ .err = "out of memory" };
            vals[0] = .{ .text = t0 };
            texts.append(self.allocator, t0) catch {};
            const t1 = dupeStr(self.allocator, entry.key_ptr.*) catch return .{ .err = "out of memory" };
            vals[1] = .{ .text = t1 };
            texts.append(self.allocator, t1) catch {};
            const t2 = dupeStr(self.allocator, "table") catch return .{ .err = "out of memory" };
            vals[2] = .{ .text = t2 };
            texts.append(self.allocator, t2) catch {};
            result_values[idx] = vals;
            result_rows[idx] = .{ .values = vals };
            idx += 1;
        }

        var col_names = self.allocator.alloc([]const u8, 3) catch return .{ .err = "out of memory" };
        col_names[0] = dupeStr(self.allocator, "schema") catch return .{ .err = "out of memory" };
        col_names[1] = dupeStr(self.allocator, "name") catch return .{ .err = "out of memory" };
        col_names[2] = dupeStr(self.allocator, "type") catch return .{ .err = "out of memory" };
        self.projected_column_names = col_names;
        self.projected_rows = result_rows;
        self.projected_values = result_values;
        self.projected_texts = texts.toOwnedSlice(self.allocator) catch null;

        return .{ .rows = result_rows };
    } else {
        return .ok;
    }
}

pub fn executeSqliteMaster(self: *Database, sel: Statement.Select) !ExecuteResult {
    var all_rows: std.ArrayList(Row) = .{};
    var texts: std.ArrayList([]const u8) = .{};
    defer all_rows.deinit(self.allocator);

    const TrackCtx = struct {
        alloc: std.mem.Allocator,
        texts: *std.ArrayList([]const u8),
        fn track(ctx: @This(), s: []const u8) []const u8 {
            const t = dupeStr(ctx.alloc, s) catch return "";
            ctx.texts.append(ctx.alloc, t) catch {};
            return t;
        }
        fn trackOwned(ctx: @This(), s: []const u8) []const u8 {
            ctx.texts.append(ctx.alloc, s) catch {};
            return s;
        }
    };
    const ctx = TrackCtx{ .alloc = self.allocator, .texts = &texts };

    // Add tables
    var tit = self.tables.iterator();
    while (tit.next()) |entry| {
        const tbl_name = entry.key_ptr.*;
        const tbl = entry.value_ptr.*;
        var sql_buf: std.ArrayList(u8) = .{};
        sql_buf.appendSlice(self.allocator, "CREATE TABLE ") catch continue;
        sql_buf.appendSlice(self.allocator, tbl_name) catch continue;
        sql_buf.appendSlice(self.allocator, "(") catch continue;
        for (tbl.columns, 0..) |col, ci| {
            if (ci > 0) sql_buf.appendSlice(self.allocator, ", ") catch continue;
            sql_buf.appendSlice(self.allocator, col.name) catch continue;
            sql_buf.appendSlice(self.allocator, " ") catch continue;
            sql_buf.appendSlice(self.allocator, col.col_type) catch continue;
            if (col.is_primary_key) sql_buf.appendSlice(self.allocator, " PRIMARY KEY") catch continue;
            if (col.not_null) sql_buf.appendSlice(self.allocator, " NOT NULL") catch continue;
        }
        sql_buf.appendSlice(self.allocator, ")") catch continue;

        var vals = self.allocator.alloc(Value, 5) catch continue;
        vals[0] = .{ .text = ctx.track("table") };
        vals[1] = .{ .text = ctx.track(tbl_name) };
        vals[2] = .{ .text = ctx.track(tbl_name) };
        vals[3] = .{ .integer = 0 };
        vals[4] = .{ .text = ctx.trackOwned(sql_buf.toOwnedSlice(self.allocator) catch continue) };
        all_rows.append(self.allocator, .{ .values = vals }) catch continue;
    }

    // Add views
    var vit = self.views.iterator();
    while (vit.next()) |entry| {
        const view_name = entry.key_ptr.*;
        const view_sql = entry.value_ptr.*;
        var vals = self.allocator.alloc(Value, 5) catch continue;
        vals[0] = .{ .text = ctx.track("view") };
        vals[1] = .{ .text = ctx.track(view_name) };
        vals[2] = .{ .text = ctx.track(view_name) };
        vals[3] = .{ .integer = 0 };
        var vsql_buf: std.ArrayList(u8) = .{};
        vsql_buf.appendSlice(self.allocator, "CREATE VIEW ") catch continue;
        vsql_buf.appendSlice(self.allocator, view_name) catch continue;
        vsql_buf.appendSlice(self.allocator, " AS ") catch continue;
        vsql_buf.appendSlice(self.allocator, view_sql) catch continue;
        vals[4] = .{ .text = ctx.trackOwned(vsql_buf.toOwnedSlice(self.allocator) catch continue) };
        all_rows.append(self.allocator, .{ .values = vals }) catch continue;
    }

    // Create temp table schema for WHERE eval
    var master_cols = [_]Column{
        .{ .name = "type", .col_type = "TEXT", .is_primary_key = false },
        .{ .name = "name", .col_type = "TEXT", .is_primary_key = false },
        .{ .name = "tbl_name", .col_type = "TEXT", .is_primary_key = false },
        .{ .name = "rootpage", .col_type = "INTEGER", .is_primary_key = false },
        .{ .name = "sql", .col_type = "TEXT", .is_primary_key = false },
    };
    var tmp_table = Table.init(self.allocator, "sqlite_master", &master_cols);
    defer tmp_table.row_storage.storage().deinit(self.allocator);
    _ = &tmp_table;

    // Apply WHERE filter
    var filtered_rows: std.ArrayList(Row) = .{};
    defer filtered_rows.deinit(self.allocator);

    for (all_rows.items) |row| {
        var keep = true;
        if (sel.where_expr) |we| {
            const val = try self.evalExpr(we, &tmp_table, row);
            defer if (val == .text) self.allocator.free(val.text);
            keep = convert_mod.valueToBool(val);
        }
        if (keep) {
            filtered_rows.append(self.allocator, row) catch continue;
        } else {
            self.allocator.free(row.values);
        }
    }

    // Sort by type, name
    const SortCtx = struct {
        pub fn lessThan(_: void, a: Row, b: Row) bool {
            const a_type = if (a.values[0] == .text) a.values[0].text else "";
            const b_type = if (b.values[0] == .text) b.values[0].text else "";
            const type_cmp = std.mem.order(u8, a_type, b_type);
            if (type_cmp != .eq) return type_cmp == .lt;
            const a_name = if (a.values[1] == .text) a.values[1].text else "";
            const b_name = if (b.values[1] == .text) b.values[1].text else "";
            return std.mem.order(u8, a_name, b_name) == .lt;
        }
    };
    std.mem.sort(Row, filtered_rows.items, {}, SortCtx.lessThan);

    // Apply LIMIT/OFFSET
    var start: usize = 0;
    if (sel.offset) |off| {
        start = @min(@as(usize, @intCast(@max(off, 0))), filtered_rows.items.len);
    }
    var count = filtered_rows.items.len - start;
    if (sel.limit) |lim| {
        count = @min(@as(usize, @intCast(@max(lim, 0))), count);
    }

    var result_rows = self.allocator.alloc(Row, count) catch return .{ .err = "out of memory" };
    var result_values = self.allocator.alloc([]Value, count) catch return .{ .err = "out of memory" };

    for (0..count) |i| {
        result_values[i] = filtered_rows.items[start + i].values;
        result_rows[i] = .{ .values = result_values[i] };
    }
    for (0..start) |i| self.allocator.free(filtered_rows.items[i].values);
    for (start + count..filtered_rows.items.len) |i| self.allocator.free(filtered_rows.items[i].values);

    var col_names = self.allocator.alloc([]const u8, 5) catch return .{ .err = "out of memory" };
    col_names[0] = dupeStr(self.allocator, "type") catch return .{ .err = "out of memory" };
    col_names[1] = dupeStr(self.allocator, "name") catch return .{ .err = "out of memory" };
    col_names[2] = dupeStr(self.allocator, "tbl_name") catch return .{ .err = "out of memory" };
    col_names[3] = dupeStr(self.allocator, "rootpage") catch return .{ .err = "out of memory" };
    col_names[4] = dupeStr(self.allocator, "sql") catch return .{ .err = "out of memory" };
    self.projected_column_names = col_names;
    self.projected_rows = result_rows;
    self.projected_values = result_values;
    self.projected_texts = texts.toOwnedSlice(self.allocator) catch null;
    return .{ .rows = result_rows };
}
