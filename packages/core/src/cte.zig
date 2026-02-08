const std = @import("std");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");
const parser = @import("parser");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Column = value_mod.Column;
const Table = table_mod.Table;
const dupeStr = value_mod.dupeStr;

const root = @import("root.zig");
const Database = root.Database;
const ExecuteResult = root.ExecuteResult;
const Statement = parser.Statement;

pub fn executeWithCTE(self: *Database, wc: Statement.WithCTE) !ExecuteResult {
    // Track CTE table names for deferred cleanup (in freeProjected)
    var cte_names: std.ArrayList([]const u8) = .{};

    // Execute each CTE and create temporary tables
    for (wc.ctes) |cte| {
        if (wc.is_recursive) {
            try executeRecursiveCTE(self, cte, &cte_names);
        } else {
            try executeNonRecursiveCTE(self, cte, &cte_names);
        }
    }

    // Register CTE names for deferred cleanup (cleaned up in freeProjected on next execute)
    // But hide them from the upcoming execute() call's freeProjected, which would delete them too early
    const cte_slice = cte_names.toOwnedSlice(self.allocator) catch null;

    // Execute the main statement (CTE tables are available as regular tables)
    const main_result = self.execute(wc.main_sql);

    // Now register for cleanup on next freeProjected call
    self.temp_cte_names = cte_slice;

    return main_result;
}

pub fn executeNonRecursiveCTE(self: *Database, cte: Statement.CteDef, cte_names: *std.ArrayList([]const u8)) !void {
    const saved_rows = self.projected_rows;
    const saved_values = self.projected_values;
    const saved_texts = self.projected_texts;
    const saved_col_names = self.projected_column_names;
    self.projected_rows = null;
    self.projected_values = null;
    self.projected_texts = null;
    self.projected_column_names = null;

    const result = try self.execute(cte.query_sql);

    switch (result) {
        .rows => |rows| {
            try createCTETable(self, cte.name, rows, cte.col_names);
            try cte_names.append(self.allocator, try dupeStr(self.allocator, cte.name));
        },
        .err => {},
        .ok => {},
    }

    self.freeProjected();
    self.projected_rows = saved_rows;
    self.projected_values = saved_values;
    self.projected_texts = saved_texts;
    self.projected_column_names = saved_col_names;
}

pub fn executeRecursiveCTE(self: *Database, cte: Statement.CteDef, cte_names: *std.ArrayList([]const u8)) !void {
    // Split query on UNION ALL to separate anchor and recursive parts
    const query = cte.query_sql;
    const split = findUnionAllSplit(query);
    if (split == null) {
        return executeNonRecursiveCTE(self, cte, cte_names);
    }

    const anchor_end = split.?[0];
    const recursive_start = split.?[1];

    // Build anchor SQL
    var anchor_sql = try self.allocator.alloc(u8, anchor_end + 1);
    defer self.allocator.free(anchor_sql);
    @memcpy(anchor_sql[0..anchor_end], query[0..anchor_end]);
    anchor_sql[anchor_end] = ';';

    // Build recursive SQL
    const recursive_len = query.len - recursive_start;
    const rlen = if (recursive_len > 0 and query[query.len - 1] == ';') recursive_len - 1 else recursive_len;
    var recursive_sql = try self.allocator.alloc(u8, rlen + 1);
    defer self.allocator.free(recursive_sql);
    @memcpy(recursive_sql[0..rlen], query[recursive_start .. recursive_start + rlen]);
    recursive_sql[rlen] = ';';

    // Execute anchor query
    var saved = self.saveProjectedState();
    const anchor_result = try self.execute(anchor_sql);

    var ncols: usize = 0;
    switch (anchor_result) {
        .rows => |rows| {
            ncols = if (rows.len > 0) rows[0].values.len else if (cte.col_names) |cn| cn.len else 0;
            try createCTETable(self, cte.name, rows, cte.col_names);
            try cte_names.append(self.allocator, try dupeStr(self.allocator, cte.name));
        },
        .err => {
            self.freeProjected();
            self.restoreProjectedState(saved);
            return;
        },
        .ok => {},
    }

    self.freeProjected();
    self.restoreProjectedState(saved);

    // We need a "working" table (current CTE table has anchor rows) and an "accumulator"
    // Strategy: before each recursive iteration, replace the CTE table contents with only
    // the most recently added rows, execute recursive query, collect new rows,
    // add them to both the CTE table (for next iteration) and track all results.

    // Collect all accumulated rows (start with anchor)
    var all_rows: std.ArrayList([][]const u8) = .{};
    defer {
        for (all_rows.items) |row_text| {
            for (row_text) |tv| self.allocator.free(tv);
            self.allocator.free(row_text);
        }
        all_rows.deinit(self.allocator);
    }

    // Copy anchor rows from CTE table
    if (self.tables.getPtr(cte.name)) |table| {
        for (table.storage().scan()) |row| {
            var text_values = try self.allocator.alloc([]const u8, ncols);
            for (0..ncols) |i| {
                if (i < row.values.len) {
                    text_values[i] = self.valueToText(row.values[i]);
                } else {
                    text_values[i] = try dupeStr(self.allocator, "NULL");
                }
            }
            all_rows.append(self.allocator, text_values) catch {};
        }
    }

    const max_iterations: usize = 1000;
    var iteration: usize = 0;
    while (iteration < max_iterations) : (iteration += 1) {
        // The CTE table currently holds only the "working set" (rows from previous iteration)
        saved = self.saveProjectedState();
        const rec_result = try self.execute(recursive_sql);

        var new_text_rows: std.ArrayList([][]const u8) = .{};
        defer {
            for (new_text_rows.items) |row_text| {
                for (row_text) |tv| self.allocator.free(tv);
                self.allocator.free(row_text);
            }
            new_text_rows.deinit(self.allocator);
        }

        switch (rec_result) {
            .rows => |rows| {
                for (rows) |row| {
                    var text_values = try self.allocator.alloc([]const u8, ncols);
                    for (0..ncols) |i| {
                        if (i < row.values.len) {
                            text_values[i] = self.valueToText(row.values[i]);
                        } else {
                            text_values[i] = try dupeStr(self.allocator, "NULL");
                        }
                    }
                    new_text_rows.append(self.allocator, text_values) catch {};
                }
            },
            .err => {},
            .ok => {},
        }

        self.freeProjected();
        self.restoreProjectedState(saved);

        if (new_text_rows.items.len == 0) break;

        // Replace CTE table with only the new rows (working set for next iteration)
        if (self.tables.fetchRemove(cte.name)) |kv| {
            var tbl = kv.value;
            tbl.deinit();
        }

        // Recreate CTE table with ONLY new rows
        var columns = try self.allocator.alloc(Column, ncols);
        for (0..ncols) |i| {
            const col_name = if (cte.col_names) |cn| blk: {
                if (i < cn.len) break :blk try dupeStr(self.allocator, cn[i]);
                break :blk try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
            } else try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
            columns[i] = .{ .name = col_name, .col_type = "TEXT", .is_primary_key = false };
        }
        const tname = try dupeStr(self.allocator, cte.name);
        var new_table = Table.init(self.allocator, tname, columns);
        for (new_text_rows.items) |row_text| {
            try new_table.insertRow(row_text);
        }
        try self.tables.put(tname, new_table);

        // Add to accumulated rows
        for (new_text_rows.items) |row_text| {
            var dup = try self.allocator.alloc([]const u8, ncols);
            for (0..ncols) |i| {
                dup[i] = try dupeStr(self.allocator, row_text[i]);
            }
            all_rows.append(self.allocator, dup) catch {};
        }
    }

    // Finally, replace CTE table with ALL accumulated rows for the main query
    if (self.tables.fetchRemove(cte.name)) |kv| {
        var tbl = kv.value;
        tbl.deinit();
    }
    var final_columns = try self.allocator.alloc(Column, ncols);
    for (0..ncols) |i| {
        const col_name = if (cte.col_names) |cn| blk: {
            if (i < cn.len) break :blk try dupeStr(self.allocator, cn[i]);
            break :blk try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
        } else try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
        final_columns[i] = .{ .name = col_name, .col_type = "TEXT", .is_primary_key = false };
    }
    const final_tname = try dupeStr(self.allocator, cte.name);
    var final_table = Table.init(self.allocator, final_tname, final_columns);
    for (all_rows.items) |row_text| {
        try final_table.insertRow(row_text);
    }
    try self.tables.put(final_tname, final_table);
}

pub fn createCTETable(self: *Database, name: []const u8, rows: []const Row, explicit_col_names: ?[]const []const u8) !void {
    const ncols = if (rows.len > 0) rows[0].values.len else if (explicit_col_names) |cn| cn.len else if (self.projected_column_names) |cn| cn.len else 0;
    var columns = try self.allocator.alloc(Column, ncols);
    for (0..ncols) |i| {
        const col_name = if (explicit_col_names) |cn| blk: {
            if (i < cn.len) break :blk try dupeStr(self.allocator, cn[i]);
            break :blk try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
        } else if (self.projected_column_names) |cn| blk: {
            if (i < cn.len) break :blk try dupeStr(self.allocator, cn[i]);
            break :blk try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
        } else try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
        columns[i] = .{
            .name = col_name,
            .col_type = "TEXT",
            .is_primary_key = false,
        };
    }
    const tname = try dupeStr(self.allocator, name);
    var table = Table.init(self.allocator, tname, columns);
    for (rows) |row| {
        var text_values = try self.allocator.alloc([]const u8, ncols);
        defer self.allocator.free(text_values);
        for (row.values, 0..) |val, i| {
            text_values[i] = self.valueToText(val);
        }
        try table.insertRow(text_values);
        for (text_values) |tv| self.allocator.free(tv);
    }
    try self.tables.put(tname, table);
}

pub fn findUnionAllSplit(sql: []const u8) ?[2]usize {
    // Find "UNION ALL" in the SQL (case-insensitive), return (anchor_end, recursive_start)
    // Must handle nested parentheses to avoid matching inside subqueries
    var depth: usize = 0;
    var i: usize = 0;
    while (i + 9 <= sql.len) : (i += 1) {
        if (sql[i] == '(') {
            depth += 1;
        } else if (sql[i] == ')') {
            if (depth > 0) depth -= 1;
        } else if (depth == 0) {
            if (std.ascii.eqlIgnoreCase(sql[i .. i + 5], "UNION")) {
                // Check for ALL after UNION (skipping whitespace)
                var j = i + 5;
                while (j < sql.len and std.ascii.isWhitespace(sql[j])) j += 1;
                if (j + 3 <= sql.len and std.ascii.eqlIgnoreCase(sql[j .. j + 3], "ALL")) {
                    var k = j + 3;
                    while (k < sql.len and std.ascii.isWhitespace(sql[k])) k += 1;
                    return .{ i, k };
                }
            }
        }
    }
    return null;
}

/// Execute CREATE TABLE ... AS SELECT
pub fn executeCreateTableAsSelect(self: *Database, table_name: []const u8, select_sql: []const u8) !ExecuteResult {
    // Execute the SELECT query
    const result = try self.execute(select_sql);
    switch (result) {
        .rows => |rows| {
            // Get column names from projected_column_names or use generic names
            const ncols = if (rows.len > 0) rows[0].values.len else if (self.projected_column_names) |cn| cn.len else 0;
            var columns = try self.allocator.alloc(Column, ncols);
            for (0..ncols) |i| {
                const col_name = if (self.projected_column_names) |cn| blk: {
                    if (i < cn.len) break :blk try dupeStr(self.allocator, cn[i]);
                    break :blk try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
                } else try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
                columns[i] = .{
                    .name = col_name,
                    .col_type = "TEXT",
                    .is_primary_key = false,
                };
            }
            const tname = try dupeStr(self.allocator, table_name);
            var table = Table.init(self.allocator, tname, columns);
            // Insert all rows
            for (rows) |row| {
                var text_values = try self.allocator.alloc([]const u8, ncols);
                defer self.allocator.free(text_values);
                for (row.values, 0..) |val, i| {
                    text_values[i] = self.valueToText(val);
                }
                try table.insertRow(text_values);
                for (text_values) |tv| self.allocator.free(tv);
            }
            try self.tables.put(tname, table);
            return .ok;
        },
        else => return .{ .err = "CREATE TABLE AS SELECT failed" },
    }
}
