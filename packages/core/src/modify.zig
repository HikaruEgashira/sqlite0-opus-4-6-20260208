const std = @import("std");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");
const parser = @import("parser");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Column = value_mod.Column;
const Table = table_mod.Table;
const Expr = parser.Expr;
const dupeStr = value_mod.dupeStr;
const convert = @import("convert.zig");

const root = @import("root.zig");
const Database = root.Database;
const ExecuteResult = root.ExecuteResult;
const Statement = parser.Statement;

const tokenizer = @import("tokenizer");
const Tokenizer = tokenizer.Tokenizer;
const Parser = parser.Parser;

pub fn expandInsertValues(self: *Database, table: *Table, col_names: []const []const u8, values: []const []const u8) ![]const []const u8 {
    var full_values = try self.allocator.alloc([]const u8, table.columns.len);
    for (table.columns, 0..) |col, i| {
        // Check if this column is in the insert column list
        var found = false;
        for (col_names, 0..) |cn, j| {
            if (std.mem.eql(u8, cn, col.name)) {
                if (j < values.len) {
                    full_values[i] = values[j];
                } else {
                    full_values[i] = "NULL";
                }
                found = true;
                break;
            }
        }
        if (!found) {
            // Use default value or NULL
            full_values[i] = col.default_value orelse "NULL";
        }
    }
    return full_values;
}

/// Check if a row with matching PK already exists
pub fn hasPkConflict(self: *Database, table: *Table, raw_values: []const []const u8) bool {
    var pk_idx: ?usize = null;
    for (table.columns, 0..) |col, i| {
        if (col.is_primary_key) {
            pk_idx = i;
            break;
        }
    }
    if (pk_idx) |pidx| {
        if (pidx < raw_values.len) {
            const pk_val: Value = if (std.mem.eql(u8, raw_values[pidx], "NULL")) .null_val else Table.parseRawValue(raw_values[pidx]);
            for (table.storage().scan()) |row| {
                if (pidx < row.values.len) {
                    if (self.valuesEqualUnion(row.values[pidx], pk_val)) return true;
                }
            }
        }
    }
    return false;
}

/// Check if a row with matching UNIQUE column already exists
pub fn hasUniqueConflict(self: *Database, table: *Table, raw_values: []const []const u8) bool {
    for (table.columns, 0..) |col, i| {
        if (col.is_unique and i < raw_values.len) {
            const new_val: Value = if (std.mem.eql(u8, raw_values[i], "NULL")) .null_val else Table.parseRawValue(raw_values[i]);
            // NULL values don't conflict with each other in UNIQUE
            if (new_val == .null_val) continue;
            for (table.storage().scan()) |row| {
                if (i < row.values.len) {
                    if (self.valuesEqualUnion(row.values[i], new_val)) return true;
                }
            }
        }
    }
    return false;
}

/// Validate CHECK constraints for a row about to be inserted
pub fn validateCheckConstraints(self: *Database, table: *Table, raw_values: []const []const u8) !bool {
    // Build a temporary row for evaluation
    var values = try self.allocator.alloc(Value, raw_values.len);
    defer {
        for (values) |v| {
            if (v == .text) self.allocator.free(v.text);
        }
        self.allocator.free(values);
    }
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
    const temp_row = Row{ .values = values };

    for (table.columns) |col| {
        if (col.check_expr_sql) |check_sql| {
            // Wrap as SELECT <expr>; to parse as expression
            const select_sql = std.fmt.allocPrint(self.allocator, "SELECT {s};", .{check_sql}) catch return true;
            defer self.allocator.free(select_sql);
            var tok = Tokenizer.init(select_sql);
            const tokens = tok.tokenize(self.allocator) catch return true;
            defer self.allocator.free(tokens);
            var p = Parser.init(self.allocator, tokens);
            const stmt = p.parse() catch return true;
            switch (stmt) {
                .select_stmt => |sel| {
                    defer self.allocator.free(sel.columns);
                    defer self.allocator.free(sel.select_exprs);
                    defer self.allocator.free(sel.aliases);
                    defer {
                        for (sel.result_exprs) |e| self.freeExprDeep(e);
                        self.allocator.free(sel.result_exprs);
                    }
                    defer if (sel.where_expr) |we| self.freeWhereExpr(we);
                    defer if (sel.group_by) |gb| {
                        for (gb) |e| self.freeExprDeep(e);
                        self.allocator.free(gb);
                    };
                    defer if (sel.having_expr) |he| self.freeExprDeep(he);
                    defer if (sel.order_by) |ob| {
                        for (ob.items) |item| {
                            if (item.expr) |e| self.freeExprDeep(e);
                        }
                        self.allocator.free(ob.items);
                    };
                    if (sel.result_exprs.len > 0) {
                        const val = self.evalExpr(sel.result_exprs[0], table, temp_row) catch return true;
                        defer if (val == .text) self.allocator.free(val.text);
                        if (!self.valueToBool(val)) return false;
                    }
                },
                else => {},
            }
        }
    }
    return true;
}

/// REPLACE INTO: delete existing row with matching PK, then insert
pub fn replaceRow(self: *Database, table: *Table, raw_values: []const []const u8) void {
    // Find primary key column index
    var pk_idx: ?usize = null;
    for (table.columns, 0..) |col, i| {
        if (col.is_primary_key) {
            pk_idx = i;
            break;
        }
    }
    if (pk_idx) |pidx| {
        if (pidx < raw_values.len) {
            const pk_val: Value = if (std.mem.eql(u8, raw_values[pidx], "NULL")) .null_val else Table.parseRawValue(raw_values[pidx]);
            var i: usize = table.storage().len();
            while (i > 0) {
                i -= 1;
                const rows = table.storage().scan();
                if (pidx < rows[i].values.len) {
                    if (self.valuesEqualUnion(rows[i].values[pidx], pk_val)) {
                        table.freeRow(rows[i]);
                        _ = table.storage().orderedRemove(i);
                    }
                }
            }
        }
    }
    table.insertRow(raw_values) catch {};
}

/// Execute UPSERT: INSERT ... ON CONFLICT DO NOTHING/UPDATE
pub fn executeUpsertRow(self: *Database, table: *Table, raw_values: []const []const u8, on_conflict: Statement.OnConflict) !void {
    // Find conflict column indices (or use primary key)
    var conflict_indices: std.ArrayList(usize) = .{};
    defer conflict_indices.deinit(self.allocator);
    if (on_conflict.conflict_columns.len > 0) {
        for (on_conflict.conflict_columns) |cc| {
            for (table.columns, 0..) |col, i| {
                if (std.ascii.eqlIgnoreCase(cc, col.name)) {
                    try conflict_indices.append(self.allocator, i);
                    break;
                }
            }
        }
    } else {
        // Default: use primary key column
        for (table.columns, 0..) |col, i| {
            if (col.is_primary_key) {
                try conflict_indices.append(self.allocator, i);
                break;
            }
        }
    }
    // Parse new values for conflict comparison (not heap-allocated, no free needed)
    var new_vals = try self.allocator.alloc(Value, raw_values.len);
    defer self.allocator.free(new_vals);
    for (raw_values, 0..) |rv, i| {
        new_vals[i] = if (std.mem.eql(u8, rv, "NULL")) .null_val else Table.parseRawValue(rv);
    }
    // Find conflicting row
    var conflict_row_idx: ?usize = null;
    if (conflict_indices.items.len > 0) {
        const rows = table.storage().scan();
        for (rows, 0..) |row, ri| {
            var all_match = true;
            for (conflict_indices.items) |ci| {
                if (ci < row.values.len and ci < new_vals.len) {
                    if (!self.valuesEqualUnion(row.values[ci], new_vals[ci])) {
                        all_match = false;
                        break;
                    }
                } else {
                    all_match = false;
                    break;
                }
            }
            if (all_match) {
                conflict_row_idx = ri;
                break;
            }
        }
    }
    if (conflict_row_idx) |cri| {
        // Conflict found
        switch (on_conflict.action) {
            .do_nothing => {}, // Skip this row
            .do_update => {
                // Apply SET assignments
                const rows = table.storage().scan();
                var row = rows[cri];
                for (on_conflict.updates) |upd| {
                    // Find target column index
                    var target_idx: ?usize = null;
                    for (table.columns, 0..) |col, i| {
                        if (std.ascii.eqlIgnoreCase(upd.column, col.name)) {
                            target_idx = i;
                            break;
                        }
                    }
                    if (target_idx) |ti| {
                        // Resolve value: "excluded.col" means the new value
                        const new_val = if (std.mem.startsWith(u8, upd.value_sql, "excluded.")) blk: {
                            const excl_col = upd.value_sql["excluded.".len..];
                            for (table.columns, 0..) |col, i| {
                                if (std.ascii.eqlIgnoreCase(excl_col, col.name)) {
                                    if (i < new_vals.len) break :blk new_vals[i];
                                }
                            }
                            break :blk Value.null_val;
                        } else Table.parseRawValue(upd.value_sql);
                        // Free old text value
                        if (row.values[ti] == .text) self.allocator.free(row.values[ti].text);
                        // Set new value (deep copy text)
                        row.values[ti] = if (new_val == .text)
                            .{ .text = dupeStr(self.allocator, new_val.text) catch "" }
                        else
                            new_val;
                    }
                }
            },
        }
    } else {
        // No conflict: insert normally
        try table.insertRow(raw_values);
    }
}

pub fn buildReturningResult(self: *Database, table: *const Table, src_rows: []const Row, returning_cols: []const []const u8) !ExecuteResult {
    const is_star = returning_cols.len == 1 and std.mem.eql(u8, returning_cols[0], "*");
    const num_cols = if (is_star) table.columns.len else returning_cols.len;
    var proj_rows = try self.allocator.alloc(Row, src_rows.len);
    var proj_values = try self.allocator.alloc([]Value, src_rows.len);
    var alloc_texts: std.ArrayList([]const u8) = .{};
    for (src_rows, 0..) |row, ri| {
        var values = try self.allocator.alloc(Value, num_cols);
        if (is_star) {
            for (0..num_cols) |ci| {
                values[ci] = switch (row.values[ci]) {
                    .text => |t| blk: {
                        const d = dupeStr(self.allocator, t) catch break :blk Value.null_val;
                        alloc_texts.append(self.allocator, d) catch {};
                        break :blk Value{ .text = d };
                    },
                    else => row.values[ci],
                };
            }
        } else {
            for (returning_cols, 0..) |col_name, ci| {
                const col_idx = table.findColumnIndex(col_name) orelse {
                    values[ci] = .null_val;
                    continue;
                };
                values[ci] = switch (row.values[col_idx]) {
                    .text => |t| blk: {
                        const d = dupeStr(self.allocator, t) catch break :blk Value.null_val;
                        alloc_texts.append(self.allocator, d) catch {};
                        break :blk Value{ .text = d };
                    },
                    else => row.values[col_idx],
                };
            }
        }
        proj_values[ri] = values;
        proj_rows[ri] = .{ .values = values };
    }
    self.projected_rows = proj_rows;
    self.projected_values = proj_values;
    if (alloc_texts.items.len > 0) {
        self.projected_texts = alloc_texts.toOwnedSlice(self.allocator) catch null;
    } else {
        alloc_texts.deinit(self.allocator);
    }
    return .{ .rows = proj_rows };
}

pub fn executeInsertSelect(self: *Database, table: *Table, select_sql: []const u8) !ExecuteResult {
    // Save and restore projected state
    const saved_rows = self.projected_rows;
    const saved_values = self.projected_values;
    const saved_texts = self.projected_texts;
    const saved_col_names = self.projected_column_names;
    self.projected_rows = null;
    self.projected_values = null;
    self.projected_texts = null;
    self.projected_column_names = null;

    const result = try self.execute(select_sql);

    switch (result) {
        .rows => |rows| {
            for (rows) |row| {
                // Deep copy each row's values for insertion
                var new_values = try self.allocator.alloc(Value, row.values.len);
                for (row.values, 0..) |val, vi| {
                    new_values[vi] = switch (val) {
                        .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                        .integer => |n| .{ .integer = n },
                        .null_val => .null_val,
                    };
                }
                const rowid = table.next_rowid;
                table.next_rowid += 1;
                try table.storage().append(self.allocator, .{ .rowid = rowid, .values = new_values });
            }
        },
        .err => |e| {
            self.freeProjected();
            self.projected_rows = saved_rows;
            self.projected_values = saved_values;
            self.projected_texts = saved_texts;
            self.projected_column_names = saved_col_names;
            return .{ .err = e };
        },
        .ok => {},
    }

    self.freeProjected();
    self.projected_rows = saved_rows;
    self.projected_values = saved_values;
    self.projected_texts = saved_texts;
    self.projected_column_names = saved_col_names;
    return .ok;
}
