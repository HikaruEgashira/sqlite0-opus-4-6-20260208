const std = @import("std");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");
const parser = @import("parser");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Column = value_mod.Column;
const Table = table_mod.Table;
const Expr = parser.Expr;
const SelectExpr = parser.SelectExpr;
const AggFunc = parser.AggFunc;
const OrderByItem = parser.OrderByItem;
const JoinClause = parser.JoinClause;
const JoinType = parser.JoinType;
const dupeStr = value_mod.dupeStr;
const rowsEqual = value_mod.rowsEqual;
const compareValuesOrder = value_mod.compareValuesOrder;
const convert = @import("convert.zig");

const root = @import("root.zig");
const Database = root.Database;
const ExecuteResult = root.ExecuteResult;
const Statement = parser.Statement;
const AliasOffset = root.AliasOffset;

pub fn executeJoin(self: *Database, sel: Statement.Select) !ExecuteResult {
    const first_table = self.tables.getPtr(sel.table_name) orelse return .{ .err = "table not found" };

    // Build combined columns and rows iteratively through each JOIN
    var combined_cols_list: std.ArrayList(Column) = .{};
    defer combined_cols_list.deinit(self.allocator);
    for (first_table.columns) |col| {
        try combined_cols_list.append(self.allocator, col);
    }

    // Start with first table rows
    var joined_rows: std.ArrayList(Row) = .{};
    var joined_values: std.ArrayList([]Value) = .{};
    for (first_table.storage().scan()) |row| {
        const values = try self.allocator.alloc(Value, row.values.len);
        @memcpy(values, row.values);
        try joined_values.append(self.allocator, values);
        try joined_rows.append(self.allocator, .{ .values = values });
    }

    // Track alias mapping: alias/name -> column offset in combined row
    var alias_offsets: std.ArrayList(AliasOffset) = .{};
    defer alias_offsets.deinit(self.allocator);
    try alias_offsets.append(self.allocator, .{
        .name = sel.table_name,
        .alias = sel.table_alias orelse sel.table_name,
        .offset = 0,
        .table = first_table,
    });

    // Process each JOIN
    for (sel.joins) |join| {
        const right_table = self.tables.getPtr(join.table_name) orelse return .{ .err = "table not found" };
        const left_col_count = combined_cols_list.items.len;
        const right_col_count = right_table.columns.len;
        const total_cols = left_col_count + right_col_count;

        // Track this table's offset
        try alias_offsets.append(self.allocator, .{
            .name = join.table_name,
            .alias = join.table_alias orelse join.table_name,
            .offset = left_col_count,
            .table = right_table,
        });

        var new_rows: std.ArrayList(Row) = .{};
        var new_values: std.ArrayList([]Value) = .{};
        const right_rows = right_table.storage().scan();

        if (join.join_type == .cross) {
            for (joined_rows.items) |left_row| {
                for (right_rows) |right_row| {
                    var values = try self.allocator.alloc(Value, total_cols);
                    @memcpy(values[0..left_col_count], left_row.values[0..left_col_count]);
                    @memcpy(values[left_col_count..total_cols], right_row.values[0..right_col_count]);
                    try new_values.append(self.allocator, values);
                    try new_rows.append(self.allocator, .{ .values = values });
                }
            }
        } else {
            // Resolve join columns
            var left_jc: ?usize = null;
            var right_jc: ?usize = null;

            // NATURAL JOIN: find first matching column name
            if (join.natural) {
                for (combined_cols_list.items, 0..) |left_col, ci| {
                    if (right_table.findColumnIndex(left_col.name)) |ri| {
                        left_jc = ci;
                        right_jc = ri;
                        break;
                    }
                }
            }

            // Find which alias the left_table reference in ON refers to
            if (left_jc == null) {
                for (alias_offsets.items) |ao| {
                    if (std.mem.eql(u8, join.left_table, ao.alias) or std.mem.eql(u8, join.left_table, ao.name)) {
                        if (ao.table.findColumnIndex(join.left_column)) |ci| {
                            left_jc = ao.offset + ci;
                        }
                        right_jc = right_table.findColumnIndex(join.right_column);
                        break;
                    }
                    if (std.mem.eql(u8, join.right_table, ao.alias) or std.mem.eql(u8, join.right_table, ao.name)) {
                        if (ao.table.findColumnIndex(join.right_column)) |ci| {
                            left_jc = ao.offset + ci;
                        }
                        right_jc = right_table.findColumnIndex(join.left_column);
                        break;
                    }
                }
            }

            // Fallback: try column names directly
            if (left_jc == null) {
                for (combined_cols_list.items, 0..) |col, ci| {
                    if (std.mem.eql(u8, col.name, join.left_column)) { left_jc = ci; break; }
                }
                right_jc = right_table.findColumnIndex(join.right_column);
            }

            const ljc = left_jc orelse return .{ .err = "join column not found" };
            const rjc = right_jc orelse return .{ .err = "join column not found" };

            for (joined_rows.items) |left_row| {
                var matched = false;
                for (right_rows) |right_row| {
                    if (compareValuesOrder(left_row.values[ljc], right_row.values[rjc]) == .eq) {
                        matched = true;
                        var values = try self.allocator.alloc(Value, total_cols);
                        @memcpy(values[0..left_col_count], left_row.values[0..left_col_count]);
                        @memcpy(values[left_col_count..total_cols], right_row.values[0..right_col_count]);
                        try new_values.append(self.allocator, values);
                        try new_rows.append(self.allocator, .{ .values = values });
                    }
                }
                if (!matched and (join.join_type == .left or join.join_type == .full)) {
                    var values = try self.allocator.alloc(Value, total_cols);
                    @memcpy(values[0..left_col_count], left_row.values[0..left_col_count]);
                    for (left_col_count..total_cols) |ci| values[ci] = .{ .null_val = {} };
                    try new_values.append(self.allocator, values);
                    try new_rows.append(self.allocator, .{ .values = values });
                }
            }
            // RIGHT JOIN: also add right rows that didn't match any left row
            if (join.join_type == .right or join.join_type == .full) {
                for (right_rows) |right_row| {
                    var matched = false;
                    for (joined_rows.items) |left_row| {
                        if (compareValuesOrder(left_row.values[ljc], right_row.values[rjc]) == .eq) {
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) {
                        var values = try self.allocator.alloc(Value, total_cols);
                        for (0..left_col_count) |ci| values[ci] = .{ .null_val = {} };
                        @memcpy(values[left_col_count..total_cols], right_row.values[0..right_col_count]);
                        try new_values.append(self.allocator, values);
                        try new_rows.append(self.allocator, .{ .values = values });
                    }
                }
            }
        }

        // Free old joined values and replace with new
        for (joined_values.items) |v| self.allocator.free(v);
        joined_values.deinit(self.allocator);
        joined_rows.deinit(self.allocator);
        joined_rows = new_rows;
        joined_values = new_values;

        // Add right table columns to combined list
        for (right_table.columns) |col| {
            try combined_cols_list.append(self.allocator, col);
        }
    }

    const total_cols = combined_cols_list.items.len;

    // Apply WHERE filter using where_expr
    if (sel.where_expr) |we| {
        const combined_cols = try self.allocator.alloc(Column, total_cols);
        defer self.allocator.free(combined_cols);
        @memcpy(combined_cols, combined_cols_list.items);
        var tmp_table = Table.init(self.allocator, "joined", combined_cols);
        defer tmp_table.row_storage.storage().deinit(self.allocator);
        var i: usize = joined_rows.items.len;
        while (i > 0) {
            i -= 1;
            const val = try self.evalExpr(we, &tmp_table, joined_rows.items[i]);
            defer if (val == .text) self.allocator.free(val.text);
            if (!self.valueToBool(val)) {
                self.allocator.free(joined_values.items[i]);
                _ = joined_rows.orderedRemove(i);
                _ = joined_values.orderedRemove(i);
            }
        }
    }

    // Apply ORDER BY for JOIN
    if (sel.order_by) |order_by| {
        var ob_indices = try self.allocator.alloc(usize, order_by.items.len);
        defer self.allocator.free(ob_indices);
        var ob_descs = try self.allocator.alloc(bool, order_by.items.len);
        defer self.allocator.free(ob_descs);

        for (order_by.items, 0..) |item, idx| {
            ob_descs[idx] = item.order == .desc;
            var col_idx: ?usize = null;

            if (item.expr) |e| {
                switch (e.*) {
                    .integer_literal => |pos| {
                        // ORDER BY position refers to SELECT output column, resolve to combined column index
                        if (pos >= 1 and pos <= @as(i64, @intCast(sel.columns.len))) {
                            const sel_idx: usize = @intCast(pos - 1);
                            // Resolve the SELECT column to combined column index
                            if (sel_idx < sel.result_exprs.len) {
                                const re = sel.result_exprs[sel_idx];
                                if (re.* == .qualified_ref) {
                                    const qr2 = re.qualified_ref;
                                    for (alias_offsets.items) |ao| {
                                        if (std.mem.eql(u8, qr2.table, ao.alias) or std.mem.eql(u8, qr2.table, ao.name)) {
                                            if (ao.table.findColumnIndex(qr2.column)) |ci| col_idx = ao.offset + ci;
                                            break;
                                        }
                                    }
                                } else if (re.* == .column_ref) {
                                    for (combined_cols_list.items, 0..) |col, ci| {
                                        if (std.mem.eql(u8, col.name, re.column_ref)) { col_idx = ci; break; }
                                    }
                                }
                            }
                            // Fallback: check sel.columns directly
                            if (col_idx == null) {
                                const sc = sel.columns[sel_idx];
                                for (combined_cols_list.items, 0..) |col, ci| {
                                    if (std.mem.eql(u8, col.name, sc)) { col_idx = ci; break; }
                                }
                            }
                        }
                    },
                    .qualified_ref => |qr| {
                        for (alias_offsets.items) |ao| {
                            if (std.mem.eql(u8, qr.table, ao.alias) or std.mem.eql(u8, qr.table, ao.name)) {
                                if (ao.table.findColumnIndex(qr.column)) |ci| col_idx = ao.offset + ci;
                                break;
                            }
                        }
                    },
                    .column_ref => |name| {
                        for (combined_cols_list.items, 0..) |col, ci| {
                            if (std.mem.eql(u8, col.name, name)) { col_idx = ci; break; }
                        }
                    },
                    else => {},
                }
            } else if (item.column.len > 0) {
                for (combined_cols_list.items, 0..) |col, ci| {
                    if (std.mem.eql(u8, col.name, item.column)) { col_idx = ci; break; }
                }
            }
            ob_indices[idx] = col_idx orelse 0;
        }

        const JoinSortCtx = struct {
            indices: []const usize,
            descs: []const bool,
            fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                for (ctx.indices, ctx.descs) |ci, is_desc| {
                    const cmp = compareValuesOrder(a.values[ci], b.values[ci]);
                    if (cmp == .eq) continue;
                    if (is_desc) return cmp == .gt;
                    return cmp == .lt;
                }
                return false;
            }
        };
        std.mem.sort(Row, joined_rows.items, JoinSortCtx{ .indices = ob_indices, .descs = ob_descs }, JoinSortCtx.lessThan);
    }

    const result_rows = joined_rows.items;

    // Check for aggregates in JOIN results
    const has_agg = blk: {
        for (sel.select_exprs) |se| {
            switch (se) {
                .aggregate => break :blk true,
                .expr => |e| {
                    if (parser.Parser.exprAsAggregate(e) != null) break :blk true;
                },
                else => {},
            }
        }
        break :blk false;
    };
    if (has_agg or sel.group_by != null) {
        // Build a temporary combined table for aggregate evaluation
        const combined_cols = try self.allocator.alloc(Column, combined_cols_list.items.len);
        @memcpy(combined_cols, combined_cols_list.items);
        var tmp_table = Table.init(self.allocator, "joined", combined_cols);
        // Store alias offsets for resolving qualified column references in aggregates
        self.join_alias_offsets = alias_offsets.items;
        defer self.join_alias_offsets = null;
        const agg_result = try self.executeAggregate(&tmp_table, sel, result_rows);
        tmp_table.row_storage.storage().deinit(self.allocator);
        self.allocator.free(combined_cols);
        for (joined_values.items) |v| self.allocator.free(v);
        joined_values.deinit(self.allocator);
        joined_rows.deinit(self.allocator);
        return agg_result;
    }

    // Check if any select_expr is a complex expression (not simple column)
    const has_expr = blk: {
        for (sel.select_exprs) |expr| {
            if (expr == .expr) break :blk true;
        }
        break :blk false;
    };

    // If expressions exist, use evaluateExprSelect with combined table
    if (has_expr) {
        const combined_cols = try self.allocator.alloc(Column, combined_cols_list.items.len);
        @memcpy(combined_cols, combined_cols_list.items);
        var tmp_table = Table.init(self.allocator, "joined", combined_cols);
        self.join_alias_offsets = alias_offsets.items;
        const expr_result = try self.evaluateExprSelect(sel, &tmp_table, result_rows);
        self.join_alias_offsets = null;
        tmp_table.row_storage.storage().deinit(self.allocator);
        self.allocator.free(combined_cols);
        for (joined_values.items) |v| self.allocator.free(v);
        joined_values.deinit(self.allocator);
        joined_rows.deinit(self.allocator);
        return expr_result;
    }

    // SELECT * for JOIN
    if (sel.columns.len == 0 and sel.select_exprs.len == 0) {
        // Apply DISTINCT
        if (sel.distinct) {
            var write_idx: usize = 0;
            for (joined_rows.items, 0..) |row, ri| {
                var is_dup = false;
                for (joined_rows.items[0..write_idx]) |prev| {
                    if (rowsEqual(row, prev)) {
                        is_dup = true;
                        break;
                    }
                }
                if (!is_dup) {
                    joined_rows.items[write_idx] = joined_rows.items[ri];
                    joined_values.items[write_idx] = joined_values.items[ri];
                    write_idx += 1;
                } else {
                    self.allocator.free(joined_values.items[ri]);
                }
            }
            joined_rows.shrinkRetainingCapacity(write_idx);
            joined_values.shrinkRetainingCapacity(write_idx);
        }
        // Apply LIMIT/OFFSET
        const total_j = joined_rows.items.len;
        var j_start: usize = 0;
        var j_end: usize = total_j;
        if (sel.offset) |off| {
            if (off > 0) j_start = @min(@as(usize, @intCast(off)), total_j);
        }
        if (sel.limit) |lim| {
            if (lim >= 0) j_end = @min(j_start + @as(usize, @intCast(lim)), total_j);
        }
        for (0..j_start) |i| self.allocator.free(joined_values.items[i]);
        for (j_end..total_j) |i| self.allocator.free(joined_values.items[i]);
        const lim_rows = joined_rows.items[j_start..j_end];
        const lim_vals = joined_values.items[j_start..j_end];
        const proj_rows = try self.allocator.alloc(Row, lim_rows.len);
        @memcpy(proj_rows, lim_rows);
        self.projected_rows = proj_rows;
        const pv = try self.allocator.alloc([]Value, lim_vals.len);
        @memcpy(pv, lim_vals);
        self.projected_values = pv;
        joined_rows.deinit(self.allocator);
        joined_values.deinit(self.allocator);
        return .{ .rows = proj_rows };
    }

    // Column projection for JOIN (with qualified_ref support)
    var col_indices = try self.allocator.alloc(usize, sel.columns.len);
    defer self.allocator.free(col_indices);
    for (sel.columns, 0..) |sel_col, i| {
        var found = false;

        // Check if this column has a qualified ref in result_exprs for disambiguation
        if (i < sel.result_exprs.len) {
            const expr = sel.result_exprs[i];
            if (expr.* == .qualified_ref) {
                const qr = expr.qualified_ref;
                for (alias_offsets.items) |ao| {
                    if (std.mem.eql(u8, qr.table, ao.alias) or std.mem.eql(u8, qr.table, ao.name)) {
                        if (ao.table.findColumnIndex(qr.column)) |ci| {
                            col_indices[i] = ao.offset + ci;
                            found = true;
                        }
                        break;
                    }
                }
            }
        }

        if (!found) {
            for (combined_cols_list.items, 0..) |tbl_col, j| {
                if (std.mem.eql(u8, sel_col, tbl_col.name)) {
                    col_indices[i] = j;
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            for (joined_values.items) |v| self.allocator.free(v);
            joined_values.deinit(self.allocator);
            joined_rows.deinit(self.allocator);
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

    for (joined_values.items) |v| self.allocator.free(v);
    joined_values.deinit(self.allocator);
    joined_rows.deinit(self.allocator);

    // Apply DISTINCT on projected rows
    if (sel.distinct and row_count > 0) {
        var write_idx: usize = 0;
        for (0..row_count) |ri| {
            var is_dup = false;
            for (0..write_idx) |pi| {
                if (rowsEqual(proj_rows[pi], proj_rows[ri])) {
                    is_dup = true;
                    break;
                }
            }
            if (!is_dup) {
                proj_rows[write_idx] = proj_rows[ri];
                proj_values[write_idx] = proj_values[ri];
                write_idx += 1;
            } else {
                self.allocator.free(proj_values[ri]);
            }
        }
        proj_rows = self.allocator.realloc(proj_rows, write_idx) catch proj_rows;
        proj_values = self.allocator.realloc(proj_values, write_idx) catch proj_values;
    }

    // Apply LIMIT/OFFSET on projected rows
    const total_proj = proj_rows.len;
    var p_start: usize = 0;
    var p_end: usize = total_proj;
    if (sel.offset) |off| {
        if (off > 0) p_start = @min(@as(usize, @intCast(off)), total_proj);
    }
    if (sel.limit) |lim| {
        if (lim >= 0) p_end = @min(p_start + @as(usize, @intCast(lim)), total_proj);
    }
    if (p_start > 0 or p_end < total_proj) {
        // Free trimmed rows
        for (0..p_start) |i| self.allocator.free(proj_values[i]);
        for (p_end..total_proj) |i| self.allocator.free(proj_values[i]);
        const kept_count = p_end - p_start;
        const new_proj_rows = try self.allocator.alloc(Row, kept_count);
        const new_proj_values = try self.allocator.alloc([]Value, kept_count);
        @memcpy(new_proj_rows, proj_rows[p_start..p_end]);
        @memcpy(new_proj_values, proj_values[p_start..p_end]);
        self.allocator.free(proj_rows);
        self.allocator.free(proj_values);
        proj_rows = new_proj_rows;
        proj_values = new_proj_values;
    }

    self.projected_rows = proj_rows;
    self.projected_values = proj_values;
    return .{ .rows = proj_rows };
}
