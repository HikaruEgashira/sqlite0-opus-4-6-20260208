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
const dupeStr = value_mod.dupeStr;
const compareValuesOrder = value_mod.compareValuesOrder;
const convert = @import("convert.zig");

const root = @import("root.zig");
const Database = root.Database;
const ExecuteResult = root.ExecuteResult;
const Statement = parser.Statement;

pub fn resolveAggColIdx(self: *const Database, tbl: *const Table, arg: []const u8) ?usize {
    if (tbl.findColumnIndex(arg)) |idx| return idx;
    // If arg is "alias.col", use join_alias_offsets to resolve
    if (std.mem.indexOf(u8, arg, ".")) |dot_pos| {
        const alias = arg[0..dot_pos];
        const col = arg[dot_pos + 1 ..];
        if (self.join_alias_offsets) |offsets| {
            for (offsets) |ao| {
                if (std.mem.eql(u8, ao.alias, alias) or std.mem.eql(u8, ao.name, alias)) {
                    if (ao.table.findColumnIndex(col)) |ci| {
                        return ao.offset + ci;
                    }
                }
            }
        }
        // Fallback: try just the column part
        return tbl.findColumnIndex(col);
    }
    return null;
}

pub fn computeAgg(self: *Database, func: AggFunc, arg: []const u8, tbl: *const Table, rows: []const Row, separator: []const u8, is_distinct: bool) !Value {
    if (func == .count) {
        if (std.mem.eql(u8, arg, "*")) {
            return .{ .integer = @intCast(rows.len) };
        }
        const col_idx = resolveAggColIdx(self, tbl, arg) orelse return error.UnexpectedToken;
        if (is_distinct) {
            // COUNT(DISTINCT col): count unique non-null values
            var seen_ints: std.ArrayList(i64) = .{};
            defer seen_ints.deinit(self.allocator);
            var seen_texts: std.ArrayList([]const u8) = .{};
            defer seen_texts.deinit(self.allocator);
            var cnt: i64 = 0;
            for (rows) |row| {
                const val = row.values[col_idx];
                if (val == .null_val) continue;
                if (val == .integer) {
                    var found = false;
                    for (seen_ints.items) |s| {
                        if (s == val.integer) { found = true; break; }
                    }
                    if (!found) {
                        seen_ints.append(self.allocator, val.integer) catch {};
                        cnt += 1;
                    }
                } else if (val == .text) {
                    var found = false;
                    for (seen_texts.items) |s| {
                        if (std.mem.eql(u8, s, val.text)) { found = true; break; }
                    }
                    if (!found) {
                        seen_texts.append(self.allocator, val.text) catch {};
                        cnt += 1;
                    }
                }
            }
            return .{ .integer = cnt };
        }
        var cnt: i64 = 0;
        for (rows) |row| {
            if (row.values[col_idx] != .null_val) cnt += 1;
        }
        return .{ .integer = cnt };
    }

    const col_idx = resolveAggColIdx(self, tbl, arg) orelse return error.UnexpectedToken;

    switch (func) {
        .sum => {
            var total_f: f64 = 0;
            var has_value = false;
            var has_float = false;
            if (is_distinct) {
                var seen_vals: std.ArrayList(f64) = .{};
                defer seen_vals.deinit(self.allocator);
                for (rows) |row| {
                    const val = row.values[col_idx];
                    if (val == .null_val) continue;
                    if (self.isFloatValue(val)) has_float = true;
                    if (self.valueToF64(val)) |fv| {
                        var found = false;
                        for (seen_vals.items) |s| {
                            if (s == fv) { found = true; break; }
                        }
                        if (!found) {
                            seen_vals.append(self.allocator, fv) catch {};
                            total_f += fv;
                            has_value = true;
                        }
                    }
                }
            } else {
                for (rows) |row| {
                    const val = row.values[col_idx];
                    if (val == .null_val) continue;
                    if (self.isFloatValue(val)) has_float = true;
                    if (self.valueToF64(val)) |fv| {
                        total_f += fv;
                        has_value = true;
                    }
                }
            }
            if (!has_value) return .null_val;
            if (has_float) {
                return self.formatFloat(total_f);
            }
            return .{ .integer = @intFromFloat(total_f) };
        },
        .avg => {
            var total: f64 = 0;
            var cnt: usize = 0;
            if (is_distinct) {
                var seen_vals: std.ArrayList(f64) = .{};
                defer seen_vals.deinit(self.allocator);
                for (rows) |row| {
                    const val = row.values[col_idx];
                    if (val == .null_val) continue;
                    if (self.valueToF64(val)) |fv| {
                        var found = false;
                        for (seen_vals.items) |s| {
                            if (s == fv) { found = true; break; }
                        }
                        if (!found) {
                            seen_vals.append(self.allocator, fv) catch {};
                            total += fv;
                            cnt += 1;
                        }
                    }
                }
            } else {
                for (rows) |row| {
                    const val = row.values[col_idx];
                    if (val == .null_val) continue;
                    if (self.valueToF64(val)) |fv| {
                        total += fv;
                        cnt += 1;
                    }
                }
            }
            if (cnt == 0) return .null_val;
            const avg = total / @as(f64, @floatFromInt(cnt));
            return self.formatFloat(avg);
        },
        .min => {
            if (rows.len == 0) return .null_val;
            var min_val: Value = .null_val;
            for (rows) |row| {
                const v = row.values[col_idx];
                if (v == .null_val) continue;
                if (min_val == .null_val or compareValuesOrder(v, min_val) == .lt) {
                    min_val = v;
                }
            }
            // Duplicate text to avoid freeing table-owned data
            if (min_val == .text) {
                return .{ .text = try dupeStr(self.allocator, min_val.text) };
            }
            return min_val;
        },
        .max => {
            if (rows.len == 0) return .null_val;
            var max_val: Value = .null_val;
            for (rows) |row| {
                const v = row.values[col_idx];
                if (v == .null_val) continue;
                if (max_val == .null_val or compareValuesOrder(v, max_val) == .gt) {
                    max_val = v;
                }
            }
            // Duplicate text to avoid freeing table-owned data
            if (max_val == .text) {
                return .{ .text = try dupeStr(self.allocator, max_val.text) };
            }
            return max_val;
        },
        .group_concat => {
            var result: std.ArrayList(u8) = .{};
            var seen_texts: std.ArrayList([]const u8) = .{};
            defer seen_texts.deinit(self.allocator);
            var first = true;
            for (rows) |row| {
                if (row.values[col_idx] == .null_val) continue;
                const text = self.valueToText(row.values[col_idx]);
                if (is_distinct) {
                    var found = false;
                    for (seen_texts.items) |s| {
                        if (std.mem.eql(u8, s, text)) { found = true; break; }
                    }
                    if (found) {
                        self.allocator.free(text);
                        continue;
                    }
                    seen_texts.append(self.allocator, text) catch {};
                }
                if (!first) {
                    result.appendSlice(self.allocator, separator) catch return .null_val;
                }
                result.appendSlice(self.allocator, text) catch {
                    self.allocator.free(text);
                    return .null_val;
                };
                if (!is_distinct) self.allocator.free(text);
                first = false;
            }
            if (is_distinct) {
                for (seen_texts.items) |s| self.allocator.free(s);
            }
            if (first) return .null_val; // all NULL
            return .{ .text = result.toOwnedSlice(self.allocator) catch return .null_val };
        },
        .total => {
            var sum: f64 = 0;
            for (rows) |row| {
                const val = row.values[col_idx];
                if (val == .null_val) continue;
                if (self.valueToF64(val)) |fv| {
                    sum += fv;
                }
            }
            // TOTAL always returns real, even for empty set (0.0)
            return self.formatFloat(sum);
        },
        else => return .null_val,
    }
}

pub fn collectAggTexts(self: *Database, sel_exprs: []const SelectExpr, all_values: []const []Value) !void {
    var texts: std.ArrayList([]const u8) = .{};
    for (all_values) |values| {
        for (sel_exprs, 0..) |expr, i| {
            // Track text values from aggregate or expr expressions
            const is_computed = switch (expr) {
                .aggregate => true,
                .expr => true,
                .column => false,
            };
            if (is_computed and values[i] == .text) {
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

pub fn executeAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row) !ExecuteResult {
    if (sel.group_by) |gb_cols| {
        return executeGroupByAggregate(self, tbl, sel, rows, gb_cols);
    }

    var values = try self.allocator.alloc(Value, sel.select_exprs.len);
    for (sel.select_exprs, 0..) |expr, i| {
        switch (expr) {
            .aggregate => |agg| {
                values[i] = try computeAgg(self, agg.func, agg.arg, tbl, rows, agg.separator, agg.distinct);
            },
            .column => |col_name| {
                const col_idx = tbl.findColumnIndex(col_name) orelse return .{ .err = "column not found" };
                if (rows.len > 0) {
                    values[i] = rows[0].values[col_idx];
                } else {
                    values[i] = .null_val;
                }
            },
            .expr => |e| {
                if (parser.Parser.exprAsAggregate(e)) |agg| {
                    values[i] = try computeAgg(self, agg.func, agg.arg, tbl, rows, agg.separator, agg.distinct);
                } else if (rows.len > 0) {
                    values[i] = try self.evalExpr(e, tbl, rows[0]);
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
    try collectAggTexts(self, sel.select_exprs, proj_values);
    return .{ .rows = proj_rows };
}

pub fn executeGroupByAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row, gb_exprs: []const *const Expr) !ExecuteResult {
    // Group rows by evaluating GROUP BY expressions
    var group_keys: std.ArrayList([]Value) = .{};
    defer {
        for (group_keys.items) |gk| {
            for (gk) |v| {
                if (v == .text) self.allocator.free(v.text);
            }
            self.allocator.free(gk);
        }
        group_keys.deinit(self.allocator);
    }
    var group_rows: std.ArrayList(std.ArrayList(Row)) = .{};
    defer {
        for (group_rows.items) |*gr| {
            gr.deinit(self.allocator);
        }
        group_rows.deinit(self.allocator);
    }

    for (rows) |row| {
        // Evaluate GROUP BY expressions for this row
        var row_key = try self.allocator.alloc(Value, gb_exprs.len);
        for (gb_exprs, 0..) |expr, ki| {
            // Handle GROUP BY column position (e.g., GROUP BY 1, 2)
            if (expr.* == .integer_literal) {
                const pos = expr.integer_literal;
                if (pos >= 1 and pos <= @as(i64, @intCast(sel.select_exprs.len))) {
                    const sel_idx: usize = @intCast(pos - 1);
                    // Resolve SELECT list column to table column
                    switch (sel.select_exprs[sel_idx]) {
                        .column => |col_name| {
                            if (tbl.findColumnIndex(col_name)) |ci| {
                                row_key[ki] = row.values[ci];
                                if (row_key[ki] == .text) {
                                    row_key[ki] = .{ .text = try dupeStr(self.allocator, row_key[ki].text) };
                                }
                            } else {
                                row_key[ki] = .null_val;
                            }
                        },
                        .expr => |e| {
                            row_key[ki] = try self.evalExpr(e, tbl, row);
                        },
                        else => {
                            row_key[ki] = .null_val;
                        },
                    }
                } else {
                    row_key[ki] = .null_val;
                }
            } else {
                row_key[ki] = try self.evalExpr(expr, tbl, row);
            }
        }

        var found_idx: ?usize = null;
        for (group_keys.items, 0..) |gk, gi| {
            var all_eq = true;
            for (0..gb_exprs.len) |ki| {
                if (compareValuesOrder(row_key[ki], gk[ki]) != .eq) {
                    all_eq = false;
                    break;
                }
            }
            if (all_eq) {
                found_idx = gi;
                break;
            }
        }
        if (found_idx) |idx| {
            // Free the duplicate key
            for (row_key) |v| {
                if (v == .text) self.allocator.free(v.text);
            }
            self.allocator.free(row_key);
            try group_rows.items[idx].append(self.allocator, row);
        } else {
            try group_keys.append(self.allocator, row_key);
            var new_list: std.ArrayList(Row) = .{};
            try new_list.append(self.allocator, row);
            try group_rows.append(self.allocator, new_list);
        }
    }

    const n_groups = group_keys.items.len;
    var proj_values_list: std.ArrayList([]Value) = .{};
    var proj_rows_list: std.ArrayList(Row) = .{};

    for (0..n_groups) |gi| {
        const grp = group_rows.items[gi].items;

        // Apply HAVING filter (expr-based) before projection
        if (sel.having_expr) |he| {
            const hval = try self.evalGroupExpr(he, tbl, grp);
            defer if (hval == .text) self.allocator.free(hval.text);
            if (!self.valueToBool(hval)) continue;
        }

        const values = try self.allocator.alloc(Value, sel.select_exprs.len);
        for (sel.select_exprs, 0..) |expr, ei| {
            switch (expr) {
                .aggregate => |agg| {
                    values[ei] = try computeAgg(self, agg.func, agg.arg, tbl, grp, agg.separator, agg.distinct);
                },
                .column => |col_name| {
                    const col_idx = tbl.findColumnIndex(col_name) orelse return .{ .err = "column not found" };
                    values[ei] = grp[0].values[col_idx];
                },
                .expr => |e| {
                    if (grp.len > 0) {
                        values[ei] = try self.evalGroupExpr(e, tbl, grp);
                    } else {
                        values[ei] = .null_val;
                    }
                },
            }
        }
        try proj_values_list.append(self.allocator, values);
        try proj_rows_list.append(self.allocator, .{ .values = values });
    }

    const proj_values = try proj_values_list.toOwnedSlice(self.allocator);
    const proj_rows = try proj_rows_list.toOwnedSlice(self.allocator);

    // Sort groups by group key columns to match sqlite3 output order
    // Find the indices of GROUP BY expressions in select_exprs
    var gb_sort_indices: std.ArrayList(usize) = .{};
    defer gb_sort_indices.deinit(self.allocator);
    for (gb_exprs) |gb_expr| {
        // Handle GROUP BY column position (e.g., GROUP BY 1)
        if (gb_expr.* == .integer_literal) {
            const pos = gb_expr.integer_literal;
            if (pos >= 1 and pos <= @as(i64, @intCast(sel.select_exprs.len))) {
                gb_sort_indices.append(self.allocator, @intCast(pos - 1)) catch {};
            }
            continue;
        }
        // Extract column name from simple column_ref expression
        const gb_col_name: ?[]const u8 = switch (gb_expr.*) {
            .column_ref => |name| name,
            else => null,
        };
        if (gb_col_name) |gb_col| {
            for (sel.select_exprs, 0..) |expr, ei| {
                switch (expr) {
                    .column => |col_name| {
                        if (std.mem.eql(u8, col_name, gb_col)) {
                            gb_sort_indices.append(self.allocator, ei) catch break;
                            break;
                        }
                    },
                    else => {},
                }
            }
        }
    }
    if (gb_sort_indices.items.len > 0) {
        const MultiGBSortCtx = struct {
            indices: []const usize,
            fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                for (ctx.indices) |idx| {
                    const cmp = compareValuesOrder(a.values[idx], b.values[idx]);
                    if (cmp == .eq) continue;
                    return cmp == .lt;
                }
                return false;
            }
        };
        const ctx = MultiGBSortCtx{ .indices = gb_sort_indices.items };
        std.mem.sort(Row, proj_rows, ctx, MultiGBSortCtx.lessThan);
        std.mem.sort([]Value, proj_values, ctx, struct {
            fn lessThan(c: MultiGBSortCtx, a: []Value, b: []Value) bool {
                for (c.indices) |idx| {
                    const cmp = compareValuesOrder(a[idx], b[idx]);
                    if (cmp == .eq) continue;
                    return cmp == .lt;
                }
                return false;
            }
        }.lessThan);
    }

    // HAVING is now applied during group projection (above)

    // Apply ORDER BY (resolve aliases to projected column indices)
    if (sel.order_by) |order_by| {
        var ob_indices = try self.allocator.alloc(usize, order_by.items.len);
        defer self.allocator.free(ob_indices);
        var ob_descs = try self.allocator.alloc(bool, order_by.items.len);
        defer self.allocator.free(ob_descs);
        var all_resolved = true;
        const n_proj_cols = sel.select_exprs.len;
        for (order_by.items, 0..) |item, oi| {
            ob_descs[oi] = item.order == .desc;
            var col_idx: ?usize = null;
            // Check for ORDER BY column position (e.g., ORDER BY 2)
            if (item.expr) |e| {
                if (e.* == .integer_literal) {
                    const pos = e.integer_literal;
                    if (pos >= 1 and pos <= @as(i64, @intCast(n_proj_cols))) {
                        col_idx = @intCast(pos - 1);
                    }
                } else if (e.* == .qualified_ref) {
                    // Resolve qualified_ref against projected columns
                    const qr = e.qualified_ref;
                    for (sel.result_exprs, 0..) |re, ei| {
                        if (re.* == .qualified_ref) {
                            const re_qr = re.qualified_ref;
                            if (std.mem.eql(u8, qr.table, re_qr.table) and std.mem.eql(u8, qr.column, re_qr.column)) {
                                col_idx = ei;
                                break;
                            }
                        } else if (re.* == .column_ref) {
                            if (std.mem.eql(u8, qr.column, re.column_ref)) {
                                col_idx = ei;
                                break;
                            }
                        }
                    }
                    // Also check aliases
                    if (col_idx == null) {
                        for (sel.aliases, 0..) |alias, ai| {
                            if (alias) |a| {
                                if (std.ascii.eqlIgnoreCase(qr.column, a)) {
                                    col_idx = ai;
                                    break;
                                }
                            }
                        }
                    }
                    // Also check select_exprs column names
                    if (col_idx == null) {
                        for (sel.select_exprs, 0..) |sexpr, ei| {
                            switch (sexpr) {
                                .column => |col_name| {
                                    if (std.mem.eql(u8, col_name, qr.column)) {
                                        col_idx = ei;
                                        break;
                                    }
                                },
                                else => {},
                            }
                        }
                    }
                } else if (e.* == .column_ref) {
                    // Simple column ref in expr — resolve like alias
                    const name = e.column_ref;
                    for (sel.aliases, 0..) |alias, ai| {
                        if (alias) |a| {
                            if (std.ascii.eqlIgnoreCase(name, a)) {
                                col_idx = ai;
                                break;
                            }
                        }
                    }
                    if (col_idx == null) {
                        for (sel.select_exprs, 0..) |sexpr, ei| {
                            switch (sexpr) {
                                .column => |col_name| {
                                    if (std.mem.eql(u8, col_name, name)) {
                                        col_idx = ei;
                                        break;
                                    }
                                },
                                else => {},
                            }
                        }
                    }
                }
            }
            if (col_idx == null and item.column.len > 0) {
                // Check aliases first
                for (sel.aliases, 0..) |alias, ai| {
                    if (alias) |a| {
                        if (std.ascii.eqlIgnoreCase(item.column, a)) {
                            col_idx = ai;
                            break;
                        }
                    }
                }
                // Fallback: check select_exprs column names
                if (col_idx == null) {
                    for (sel.select_exprs, 0..) |expr, ei| {
                        switch (expr) {
                            .column => |col_name| {
                                if (std.mem.eql(u8, col_name, item.column)) {
                                    col_idx = ei;
                                    break;
                                }
                            },
                            else => {},
                        }
                    }
                }
            }
            if (col_idx) |ci| {
                ob_indices[oi] = ci;
            } else {
                all_resolved = false;
                break;
            }
        }
        if (all_resolved) {
            const AggSortCtx = struct {
                indices: []const usize,
                descs: []const bool,
                fn lessThan(ctx2: @This(), a: Row, b: Row) bool {
                    for (ctx2.indices, ctx2.descs) |ci, is_desc| {
                        const cmp = compareValuesOrder(a.values[ci], b.values[ci]);
                        if (cmp == .eq) continue;
                        if (is_desc) return cmp == .gt;
                        return cmp == .lt;
                    }
                    return false;
                }
            };
            std.mem.sort(Row, proj_rows, AggSortCtx{ .indices = ob_indices, .descs = ob_descs }, AggSortCtx.lessThan);
        } else if (proj_rows.len > 0) {
            // Fallback: evaluate ORDER BY expressions per group using evalGroupExpr
            const n_items = order_by.items.len;
            var sort_keys = try self.allocator.alloc([]Value, proj_rows.len);
            defer {
                for (sort_keys) |keys| {
                    for (keys) |v| if (v == .text) self.allocator.free(v.text);
                    self.allocator.free(keys);
                }
                self.allocator.free(sort_keys);
            }
            for (0..proj_rows.len) |gi| {
                var keys = try self.allocator.alloc(Value, n_items);
                for (order_by.items, 0..) |item, ki| {
                    if (item.expr) |e| {
                        keys[ki] = try self.evalGroupExpr(e, tbl, group_rows.items[gi].items);
                    } else {
                        keys[ki] = .null_val;
                    }
                }
                sort_keys[gi] = keys;
            }
            var ob_desc_flags = try self.allocator.alloc(bool, n_items);
            defer self.allocator.free(ob_desc_flags);
            for (order_by.items, 0..) |item, i| ob_desc_flags[i] = item.order == .desc;

            const indices = try self.allocator.alloc(usize, proj_rows.len);
            defer self.allocator.free(indices);
            for (indices, 0..) |*idx, ii| idx.* = ii;
            const ExprAggSortCtx = struct {
                keys: []const []Value,
                descs: []const bool,
                fn lessThan(ctx2: @This(), a: usize, b: usize) bool {
                    for (ctx2.keys[a], ctx2.keys[b], ctx2.descs) |va, vb, is_desc| {
                        const cmp = compareValuesOrder(va, vb);
                        if (cmp == .eq) continue;
                        if (is_desc) return cmp == .gt;
                        return cmp == .lt;
                    }
                    return false;
                }
            };
            std.mem.sort(usize, indices, ExprAggSortCtx{ .keys = sort_keys, .descs = ob_desc_flags }, ExprAggSortCtx.lessThan);
            const sorted_rows = try self.allocator.alloc(Row, proj_rows.len);
            defer self.allocator.free(sorted_rows);
            const sorted_vals = try self.allocator.alloc([]Value, proj_values.len);
            defer self.allocator.free(sorted_vals);
            for (indices, 0..) |idx, ii| {
                sorted_rows[ii] = proj_rows[idx];
                sorted_vals[ii] = proj_values[idx];
            }
            @memcpy(proj_rows, sorted_rows);
            @memcpy(proj_values, sorted_vals);
        }
    }

    // Apply LIMIT/OFFSET
    var result_start: usize = 0;
    var result_end: usize = proj_rows.len;
    if (sel.offset) |off| {
        if (off > 0) {
            result_start = @min(@as(usize, @intCast(off)), proj_rows.len);
        }
    }
    if (sel.limit) |lim| {
        if (lim >= 0) {
            result_end = @min(result_start + @as(usize, @intCast(lim)), proj_rows.len);
        }
    }
    if (result_start > 0 or result_end < proj_rows.len) {
        const sliced_rows = try self.allocator.alloc(Row, result_end - result_start);
        @memcpy(sliced_rows, proj_rows[result_start..result_end]);
        self.projected_rows = sliced_rows;
        self.projected_values = proj_values;
        try collectAggTexts(self, sel.select_exprs, proj_values);
        self.allocator.free(proj_rows);
        return .{ .rows = sliced_rows };
    }

    self.projected_rows = proj_rows;
    self.projected_values = proj_values;
    try collectAggTexts(self, sel.select_exprs, proj_values);

    // Set column names for aggregate results (used by materializeView)
    var agg_col_names = try self.allocator.alloc([]const u8, sel.select_exprs.len);
    for (0..sel.select_exprs.len) |i| {
        if (i < sel.aliases.len) {
            if (sel.aliases[i]) |alias| {
                agg_col_names[i] = try dupeStr(self.allocator, alias);
                continue;
            }
        }
        switch (sel.select_exprs[i]) {
            .column => |col_name| {
                agg_col_names[i] = try dupeStr(self.allocator, col_name);
            },
            else => {
                agg_col_names[i] = try dupeStr(self.allocator, "");
            },
        }
    }
    self.projected_column_names = agg_col_names;

    return .{ .rows = proj_rows };
}
