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
const WhereClause = parser.WhereClause;
const CompOp = parser.CompOp;

const rowsEqual = value_mod.rowsEqual;

pub fn executeSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
    // Handle table-less SELECT (e.g., SELECT ABS(-10);)
    if (sel.table_name.len == 0) {
        return executeTablelessSelect(self, sel);
    }

    // Handle sqlite_master / sqlite_schema virtual table
    if (std.mem.eql(u8, sel.table_name, "sqlite_master") or std.mem.eql(u8, sel.table_name, "sqlite_schema")) {
        return self.executeSqliteMaster(sel);
    }

    // Resolve derived table (FROM subquery)
    if (sel.subquery_sql) |sq_sql| {
        defer self.allocator.free(@constCast(sq_sql));
        try materializeView(self, sel.table_name, sq_sql);
    }

    // Resolve view: if table_name is a view, materialize it as a temp table
    if (!self.tables.contains(sel.table_name)) {
        if (self.views.get(sel.table_name)) |view_sql_str| {
            try materializeView(self, sel.table_name, view_sql_str);
        }
    }

    // Handle JOIN queries
    if (sel.joins.len > 0) {
        return self.executeJoin(sel);
    }

    if (self.tables.getPtr(sel.table_name)) |table| {
        // Set current table alias for correlated subquery context
        const saved_alias = self.current_table_alias;
        self.current_table_alias = sel.table_alias;
        defer self.current_table_alias = saved_alias;

        // Collect matching rows (with optional WHERE filter)
        var matching_rows: std.ArrayList(Row) = .{};
        defer matching_rows.deinit(self.allocator);
        const src_rows = table.storage().scan();
        for (src_rows) |row| {
            if (sel.where_expr) |we| {
                const val = try self.evalExpr(we, table, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (!self.valueToBool(val)) continue;
            } else if (sel.where) |where| {
                if (!(try self.matchesWhereWithSubquery(table, row, where))) continue;
            }
            try matching_rows.append(self.allocator, row);
        }

        // Check if this is an aggregate query
        const has_agg = blk: {
            for (sel.select_exprs) |expr| {
                if (expr == .aggregate) break :blk true;
                if (expr == .expr) {
                    if (parser.Parser.exprAsAggregate(expr.expr) != null) break :blk true;
                }
            }
            break :blk false;
        };

        // Check if any select_expr is a complex expression
        const has_expr = blk: {
            for (sel.select_exprs) |expr| {
                if (expr == .expr) break :blk true;
            }
            break :blk false;
        };

        if (has_agg or sel.group_by != null) {
            return self.executeAggregate(table, sel, matching_rows.items);
        }

        // Apply ORDER BY (multi-column)
        if (sel.order_by) |order_by| {
            try self.sortRowsByOrderBy(table, matching_rows.items, order_by.items);
        }

        // Apply DISTINCT (remove duplicate rows)
        if (sel.distinct) {
            var write_idx: usize = 0;
            for (matching_rows.items, 0..) |row, ri| {
                var is_dup = false;
                for (matching_rows.items[0..write_idx]) |prev| {
                    if (rowsEqual(row, prev)) {
                        is_dup = true;
                        break;
                    }
                }
                if (!is_dup) {
                    matching_rows.items[write_idx] = matching_rows.items[ri];
                    write_idx += 1;
                }
            }
            matching_rows.shrinkRetainingCapacity(write_idx);
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
            // Store column names from table
            var col_names = try self.allocator.alloc([]const u8, table.columns.len);
            for (table.columns, 0..) |col, i| {
                col_names[i] = try dupeStr(self.allocator, col.name);
            }
            self.projected_column_names = col_names;
            return .{ .rows = proj_rows };
        }

        // If any expression is complex (not a simple column), evaluate using Expr AST
        if (has_expr) {
            return evaluateExprSelect(self, sel, table, result_rows);
        }

        return projectColumns(self, sel, table, result_rows);
    }
    return .{ .err = "table not found" };
}

/// Materialize a view as a temporary table for querying.
/// Cleanup is deferred to freeProjected via temp_cte_names.
pub fn materializeView(self: *Database, view_name: []const u8, view_sql: []const u8) !void {
    // Save and restore projected state around view execution
    const saved = self.saveProjectedState();
    const view_result = try self.execute(view_sql);
    const view_col_names = self.projected_column_names;
    // Save inner projected state for cleanup after deep-copy
    self.projected_column_names = null;
    const inner_rows = self.projected_rows;
    const inner_values = self.projected_values;
    const inner_texts = self.projected_texts;
    self.projected_rows = null;
    self.projected_values = null;
    self.projected_texts = null;
    self.restoreProjectedState(saved);

    switch (view_result) {
        .rows => |rows| {
            // Determine column count
            const num_cols = if (rows.len > 0) rows[0].values.len else 0;
            var cols = try self.allocator.alloc(Column, num_cols);
            for (0..num_cols) |ci| {
                var col_name: []const u8 = "";
                if (view_col_names) |pcn| {
                    if (ci < pcn.len) col_name = pcn[ci];
                }
                cols[ci] = .{ .name = try dupeStr(self.allocator, col_name), .col_type = "TEXT", .is_primary_key = false };
            }
            const tbl_name = try dupeStr(self.allocator, view_name);
            var tbl = Table.init(self.allocator, tbl_name, cols);
            // Deep-copy row values
            for (rows) |row| {
                var new_values = try self.allocator.alloc(Value, row.values.len);
                for (row.values, 0..) |v, vi| {
                    new_values[vi] = switch (v) {
                        .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                        else => v,
                    };
                }
                const rowid = tbl.next_rowid;
                tbl.next_rowid += 1;
                tbl.storage().append(self.allocator, .{ .rowid = rowid, .values = new_values }) catch {};
            }
            self.tables.put(tbl_name, tbl) catch {};

            // Register for cleanup in freeProjected (same as CTE pattern)
            const cte_name = try dupeStr(self.allocator, view_name);
            if (self.temp_cte_names) |existing| {
                var new_names = try self.allocator.alloc([]const u8, existing.len + 1);
                @memcpy(new_names[0..existing.len], existing);
                new_names[existing.len] = cte_name;
                self.allocator.free(existing);
                self.temp_cte_names = new_names;
            } else {
                var new_names = try self.allocator.alloc([]const u8, 1);
                new_names[0] = cte_name;
                self.temp_cte_names = new_names;
            }

            // Free view column names and inner projected state
            if (view_col_names) |pcn| {
                for (pcn) |name| self.allocator.free(name);
                self.allocator.free(pcn);
            }
            if (inner_texts) |pt| {
                for (pt) |t| self.allocator.free(t);
                self.allocator.free(pt);
            }
            if (inner_values) |pv| {
                for (pv) |v| self.allocator.free(v);
                self.allocator.free(pv);
            }
            if (inner_rows) |pr| self.allocator.free(pr);
        },
        else => {
            if (view_col_names) |pcn| {
                for (pcn) |name| self.allocator.free(name);
                self.allocator.free(pcn);
            }
            if (inner_texts) |pt| {
                for (pt) |t| self.allocator.free(t);
                self.allocator.free(pt);
            }
            if (inner_values) |pv| {
                for (pv) |v| self.allocator.free(v);
                self.allocator.free(pv);
            }
            if (inner_rows) |pr| self.allocator.free(pr);
        },
    }
}

/// Handle SELECT without FROM (e.g., SELECT ABS(-10); SELECT 1+2;)
pub fn executeTablelessSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
    const expr_count = sel.result_exprs.len;
    // Create a dummy table with no columns for evalExpr context
    var dummy_table = Table.init(self.allocator, "", &.{});
    defer dummy_table.row_storage.storage().deinit(self.allocator);
    const dummy_row = Row{ .values = &.{} };

    var values = try self.allocator.alloc(Value, expr_count);
    var alloc_texts: std.ArrayList([]const u8) = .{};

    for (sel.result_exprs, 0..) |expr, i| {
        values[i] = try self.evalExpr(expr, &dummy_table, dummy_row);
        if (values[i] == .text) {
            alloc_texts.append(self.allocator, values[i].text) catch {};
        }
    }

    var proj_values = try self.allocator.alloc([]Value, 1);
    proj_values[0] = values;
    var proj_rows = try self.allocator.alloc(Row, 1);
    proj_rows[0] = .{ .values = values };
    self.projected_rows = proj_rows;
    self.projected_values = proj_values;
    if (alloc_texts.items.len > 0) {
        self.projected_texts = alloc_texts.toOwnedSlice(self.allocator) catch null;
    } else {
        alloc_texts.deinit(self.allocator);
    }
    return .{ .rows = proj_rows };
}

/// Evaluate SELECT with expression ASTs (handles arithmetic, concat, etc.)
pub fn evaluateExprSelect(self: *Database, sel: Statement.Select, tbl: *const Table, result_rows: []const Row) !ExecuteResult {
    const expr_count = sel.result_exprs.len;
    const row_count = result_rows.len;
    var proj_values = try self.allocator.alloc([]Value, row_count);
    var proj_rows = try self.allocator.alloc(Row, row_count);
    var alloc_texts: std.ArrayList([]const u8) = .{};

    for (result_rows, 0..) |row, ri| {
        var values = try self.allocator.alloc(Value, expr_count);
        for (sel.result_exprs, 0..) |expr, ei| {
            values[ei] = try self.evalExpr(expr, tbl, row);
            // Track allocated text values for cleanup
            if (values[ei] == .text) {
                alloc_texts.append(self.allocator, values[ei].text) catch {};
            }
        }
        proj_values[ri] = values;
        proj_rows[ri] = .{ .values = values };
    }

    // Evaluate window functions (post-processing)
    try self.evaluateWindowFunctions(sel.result_exprs, proj_rows, proj_values, tbl, result_rows, &alloc_texts);

    // If no explicit ORDER BY but window functions have ORDER BY, sort by window ORDER BY
    // (SQLite implicitly sorts output by PARTITION BY + window ORDER BY)
    if (sel.order_by == null) {
        for (sel.result_exprs) |rexpr| {
            if (rexpr.* == .window_func and rexpr.window_func.order_by.len > 0) {
                const wf_data = rexpr.window_func;
                // Build sort keys: PARTITION BY columns (ASC) + ORDER BY columns
                var win_ob_indices: std.ArrayList(usize) = .{};
                defer win_ob_indices.deinit(self.allocator);
                var win_ob_descs: std.ArrayList(bool) = .{};
                defer win_ob_descs.deinit(self.allocator);
                var all_found = true;

                // First: PARTITION BY columns (always ASC)
                for (wf_data.partition_by) |pcol| {
                    var found_idx: ?usize = null;
                    for (sel.aliases, 0..) |alias, ai| {
                        if (alias) |a| {
                            if (std.ascii.eqlIgnoreCase(pcol, a)) {
                                found_idx = ai;
                                break;
                            }
                        }
                    }
                    if (found_idx == null) {
                        for (sel.result_exprs, 0..) |re, ei| {
                            if (re.* == .column_ref and std.ascii.eqlIgnoreCase(re.column_ref, pcol)) {
                                found_idx = ei;
                                break;
                            }
                        }
                    }
                    if (found_idx) |fi| {
                        win_ob_indices.append(self.allocator, fi) catch {};
                        win_ob_descs.append(self.allocator, false) catch {};
                    } else {
                        all_found = false;
                        break;
                    }
                }

                // Then: ORDER BY columns
                if (all_found) {
                    for (wf_data.order_by) |wob| {
                        var found_idx: ?usize = null;
                        if (wob.column.len > 0) {
                            for (sel.aliases, 0..) |alias, ai| {
                                if (alias) |a| {
                                    if (std.ascii.eqlIgnoreCase(wob.column, a)) {
                                        found_idx = ai;
                                        break;
                                    }
                                }
                            }
                            if (found_idx == null) {
                                for (sel.result_exprs, 0..) |re, ei| {
                                    if (re.* == .column_ref and std.ascii.eqlIgnoreCase(re.column_ref, wob.column)) {
                                        found_idx = ei;
                                        break;
                                    }
                                }
                            }
                        }
                        if (found_idx) |fi| {
                            win_ob_indices.append(self.allocator, fi) catch {};
                            win_ob_descs.append(self.allocator, wob.order == .desc) catch {};
                        } else {
                            all_found = false;
                            break;
                        }
                    }
                }

                if (all_found and win_ob_indices.items.len > 0) {
                    const WinSortCtx = struct {
                        idx: []const usize,
                        descs: []const bool,
                        fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                            for (ctx.idx, ctx.descs) |ci, is_desc| {
                                const va = a.values[ci];
                                const vb = b.values[ci];
                                const cmp = compareValuesOrder(va, vb);
                                if (cmp == .eq) continue;
                                if (is_desc) return cmp == .gt;
                                return cmp == .lt;
                            }
                            return false;
                        }
                    };
                    std.mem.sort(Row, proj_rows, WinSortCtx{ .idx = win_ob_indices.items, .descs = win_ob_descs.items }, WinSortCtx.lessThan);
                }
                break; // Use first window function's ORDER BY
            }
        }
    }

    // Re-sort by ORDER BY (resolving aliases to projected column indices)
    if (sel.order_by) |order_by| {
        var ob_indices = try self.allocator.alloc(usize, order_by.items.len);
        defer self.allocator.free(ob_indices);
        var ob_descs = try self.allocator.alloc(bool, order_by.items.len);
        defer self.allocator.free(ob_descs);
        var all_resolved = true;
        for (order_by.items, 0..) |item, oi| {
            ob_descs[oi] = item.order == .desc;
            var col_idx: ?usize = null;
            // Check for ORDER BY column position (e.g., ORDER BY 2)
            if (item.expr) |e| {
                if (e.* == .integer_literal) {
                    const pos = e.integer_literal;
                    if (pos >= 1 and pos <= @as(i64, @intCast(expr_count))) {
                        col_idx = @intCast(pos - 1);
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
                // Fallback: check column refs in result_exprs
                if (col_idx == null) {
                    for (sel.result_exprs, 0..) |rexpr, ei| {
                        if (rexpr.* == .column_ref and std.mem.eql(u8, rexpr.column_ref, item.column)) {
                            col_idx = ei;
                            break;
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
            const MultiSortCtx2 = struct {
                indices: []const usize,
                descs: []const bool,
                fn lessThan(ctx2: @This(), a: Row, b: Row) bool {
                    for (ctx2.indices, ctx2.descs) |ci, is_desc| {
                        const va = a.values[ci];
                        const vb = b.values[ci];
                        const cmp = compareValuesOrder(va, vb);
                        if (cmp == .eq) continue;
                        if (is_desc) return cmp == .gt;
                        return cmp == .lt;
                    }
                    return false;
                }
            };
            std.mem.sort(Row, proj_rows, MultiSortCtx2{ .indices = ob_indices, .descs = ob_descs }, MultiSortCtx2.lessThan);
        }
    }

    // Store column names for CREATE TABLE AS SELECT
    var col_names = try self.allocator.alloc([]const u8, expr_count);
    for (0..expr_count) |i| {
        if (i < sel.aliases.len) {
            if (sel.aliases[i]) |alias| {
                col_names[i] = try dupeStr(self.allocator, alias);
                continue;
            }
        }
        if (sel.result_exprs[i].* == .column_ref) {
            col_names[i] = try dupeStr(self.allocator, sel.result_exprs[i].column_ref);
        } else {
            col_names[i] = try std.fmt.allocPrint(self.allocator, "column{d}", .{i});
        }
    }
    self.projected_column_names = col_names;

    // Apply DISTINCT on evaluated results
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

    self.projected_rows = proj_rows;
    self.projected_values = proj_values;
    if (alloc_texts.items.len > 0) {
        self.projected_texts = alloc_texts.toOwnedSlice(self.allocator) catch null;
    } else {
        alloc_texts.deinit(self.allocator);
    }
    return .{ .rows = proj_rows };
}

pub fn projectColumns(self: *Database, sel: Statement.Select, table: *const Table, result_rows: []const Row) !ExecuteResult {
    // Use sentinel value to indicate rowid pseudo-column
    const ROWID_SENTINEL: usize = std.math.maxInt(usize);
    var col_indices = try self.allocator.alloc(usize, sel.columns.len);
    defer self.allocator.free(col_indices);
    for (sel.columns, 0..) |sel_col, i| {
        if (std.ascii.eqlIgnoreCase(sel_col, "rowid")) {
            col_indices[i] = ROWID_SENTINEL;
            continue;
        }
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
            if (src_idx == ROWID_SENTINEL) {
                values[dst_idx] = .{ .integer = row.rowid };
            } else {
                values[dst_idx] = row.values[src_idx];
            }
        }
        proj_values[ri] = values;
        proj_rows[ri] = .{ .values = values };
    }
    // Apply DISTINCT on projected columns
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
    self.projected_rows = proj_rows;
    self.projected_values = proj_values;
    // Store column names
    var col_names = try self.allocator.alloc([]const u8, sel.columns.len);
    for (sel.columns, 0..) |col, i| {
        col_names[i] = try dupeStr(self.allocator, col);
    }
    self.projected_column_names = col_names;
    return .{ .rows = proj_rows };
}
