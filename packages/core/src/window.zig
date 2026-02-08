const std = @import("std");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");
const parser = @import("parser");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Table = table_mod.Table;
const Expr = parser.Expr;
const SelectExpr = parser.SelectExpr;
const AggFunc = parser.AggFunc;
const OrderByItem = parser.OrderByItem;
const dupeStr = value_mod.dupeStr;

const root = @import("root.zig");
const Database = root.Database;

const compareValuesOrder = value_mod.compareValuesOrder;

pub fn evaluateWindowFunctions(
    self: *Database,
    result_exprs: []const *const Expr,
    proj_rows: []Row,
    proj_values: [][]Value,
    tbl: *const Table,
    src_rows: []const Row,
    alloc_texts: *std.ArrayList([]const u8),
) !void {
    const row_count = proj_rows.len;
    if (row_count == 0) return;

    for (result_exprs, 0..) |expr, col_idx| {
        if (expr.* != .window_func) continue;
        const wf = expr.window_func;

        // Build sort indices based on OVER (PARTITION BY ... ORDER BY ...)
        var indices = try self.allocator.alloc(usize, row_count);
        defer self.allocator.free(indices);
        for (0..row_count) |i| indices[i] = i;

        // Sort indices by partition columns then order columns
        const SortCtx = struct {
            db: *Database,
            tbl: *const Table,
            src_rows: []const Row,
            wf: @TypeOf(wf),

            fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                // Compare partition columns first
                for (ctx.wf.partition_by) |pcol| {
                    const pa_idx = ctx.tbl.findColumnIndex(pcol) orelse continue;
                    const va = ctx.src_rows[a].values[pa_idx];
                    const vb = ctx.src_rows[b].values[pa_idx];
                    const cmp = compareValuesOrder(va, vb);
                    if (cmp == .lt) return true;
                    if (cmp == .gt) return false;
                }
                // Then compare order columns
                for (ctx.wf.order_by) |ob| {
                    if (ob.column.len > 0) {
                        const oi = ctx.tbl.findColumnIndex(ob.column) orelse continue;
                        const va = ctx.src_rows[a].values[oi];
                        const vb = ctx.src_rows[b].values[oi];
                        const cmp = compareValuesOrder(va, vb);
                        if (ob.order == .asc) {
                            if (cmp == .lt) return true;
                            if (cmp == .gt) return false;
                        } else {
                            if (cmp == .gt) return true;
                            if (cmp == .lt) return false;
                        }
                    }
                }
                return false;
            }
        };

        const ctx = SortCtx{ .db = self, .tbl = tbl, .src_rows = src_rows, .wf = wf };
        std.mem.sortUnstable(usize, indices, ctx, SortCtx.lessThan);

        // Compute window function values
        var row_num: i64 = 0;
        var rank: i64 = 0;
        var dense_rank: i64 = 0;
        var prev_partition: ?usize = null;

        for (indices) |idx| {
            // Check partition boundary
            var new_partition = false;
            if (prev_partition == null) {
                new_partition = true;
            } else {
                for (wf.partition_by) |pcol| {
                    const pi = tbl.findColumnIndex(pcol) orelse continue;
                    if (compareValuesOrder(src_rows[idx].values[pi], src_rows[prev_partition.?].values[pi]) != .eq) {
                        new_partition = true;
                        break;
                    }
                }
            }

            if (new_partition) {
                row_num = 1;
                rank = 1;
                dense_rank = 1;
            } else {
                row_num += 1;
                // Check if order values are same as previous row (for RANK/DENSE_RANK)
                var same_order = true;
                for (wf.order_by) |ob| {
                    if (ob.column.len > 0) {
                        const oi = tbl.findColumnIndex(ob.column) orelse continue;
                        if (compareValuesOrder(src_rows[idx].values[oi], src_rows[prev_partition.?].values[oi]) != .eq) {
                            same_order = false;
                            break;
                        }
                    }
                }
                if (same_order) {
                    // Same rank values
                } else {
                    rank = row_num;
                    dense_rank += 1;
                }
            }

            switch (wf.func) {
                .row_number => {
                    proj_values[idx][col_idx] = .{ .integer = row_num };
                },
                .rank => {
                    proj_values[idx][col_idx] = .{ .integer = rank };
                },
                .dense_rank => {
                    proj_values[idx][col_idx] = .{ .integer = dense_rank };
                },
                .win_sum, .win_avg, .win_count, .win_min, .win_max, .win_total,
                .lag, .lead, .ntile, .first_value, .last_value,
                => {
                    // Computed per-partition below
                },
            }

            proj_rows[idx] = .{ .values = proj_values[idx] };
            prev_partition = idx;
        }

        // For aggregate/value window functions, compute per-partition
        switch (wf.func) {
            .win_sum, .win_avg, .win_count, .win_min, .win_max, .win_total => {
                try computeWindowAggregates(self, wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows, alloc_texts);
            },
            .lag, .lead, .ntile, .first_value, .last_value => {
                try computeWindowValueFunctions(self, wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows);
            },
            else => {},
        }
    }
}

pub fn computeWindowAggregates(
    self: *Database,
    wf: anytype,
    indices: []const usize,
    col_idx: usize,
    tbl: *const Table,
    src_rows: []const Row,
    proj_values: [][]Value,
    proj_rows: []Row,
    alloc_texts: *std.ArrayList([]const u8),
) !void {
    // Process rows in sorted order (by partition then order)
    // Group into partitions and compute aggregate for each
    var part_start: usize = 0;
    while (part_start < indices.len) {
        // Find end of current partition
        var part_end = part_start + 1;
        while (part_end < indices.len) {
            var same_partition = true;
            for (wf.partition_by) |pcol| {
                const pi = tbl.findColumnIndex(pcol) orelse continue;
                if (compareValuesOrder(src_rows[indices[part_start]].values[pi], src_rows[indices[part_end]].values[pi]) != .eq) {
                    same_partition = false;
                    break;
                }
            }
            if (!same_partition) break;
            part_end += 1;
        }

        const has_order_by = wf.order_by.len > 0;

        if (has_order_by) {
            // Running/cumulative aggregate (default frame: UNBOUNDED PRECEDING to CURRENT ROW)
            var run_sum: f64 = 0;
            var run_count: i64 = 0;
            var run_min: Value = .null_val;
            var run_max: Value = .null_val;
            var run_has_value = false;

            for (part_start..part_end) |i| {
                const row_idx = indices[i];
                const is_star = if (wf.arg) |arg_expr| (arg_expr.* == .star) else true;
                const val = if (is_star) Value{ .integer = 1 } else (self.evalExpr(wf.arg.?, tbl, src_rows[row_idx]) catch .null_val);

                if (val != .null_val) {
                    if (wf.func == .win_count) {
                        run_count += 1;
                    } else {
                        const fval = self.valueToF64(val) orelse 0;
                        if (!run_has_value) {
                            run_sum = fval;
                            run_min = val;
                            run_max = val;
                            run_count = 1;
                            run_has_value = true;
                        } else {
                            run_sum += fval;
                            run_count += 1;
                            if (compareValuesOrder(val, run_min) == .lt) run_min = val;
                            if (compareValuesOrder(val, run_max) == .gt) run_max = val;
                        }
                    }
                }

                // Assign running aggregate value to this row
                const result: Value = switch (wf.func) {
                    .win_count => .{ .integer = run_count },
                    .win_sum => if (run_has_value) blk: {
                        const int_val = @as(i64, @intFromFloat(run_sum));
                        if (run_sum == @as(f64, @floatFromInt(int_val))) {
                            break :blk Value{ .integer = int_val };
                        }
                        break :blk self.formatFloat(run_sum);
                    } else .null_val,
                    .win_avg => if (run_count > 0) self.formatFloat(run_sum / @as(f64, @floatFromInt(run_count))) else .null_val,
                    .win_min => run_min,
                    .win_max => run_max,
                    .win_total => self.formatFloat(run_sum),
                    else => .null_val,
                };

                if (result == .text) {
                    alloc_texts.append(self.allocator, result.text) catch {};
                }
                proj_values[row_idx][col_idx] = result;
                proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
            }
        } else {
            // No ORDER BY: aggregate over entire partition
            var agg_sum: f64 = 0;
            var agg_count: i64 = 0;
            var agg_min: Value = .null_val;
            var agg_max: Value = .null_val;
            var has_value = false;

            for (part_start..part_end) |i| {
                const row_idx = indices[i];
                const is_star = if (wf.arg) |arg_expr| (arg_expr.* == .star) else true;
                const val = if (is_star) Value{ .integer = 1 } else (self.evalExpr(wf.arg.?, tbl, src_rows[row_idx]) catch .null_val);

                if (val == .null_val) continue;

                if (wf.func == .win_count) {
                    agg_count += 1;
                } else {
                    const fval = self.valueToF64(val) orelse 0;
                    if (!has_value) {
                        agg_sum = fval;
                        agg_min = val;
                        agg_max = val;
                        agg_count = 1;
                        has_value = true;
                    } else {
                        agg_sum += fval;
                        agg_count += 1;
                        if (compareValuesOrder(val, agg_min) == .lt) agg_min = val;
                        if (compareValuesOrder(val, agg_max) == .gt) agg_max = val;
                    }
                }
            }

            const result: Value = switch (wf.func) {
                .win_count => .{ .integer = agg_count },
                .win_sum => if (has_value) blk: {
                    const int_val = @as(i64, @intFromFloat(agg_sum));
                    if (agg_sum == @as(f64, @floatFromInt(int_val))) {
                        break :blk Value{ .integer = int_val };
                    }
                    break :blk self.formatFloat(agg_sum);
                } else .null_val,
                .win_avg => if (agg_count > 0) self.formatFloat(agg_sum / @as(f64, @floatFromInt(agg_count))) else .null_val,
                .win_min => agg_min,
                .win_max => agg_max,
                .win_total => self.formatFloat(agg_sum),
                else => .null_val,
            };

            if (result == .text) {
                alloc_texts.append(self.allocator, result.text) catch {};
            }
            for (part_start..part_end) |i| {
                const row_idx = indices[i];
                proj_values[row_idx][col_idx] = result;
                proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
            }
        }

        part_start = part_end;
    }
}

pub fn computeWindowValueFunctions(
    self: *Database,
    wf: anytype,
    indices: []const usize,
    col_idx: usize,
    tbl: *const Table,
    src_rows: []const Row,
    proj_values: [][]Value,
    proj_rows: []Row,
) !void {
    // Process rows in sorted order, group into partitions
    var part_start: usize = 0;
    while (part_start < indices.len) {
        var part_end = part_start + 1;
        while (part_end < indices.len) {
            var same_partition = true;
            for (wf.partition_by) |pcol| {
                const pi = tbl.findColumnIndex(pcol) orelse continue;
                if (compareValuesOrder(src_rows[indices[part_start]].values[pi], src_rows[indices[part_end]].values[pi]) != .eq) {
                    same_partition = false;
                    break;
                }
            }
            if (!same_partition) break;
            part_end += 1;
        }

        const part_len = part_end - part_start;

        switch (wf.func) {
            .lag => {
                const offset: usize = @intCast(@max(wf.offset, 0));
                for (part_start..part_end) |i| {
                    const row_idx = indices[i];
                    const pos_in_part = i - part_start;
                    if (pos_in_part >= offset) {
                        const src_idx = indices[i - offset];
                        const val = if (wf.arg) |arg_expr| (self.evalExpr(arg_expr, tbl, src_rows[src_idx]) catch .null_val) else .null_val;
                        proj_values[row_idx][col_idx] = val;
                    } else {
                        // Use default value
                        const val = if (wf.default_val) |def_expr| (self.evalExpr(def_expr, tbl, src_rows[row_idx]) catch .null_val) else .null_val;
                        proj_values[row_idx][col_idx] = val;
                    }
                    proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
                }
            },
            .lead => {
                const offset: usize = @intCast(@max(wf.offset, 0));
                for (part_start..part_end) |i| {
                    const row_idx = indices[i];
                    const pos_in_part = i - part_start;
                    if (pos_in_part + offset < part_len) {
                        const src_idx = indices[i + offset];
                        const val = if (wf.arg) |arg_expr| (self.evalExpr(arg_expr, tbl, src_rows[src_idx]) catch .null_val) else .null_val;
                        proj_values[row_idx][col_idx] = val;
                    } else {
                        const val = if (wf.default_val) |def_expr| (self.evalExpr(def_expr, tbl, src_rows[row_idx]) catch .null_val) else .null_val;
                        proj_values[row_idx][col_idx] = val;
                    }
                    proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
                }
            },
            .ntile => {
                const n: usize = @intCast(@max(wf.offset, 1));
                // SQLite NTILE: first (part_len % n) tiles get (part_len/n + 1) rows, rest get (part_len/n)
                const base_size = part_len / n;
                const extra = part_len % n;
                var pos: usize = 0;
                var tile: i64 = 1;
                for (part_start..part_end) |i| {
                    const row_idx = indices[i];
                    const cur_tile_size = base_size + @as(usize, if (@as(usize, @intCast(tile - 1)) < extra) 1 else 0);
                    proj_values[row_idx][col_idx] = .{ .integer = tile };
                    proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
                    pos += 1;
                    if (pos >= cur_tile_size) {
                        tile += 1;
                        pos = 0;
                    }
                }
            },
            .first_value => {
                // First value in partition
                const first_idx = indices[part_start];
                const val = if (wf.arg) |arg_expr| (self.evalExpr(arg_expr, tbl, src_rows[first_idx]) catch .null_val) else .null_val;
                for (part_start..part_end) |i| {
                    const row_idx = indices[i];
                    proj_values[row_idx][col_idx] = val;
                    proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
                }
            },
            .last_value => {
                // Last value in partition
                const last_idx = indices[part_end - 1];
                const val = if (wf.arg) |arg_expr| (self.evalExpr(arg_expr, tbl, src_rows[last_idx]) catch .null_val) else .null_val;
                for (part_start..part_end) |i| {
                    const row_idx = indices[i];
                    proj_values[row_idx][col_idx] = val;
                    proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
                }
            },
            else => {},
        }

        part_start = part_end;
    }
}
