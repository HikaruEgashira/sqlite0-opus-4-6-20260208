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
const OrderByClause = parser.OrderByClause;
const OrderByItem = parser.OrderByItem;
const SetOp = parser.SetOp;
const NullOrder = parser.NullOrder;
const dupeStr = value_mod.dupeStr;
const compareValuesOrder = value_mod.compareValuesOrder;
const convert = @import("convert.zig");

const root = @import("root.zig");
const Database = root.Database;
const ExecuteResult = root.ExecuteResult;
const Statement = parser.Statement;

pub fn executeSetOperands(self: *Database, selects: []const []const u8, saved_rows: ?[]Row, saved_values: ?[][]Value, saved_texts: ?[][]const u8) !?std.ArrayList(std.ArrayList(Row)) {
    var operand_results = std.ArrayList(std.ArrayList(Row)){};

    for (selects) |select_sql| {
        self.projected_rows = null;
        self.projected_values = null;
        self.projected_texts = null;

        const result = try self.execute(select_sql);
        switch (result) {
            .rows => |rows| {
                var operand_rows = std.ArrayList(Row){};
                for (rows) |row| {
                    var new_values = try self.allocator.alloc(Value, row.values.len);
                    for (row.values, 0..) |val, vi| {
                        new_values[vi] = switch (val) {
                            .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                            .integer => |n| .{ .integer = n },
                            .null_val => .null_val,
                        };
                    }
                    try operand_rows.append(self.allocator, .{ .values = new_values });
                }
                try operand_results.append(self.allocator, operand_rows);
            },
            .err => |e| {
                _ = e;
                self.freeProjected();
                self.projected_rows = saved_rows;
                self.projected_values = saved_values;
                self.projected_texts = saved_texts;
                // Clean up already collected results
                for (operand_results.items) |*op_rows| {
                    for (op_rows.items) |r| self.freeRow(r);
                    op_rows.deinit(self.allocator);
                }
                operand_results.deinit(self.allocator);
                return null;
            },
            .ok => {
                try operand_results.append(self.allocator, std.ArrayList(Row){});
            },
        }

        self.freeProjected();
    }

    return operand_results;
}

pub fn executeUnion(self: *Database, union_sel: Statement.UnionSelect) !ExecuteResult {
    const saved_rows = self.projected_rows;
    const saved_values = self.projected_values;
    const saved_texts = self.projected_texts;

    const operand_results_opt = try executeSetOperands(self, union_sel.selects, saved_rows, saved_values, saved_texts);
    if (operand_results_opt == null) return .{ .err = "set operation failed" };
    var operand_results = operand_results_opt.?;
    defer {
        for (operand_results.items) |*op_rows| {
            op_rows.deinit(self.allocator);
        }
        operand_results.deinit(self.allocator);
    }

    var all_rows: std.ArrayList(Row) = .{};
    defer all_rows.deinit(self.allocator);

    switch (union_sel.set_op) {
        .union_all => {
            // Collect all rows from all operands
            for (operand_results.items) |*op_rows| {
                for (op_rows.items) |row| {
                    try all_rows.append(self.allocator, row);
                }
            }
        },
        .union_distinct => {
            // Collect all rows, then remove duplicates
            for (operand_results.items) |*op_rows| {
                for (op_rows.items) |row| {
                    try all_rows.append(self.allocator, row);
                }
            }
            try removeDuplicateRows(self, &all_rows);
        },
        .intersect => {
            // Return only rows present in ALL operands
            if (operand_results.items.len >= 1) {
                // Start with first operand (deduplicated)
                for (operand_results.items[0].items) |row| {
                    try all_rows.append(self.allocator, row);
                }
                try removeDuplicateRows(self, &all_rows);

                // Keep only rows that also appear in each subsequent operand
                for (operand_results.items[1..]) |*op_rows| {
                    var write_idx: usize = 0;
                    for (0..all_rows.items.len) |ri| {
                        var found = false;
                        for (op_rows.items) |other_row| {
                            if (rowsEqualUnion(self, all_rows.items[ri], other_row)) {
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            all_rows.items[write_idx] = all_rows.items[ri];
                            write_idx += 1;
                        } else {
                            self.freeRow(all_rows.items[ri]);
                        }
                    }
                    all_rows.shrinkRetainingCapacity(write_idx);
                }

                // Free rows from subsequent operands (they were not moved into all_rows)
                for (operand_results.items[1..]) |*op_rows| {
                    for (op_rows.items) |row| {
                        self.freeRow(row);
                    }
                }
            }
        },
        .except => {
            // Return rows from first operand not in subsequent operands
            if (operand_results.items.len >= 1) {
                // Start with first operand (deduplicated)
                for (operand_results.items[0].items) |row| {
                    try all_rows.append(self.allocator, row);
                }
                try removeDuplicateRows(self, &all_rows);

                // Remove rows that appear in any subsequent operand
                for (operand_results.items[1..]) |*op_rows| {
                    var write_idx: usize = 0;
                    for (0..all_rows.items.len) |ri| {
                        var found = false;
                        for (op_rows.items) |other_row| {
                            if (rowsEqualUnion(self, all_rows.items[ri], other_row)) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            all_rows.items[write_idx] = all_rows.items[ri];
                            write_idx += 1;
                        } else {
                            self.freeRow(all_rows.items[ri]);
                        }
                    }
                    all_rows.shrinkRetainingCapacity(write_idx);
                }

                // Free rows from subsequent operands
                for (operand_results.items[1..]) |*op_rows| {
                    for (op_rows.items) |row| {
                        self.freeRow(row);
                    }
                }
            }
        },
    }

    // Apply ORDER BY if present
    if (union_sel.order_by) |order_by| {
        const sorted_rows = all_rows.items;
        std.mem.sortUnstable(Row, sorted_rows, order_by, rowComparator);
        if (order_by.order == .desc) {
            std.mem.reverse(Row, sorted_rows);
        }
    }

    // Apply LIMIT/OFFSET if present
    var final_rows = all_rows.items;
    if (union_sel.offset) |offset| {
        if (offset > 0 and offset < final_rows.len) {
            final_rows = final_rows[@intCast(offset)..];
        }
    }
    if (union_sel.limit) |limit| {
        if (limit > 0 and limit < final_rows.len) {
            final_rows = final_rows[0..@intCast(limit)];
        }
    }

    // Store results — track values and texts for proper cleanup
    self.projected_rows = try self.allocator.dupe(Row, final_rows);
    var proj_vals = try self.allocator.alloc([]Value, final_rows.len);
    var alloc_texts_list: std.ArrayList([]const u8) = .{};
    for (final_rows, 0..) |row, i| {
        proj_vals[i] = row.values;
        for (row.values) |val| {
            if (val == .text) {
                alloc_texts_list.append(self.allocator, val.text) catch {};
            }
        }
    }
    self.projected_values = proj_vals;
    // Free rows that were trimmed by LIMIT/OFFSET
    for (all_rows.items) |row| {
        var in_final = false;
        for (final_rows) |fr| {
            if (fr.values.ptr == row.values.ptr) {
                in_final = true;
                break;
            }
        }
        if (!in_final) {
            self.freeRow(row);
        }
    }
    if (alloc_texts_list.items.len > 0) {
        self.projected_texts = alloc_texts_list.toOwnedSlice(self.allocator) catch null;
    } else {
        alloc_texts_list.deinit(self.allocator);
        self.projected_texts = null;
    }

    return .{ .rows = self.projected_rows.? };
}

pub fn removeDuplicateRows(self: *Database, rows: *std.ArrayList(Row)) !void {
    var i: usize = 0;
    while (i < rows.items.len) {
        var j = i + 1;
        while (j < rows.items.len) {
            if (rowsEqualUnion(self, rows.items[i], rows.items[j])) {
                // Free duplicate and remove
                self.freeRow(rows.items[j]);
                _ = rows.orderedRemove(j);
            } else {
                j += 1;
            }
        }
        i += 1;
    }
}

pub fn rowsEqualUnion(self: *Database, row1: Row, row2: Row) bool {
    if (row1.values.len != row2.values.len) return false;
    for (row1.values, row2.values) |v1, v2| {
        if (!valuesEqualUnion(self, v1, v2)) return false;
    }
    return true;
}

pub fn valuesEqualUnion(self: *Database, v1: Value, v2: Value) bool {
    _ = self;
    return switch (v1) {
        .integer => |n1| switch (v2) {
            .integer => |n2| n1 == n2,
            else => false,
        },
        .text => |t1| switch (v2) {
            .text => |t2| std.mem.eql(u8, t1, t2),
            else => false,
        },
        .null_val => switch (v2) {
            .null_val => true,
            else => false,
        },
    };
}

/// Sort rows by multiple ORDER BY columns
pub fn sortRowsByOrderBy(self: *Database, tbl: *const Table, rows: []Row, items: []const OrderByItem) !void {
    if (items.len == 0) return;

    // Check if any item uses expressions
    var has_expr = false;
    for (items) |item| {
        if (item.expr != null) { has_expr = true; break; }
    }

    if (has_expr) {
        // Precompute sort keys for expression-based ORDER BY
        const n_keys = items.len;
        var sort_keys = try self.allocator.alloc([]Value, rows.len);
        defer {
            for (sort_keys) |keys| {
                for (keys) |v| {
                    if (v == .text) self.allocator.free(v.text);
                }
                self.allocator.free(keys);
            }
            self.allocator.free(sort_keys);
        }
        for (rows, 0..) |row, ri| {
            var keys = try self.allocator.alloc(Value, n_keys);
            for (items, 0..) |item, ki| {
                if (item.expr) |expr| {
                    // Handle ORDER BY column position (e.g., ORDER BY 2)
                    if (expr.* == .integer_literal) {
                        const pos = expr.integer_literal;
                        if (pos >= 1 and pos <= @as(i64, @intCast(tbl.columns.len))) {
                            const ci: usize = @intCast(pos - 1);
                            keys[ki] = row.values[ci];
                            if (keys[ki] == .text) {
                                keys[ki] = .{ .text = try dupeStr(self.allocator, keys[ki].text) };
                            }
                        } else {
                            keys[ki] = .null_val;
                        }
                    } else {
                        keys[ki] = try self.evalExpr(expr, tbl, row);
                    }
                } else {
                    const col_idx = tbl.findColumnIndex(item.column) orelse {
                        keys[ki] = .null_val;
                        continue;
                    };
                    keys[ki] = row.values[col_idx];
                    // Don't duplicate text - just reference
                    if (keys[ki] == .text) {
                        keys[ki] = .{ .text = try dupeStr(self.allocator, keys[ki].text) };
                    }
                }
            }
            sort_keys[ri] = keys;
        }

        var desc_flags = try self.allocator.alloc(bool, n_keys);
        defer self.allocator.free(desc_flags);
        for (items, 0..) |item, i| desc_flags[i] = item.order == .desc;

        // Create index array and sort indices
        const indices = try self.allocator.alloc(usize, rows.len);
        defer self.allocator.free(indices);
        for (indices, 0..) |*idx, i| idx.* = i;

        const ExprSortCtx = struct {
            keys: []const []Value,
            descs: []const bool,
            fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                for (ctx.keys[a], ctx.keys[b], ctx.descs) |va, vb, is_desc| {
                    const cmp = compareValuesOrder(va, vb);
                    if (cmp == .eq) continue;
                    if (is_desc) return cmp == .gt;
                    return cmp == .lt;
                }
                return false;
            }
        };
        std.mem.sort(usize, indices, ExprSortCtx{ .keys = sort_keys, .descs = desc_flags }, ExprSortCtx.lessThan);

        // Reorder rows in-place using indices
        var sorted = try self.allocator.alloc(Row, rows.len);
        defer self.allocator.free(sorted);
        for (indices, 0..) |idx, i| sorted[i] = rows[idx];
        @memcpy(rows, sorted);
    } else {
        // Simple column-based sort
        var col_indices = try self.allocator.alloc(usize, items.len);
        defer self.allocator.free(col_indices);
        var desc_flags = try self.allocator.alloc(bool, items.len);
        defer self.allocator.free(desc_flags);

        var null_flags = try self.allocator.alloc(NullOrder, items.len);
        defer self.allocator.free(null_flags);

        for (items, 0..) |item, i| {
            col_indices[i] = tbl.findColumnIndex(item.column) orelse return;
            desc_flags[i] = item.order == .desc;
            null_flags[i] = item.null_order;
        }

        const MultiSortCtx = struct {
            indices: []const usize,
            descs: []const bool,
            null_orders: []const NullOrder,

            fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                for (ctx.indices, ctx.descs, ctx.null_orders) |col_idx, is_desc, null_ord| {
                    const va = a.values[col_idx];
                    const vb = b.values[col_idx];
                    // Handle explicit NULLS FIRST/LAST
                    if (null_ord != .default) {
                        const a_null = (va == .null_val);
                        const b_null = (vb == .null_val);
                        if (a_null and b_null) continue;
                        if (a_null or b_null) {
                            const nulls_first = (null_ord == .first);
                            if (a_null) return nulls_first;
                            return !nulls_first;
                        }
                    }
                    // Default: use compareValuesOrder (NULL is smallest)
                    const cmp = compareValuesOrder(va, vb);
                    if (cmp == .eq) continue;
                    if (is_desc) return cmp == .gt;
                    return cmp == .lt;
                }
                return false;
            }
        };

        std.mem.sort(Row, rows, MultiSortCtx{ .indices = col_indices, .descs = desc_flags, .null_orders = null_flags }, MultiSortCtx.lessThan);
    }
}

fn rowComparator(_: OrderByClause, row1: Row, row2: Row) bool {
    // Placeholder: compare first column for now
    if (row1.values.len == 0 or row2.values.len == 0) return false;
    const v1 = row1.values[0];
    const v2 = row2.values[0];
    return switch (v1) {
        .integer => |n1| switch (v2) {
            .integer => |n2| n1 < n2,
            else => false,
        },
        .text => |t1| switch (v2) {
            .text => |t2| std.mem.order(u8, t1, t2) == .lt,
            else => false,
        },
        .null_val => false,
    };
}
