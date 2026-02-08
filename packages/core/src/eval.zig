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
const ScalarFunc = parser.ScalarFunc;
const BinOp = parser.BinOp;
const UnaryOp = parser.UnaryOp;
const CompOp = parser.CompOp;
const dupeStr = value_mod.dupeStr;
const likeMatch = value_mod.likeMatch;
const globMatch = value_mod.globMatch;
const compareValuesOrder = value_mod.compareValuesOrder;

const root = @import("root.zig");
const Database = root.Database;

/// Evaluate an expression AST against a row
pub fn evalExpr(self: *Database, expr: *const Expr, tbl: *const Table, row: Row) !Value {
    switch (expr.*) {
        .integer_literal => |n| return .{ .integer = n },
        .float_literal => |f| {
            return self.formatFloat(f);
        },
        .string_literal => |s| return .{ .text = try dupeStr(self.allocator, s) },
        .null_literal => return .null_val,
        .star => return .null_val, // star is not valid in eval context
        .column_ref => |name| {
            if (tbl.findColumnIndex(name)) |col_idx| {
                const val = row.values[col_idx];
                return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
            }
            // Fallback: check outer context for correlated subqueries
            if (self.outer_context) |ctx| {
                if (ctx.table.findColumnIndex(name)) |outer_idx| {
                    const val = ctx.row.values[outer_idx];
                    return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
                }
            }
            return .null_val;
        },
        .qualified_ref => |qr| {
            // Check if qualified ref matches outer context table
            if (self.outer_context) |ctx| {
                const matches_outer = std.mem.eql(u8, qr.table, ctx.table_name) or
                    (if (ctx.table_alias) |a| std.mem.eql(u8, qr.table, a) else false);
                if (matches_outer) {
                    if (ctx.table.findColumnIndex(qr.column)) |outer_idx| {
                        const val = ctx.row.values[outer_idx];
                        return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
                    }
                    return .null_val;
                }
            }
            // For JOIN context, use alias offsets to resolve qualified references
            if (self.join_alias_offsets) |offsets| {
                for (offsets) |ao| {
                    if (std.mem.eql(u8, qr.table, ao.alias) or std.mem.eql(u8, qr.table, ao.name)) {
                        if (ao.table.findColumnIndex(qr.column)) |ci| {
                            const idx = ao.offset + ci;
                            if (idx < row.values.len) {
                                const val = row.values[idx];
                                return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
                            }
                        }
                        break;
                    }
                }
            }
            // For non-JOIN context, just use the column name
            const col_idx = tbl.findColumnIndex(qr.column) orelse return .null_val;
            const val = row.values[col_idx];
            return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
        },
        .binary_op => |bin| {
            // AND/OR: short-circuit evaluation
            if (bin.op == .logical_and) {
                const left_val = try evalExpr(self, bin.left, tbl, row);
                defer if (left_val == .text) self.allocator.free(left_val.text);
                const left_true = self.valueToBool(left_val);
                if (!left_true) return .{ .integer = 0 };
                const right_val = try evalExpr(self, bin.right, tbl, row);
                defer if (right_val == .text) self.allocator.free(right_val.text);
                return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
            }
            if (bin.op == .logical_or) {
                const left_val = try evalExpr(self, bin.left, tbl, row);
                defer if (left_val == .text) self.allocator.free(left_val.text);
                const left_true = self.valueToBool(left_val);
                if (left_true) return .{ .integer = 1 };
                const right_val = try evalExpr(self, bin.right, tbl, row);
                defer if (right_val == .text) self.allocator.free(right_val.text);
                return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
            }

            const left_val = try evalExpr(self, bin.left, tbl, row);
            const right_val = try evalExpr(self, bin.right, tbl, row);
            defer {
                if (left_val == .text) self.allocator.free(left_val.text);
                if (right_val == .text) self.allocator.free(right_val.text);
            }

            // For comparison operators, NULL handling is special
            if (bin.op == .eq or bin.op == .ne or bin.op == .lt or bin.op == .le or bin.op == .gt or bin.op == .ge) {
                if (left_val == .null_val or right_val == .null_val) return .null_val;

                // Perform comparison and return 0 (false) or 1 (true)
                const cmp = compareValuesOrder(left_val, right_val);
                const cmp_result = switch (bin.op) {
                    .eq => cmp == .eq,
                    .ne => cmp != .eq,
                    .lt => cmp == .lt,
                    .le => cmp == .lt or cmp == .eq,
                    .gt => cmp == .gt,
                    .ge => cmp == .gt or cmp == .eq,
                    else => false,
                };
                return .{ .integer = if (cmp_result) 1 else 0 };
            }

            // LIKE operator
            if (bin.op == .like) {
                if (left_val == .null_val or right_val == .null_val) return .null_val;

                const left_text = self.valueToText(left_val);
                defer self.allocator.free(left_text);
                const right_text = self.valueToText(right_val);
                defer self.allocator.free(right_text);

                const matches = try likeMatch(left_text, right_text);
                return .{ .integer = if (matches) 1 else 0 };
            }

            // GLOB operator (case-sensitive)
            if (bin.op == .glob) {
                if (left_val == .null_val or right_val == .null_val) return .null_val;

                const left_text = self.valueToText(left_val);
                defer self.allocator.free(left_text);
                const right_text = self.valueToText(right_val);
                defer self.allocator.free(right_text);

                const matches = try globMatch(left_text, right_text);
                return .{ .integer = if (matches) 1 else 0 };
            }

            // For other operators, NULL propagation applies
            if (left_val == .null_val or right_val == .null_val) return .null_val;

            switch (bin.op) {
                .concat => {
                    // String concatenation
                    const l = self.valueToText(left_val);
                    defer self.allocator.free(l);
                    const r = self.valueToText(right_val);
                    defer self.allocator.free(r);
                    const result = self.allocator.alloc(u8, l.len + r.len) catch return .null_val;
                    @memcpy(result[0..l.len], l);
                    @memcpy(result[l.len..], r);
                    return .{ .text = result };
                },
                .add, .sub, .mul, .div, .mod => {
                    // Use float arithmetic if either operand is a float
                    if (self.isFloatValue(left_val) or self.isFloatValue(right_val)) {
                        const l = self.valueToF64(left_val) orelse return .null_val;
                        const r = self.valueToF64(right_val) orelse return .null_val;
                        const result: f64 = switch (bin.op) {
                            .add => l + r,
                            .sub => l - r,
                            .mul => l * r,
                            .div => if (r == 0.0) return .null_val else l / r,
                            .mod => if (r == 0.0) return .null_val else @rem(l, r),
                            else => unreachable,
                        };
                        return self.formatFloat(result);
                    }
                    const l = self.valueToI64(left_val);
                    const r = self.valueToI64(right_val);
                    if (l == null or r == null) return .null_val;
                    return .{ .integer = switch (bin.op) {
                        .add => l.? + r.?,
                        .sub => l.? - r.?,
                        .mul => l.? * r.?,
                        .div => if (r.? == 0) return .null_val else @divTrunc(l.?, r.?),
                        .mod => if (r.? == 0) return .null_val else @rem(l.?, r.?),
                        else => unreachable,
                    } };
                },
                .bit_and, .bit_or, .left_shift, .right_shift => {
                    const l = self.valueToI64(left_val);
                    const r = self.valueToI64(right_val);
                    if (l == null or r == null) return .null_val;
                    return .{ .integer = switch (bin.op) {
                        .bit_and => l.? & r.?,
                        .bit_or => l.? | r.?,
                        .left_shift => if (r.? < 0 or r.? > 63) 0 else l.? << @intCast(@as(u6, @truncate(@as(u64, @bitCast(r.?))))),
                        .right_shift => if (r.? < 0 or r.? > 63) 0 else l.? >> @intCast(@as(u6, @truncate(@as(u64, @bitCast(r.?))))),
                        else => unreachable,
                    } };
                },
                .eq, .ne, .lt, .le, .gt, .ge, .like, .glob, .logical_and, .logical_or => unreachable,
            }
        },
        .aggregate => {
            // Aggregate expressions are evaluated differently (in computeAgg)
            return .null_val;
        },
        .window_func => {
            // Window functions are evaluated in post-processing (evaluateWindowFunctions)
            return .{ .integer = 0 }; // placeholder
        },
        .case_when => |cw| {
            // Evaluate CASE WHEN: find first condition that is true
            for (cw.conditions, cw.results) |cond_expr, result_expr| {
                const cond_val = try evalExpr(self, cond_expr, tbl, row);
                defer if (cond_val == .text) self.allocator.free(cond_val.text);

                // Non-zero and non-NULL is true
                const is_true = switch (cond_val) {
                    .integer => |n| n != 0,
                    else => false,
                };

                if (is_true) {
                    return try evalExpr(self, result_expr, tbl, row);
                }
            }

            // No condition matched, evaluate ELSE if present
            if (cw.else_result) |else_expr| {
                return try evalExpr(self, else_expr, tbl, row);
            }

            // No ELSE and no match -> NULL
            return .null_val;
        },
        .unary_op => |u| {
            const operand_val = try evalExpr(self, u.operand, tbl, row);
            defer if (operand_val == .text) self.allocator.free(operand_val.text);
            return switch (u.op) {
                .is_null => .{ .integer = if (operand_val == .null_val) @as(i64, 1) else 0 },
                .is_not_null => .{ .integer = if (operand_val != .null_val) @as(i64, 1) else 0 },
                .not => {
                    if (operand_val == .null_val) return .null_val;
                    return .{ .integer = if (self.valueToBool(operand_val)) @as(i64, 0) else 1 };
                },
                .bit_not => {
                    if (operand_val == .null_val) return .null_val;
                    const v = self.valueToI64(operand_val) orelse return .null_val;
                    return .{ .integer = ~v };
                },
            };
        },
        .in_list => |il| {
            const operand_val = try evalExpr(self, il.operand, tbl, row);
            defer if (operand_val == .text) self.allocator.free(operand_val.text);
            if (operand_val == .null_val) return .null_val;

            // Set outer context for correlated subqueries
            const saved_outer = self.outer_context;
            self.outer_context = .{ .table = tbl, .row = row, .table_name = tbl.name, .table_alias = self.current_table_alias };
            defer self.outer_context = saved_outer;

            const sub_values = try self.executeSubquery(il.subquery_sql);
            defer if (sub_values) |sv| {
                for (sv) |v| {
                    if (v == .text) self.allocator.free(v.text);
                }
                self.allocator.free(sv);
            };

            if (sub_values == null) return .{ .integer = 0 };
            const sv = sub_values.?;
            for (sv) |sub_val| {
                if (Table.compareValues(operand_val, sub_val, .eq)) return .{ .integer = 1 };
            }
            return .{ .integer = 0 };
        },
        .in_values => |iv| {
            const operand_val = try evalExpr(self, iv.operand, tbl, row);
            defer if (operand_val == .text) self.allocator.free(operand_val.text);
            if (operand_val == .null_val) return .null_val;

            for (iv.values) |val_expr| {
                const val = try evalExpr(self, val_expr, tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (self.valuesEqualUnion(operand_val, val)) return .{ .integer = 1 };
            }
            return .{ .integer = 0 };
        },
        .scalar_subquery => |sq| {
            // Set outer context for correlated subqueries
            const saved_outer = self.outer_context;
            self.outer_context = .{ .table = tbl, .row = row, .table_name = tbl.name, .table_alias = self.current_table_alias };
            defer self.outer_context = saved_outer;

            const sub_values = try self.executeSubquery(sq.subquery_sql);
            defer if (sub_values) |sv| {
                for (sv) |v| {
                    if (v == .text) self.allocator.free(v.text);
                }
                self.allocator.free(sv);
            };

            if (sub_values == null or sub_values.?.len == 0) return .null_val;
            const result = sub_values.?[0];
            // Deep copy text to let caller free uniformly
            return if (result == .text) .{ .text = try dupeStr(self.allocator, result.text) } else result;
        },
        .exists => |ex| {
            // Set outer context for correlated subqueries
            const saved_outer = self.outer_context;
            self.outer_context = .{ .table = tbl, .row = row, .table_name = tbl.name, .table_alias = self.current_table_alias };
            defer self.outer_context = saved_outer;

            const sub_values = try self.executeSubquery(ex.subquery_sql);
            defer if (sub_values) |sv| {
                for (sv) |v| {
                    if (v == .text) self.allocator.free(v.text);
                }
                self.allocator.free(sv);
            };
            const has_rows = sub_values != null and sub_values.?.len > 0;
            const result = if (ex.negated) !has_rows else has_rows;
            return .{ .integer = if (result) 1 else 0 };
        },
        .scalar_func => |sf| {
            return evalScalarFunc(self, sf.func, sf.args, tbl, row);
        },
        .cast => |c| {
            const val = try evalExpr(self, c.operand, tbl, row);
            switch (c.target_type) {
                .integer => {
                    if (val == .integer) return val;
                    if (val == .null_val) return .null_val;
                    // text -> integer
                    const text = val.text;
                    defer self.allocator.free(text);
                    const n = std.fmt.parseInt(i64, text, 10) catch return .{ .integer = 0 };
                    return .{ .integer = n };
                },
                .text => {
                    if (val == .text) return val;
                    if (val == .null_val) return .null_val;
                    // integer -> text
                    const text = self.valueToText(val);
                    return .{ .text = text };
                },
            }
        },
    }
}

/// Evaluate an expression in GROUP BY context where aggregates are computed over group rows.
pub fn evalGroupExpr(self: *Database, expr: *const Expr, tbl: *const Table, group_rows: []const Row) !Value {
    switch (expr.*) {
        .aggregate => |agg| {
            const arg_name: []const u8 = switch (agg.arg.*) {
                .column_ref => |name| name,
                .qualified_ref => |qr| blk: {
                    // Use pointer arithmetic to get "table.column" as source slice
                    const start = qr.table.ptr;
                    const end = qr.column.ptr + qr.column.len;
                    break :blk start[0..@intFromPtr(end) - @intFromPtr(start)];
                },
                .star => "*",
                else => return .null_val,
            };
            return self.computeAgg(agg.func, arg_name, tbl, group_rows, agg.separator, agg.distinct);
        },
        .binary_op => |bin| {
            // Short-circuit for logical ops
            if (bin.op == .logical_and) {
                const left_val = try evalGroupExpr(self, bin.left, tbl, group_rows);
                defer if (left_val == .text) self.allocator.free(left_val.text);
                if (!self.valueToBool(left_val)) return .{ .integer = 0 };
                const right_val = try evalGroupExpr(self, bin.right, tbl, group_rows);
                defer if (right_val == .text) self.allocator.free(right_val.text);
                return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
            }
            if (bin.op == .logical_or) {
                const left_val = try evalGroupExpr(self, bin.left, tbl, group_rows);
                defer if (left_val == .text) self.allocator.free(left_val.text);
                if (self.valueToBool(left_val)) return .{ .integer = 1 };
                const right_val = try evalGroupExpr(self, bin.right, tbl, group_rows);
                defer if (right_val == .text) self.allocator.free(right_val.text);
                return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
            }

            const left_val = try evalGroupExpr(self, bin.left, tbl, group_rows);
            const right_val = try evalGroupExpr(self, bin.right, tbl, group_rows);
            defer {
                if (left_val == .text) self.allocator.free(left_val.text);
                if (right_val == .text) self.allocator.free(right_val.text);
            }

            // Comparison operators
            if (bin.op == .eq or bin.op == .ne or bin.op == .lt or bin.op == .le or bin.op == .gt or bin.op == .ge) {
                if (left_val == .null_val or right_val == .null_val) return .null_val;
                const cmp = compareValuesOrder(left_val, right_val);
                const cmp_result = switch (bin.op) {
                    .eq => cmp == .eq,
                    .ne => cmp != .eq,
                    .lt => cmp == .lt,
                    .le => cmp == .lt or cmp == .eq,
                    .gt => cmp == .gt,
                    .ge => cmp == .gt or cmp == .eq,
                    else => false,
                };
                return .{ .integer = if (cmp_result) 1 else 0 };
            }

            if (bin.op == .concat) {
                if (left_val == .null_val or right_val == .null_val) return .null_val;
                const l = self.valueToText(left_val);
                defer self.allocator.free(l);
                const r = self.valueToText(right_val);
                defer self.allocator.free(r);
                const result = self.allocator.alloc(u8, l.len + r.len) catch return .null_val;
                @memcpy(result[0..l.len], l);
                @memcpy(result[l.len..], r);
                return .{ .text = result };
            }

            if (left_val == .null_val or right_val == .null_val) return .null_val;

            switch (bin.op) {
                .add, .sub, .mul, .div, .mod => {
                    // Use float arithmetic if either operand is a float
                    if (self.isFloatValue(left_val) or self.isFloatValue(right_val)) {
                        const l = self.valueToF64(left_val) orelse return .null_val;
                        const r = self.valueToF64(right_val) orelse return .null_val;
                        const result: f64 = switch (bin.op) {
                            .add => l + r,
                            .sub => l - r,
                            .mul => l * r,
                            .div => if (r == 0.0) return .null_val else l / r,
                            .mod => if (r == 0.0) return .null_val else @rem(l, r),
                            else => unreachable,
                        };
                        return self.formatFloat(result);
                    }
                    const l = self.valueToI64(left_val);
                    const r = self.valueToI64(right_val);
                    if (l == null or r == null) return .null_val;
                    return .{ .integer = switch (bin.op) {
                        .add => l.? + r.?,
                        .sub => l.? - r.?,
                        .mul => l.? * r.?,
                        .div => if (r.? == 0) return .null_val else @divTrunc(l.?, r.?),
                        .mod => if (r.? == 0) return .null_val else @rem(l.?, r.?),
                        else => unreachable,
                    } };
                },
                .bit_and, .bit_or, .left_shift, .right_shift => {
                    const l = self.valueToI64(left_val);
                    const r = self.valueToI64(right_val);
                    if (l == null or r == null) return .null_val;
                    return .{ .integer = switch (bin.op) {
                        .bit_and => l.? & r.?,
                        .bit_or => l.? | r.?,
                        .left_shift => if (r.? < 0 or r.? > 63) 0 else l.? << @intCast(@as(u6, @truncate(@as(u64, @bitCast(r.?))))),
                        .right_shift => if (r.? < 0 or r.? > 63) 0 else l.? >> @intCast(@as(u6, @truncate(@as(u64, @bitCast(r.?))))),
                        else => unreachable,
                    } };
                },
                else => return .null_val,
            }
        },
        .scalar_func => |sf| {
            // Evaluate scalar function args via evalGroupExpr to handle nested aggregates
            var resolved_args = self.allocator.alloc(Value, sf.args.len) catch return .null_val;
            defer self.allocator.free(resolved_args);
            var n_resolved: usize = 0;
            defer {
                for (resolved_args[0..n_resolved]) |v| {
                    if (v == .text) self.allocator.free(v.text);
                }
            }
            for (sf.args, 0..) |arg, i| {
                resolved_args[i] = try evalGroupExpr(self, arg, tbl, group_rows);
                n_resolved = i + 1;
            }
            // Evaluate scalar function with resolved values
            return evalScalarFuncValues(self, sf.func, resolved_args[0..sf.args.len]);
        },
        .case_when => |cw| {
            for (cw.conditions, 0..) |cond, i| {
                const cond_val = try evalGroupExpr(self, cond, tbl, group_rows);
                defer if (cond_val == .text) self.allocator.free(cond_val.text);
                if (self.valueToBool(cond_val)) {
                    return evalGroupExpr(self, cw.results[i], tbl, group_rows);
                }
            }
            if (cw.else_result) |else_expr| {
                return evalGroupExpr(self, else_expr, tbl, group_rows);
            }
            return .null_val;
        },
        .unary_op => |un| {
            const operand_val = try evalGroupExpr(self, un.operand, tbl, group_rows);
            defer if (operand_val == .text) self.allocator.free(operand_val.text);
            if (operand_val == .null_val) return .null_val;
            switch (un.op) {
                .not => return .{ .integer = if (self.valueToBool(operand_val)) 0 else 1 },
                .bit_not => {
                    const n = self.valueToI64(operand_val) orelse return .null_val;
                    return .{ .integer = ~n };
                },
                .is_null => return .{ .integer = if (operand_val == .null_val) 1 else 0 },
                .is_not_null => return .{ .integer = if (operand_val != .null_val) 1 else 0 },
            }
        },
        else => {
            // For non-aggregate expressions, evaluate against the first row
            if (group_rows.len > 0) {
                return evalExpr(self, expr, tbl, group_rows[0]);
            }
            return .null_val;
        },
    }
}

pub fn evalScalarFunc(self: *Database, func: ScalarFunc, args: []const *const Expr, tbl: *const Table, row: Row) !Value {
    switch (func) {
        .abs => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            // Try integer first, then float
            if (self.valueToI64(val)) |n| {
                return .{ .integer = if (n < 0) -n else n };
            }
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@abs(f));
        },
        .length => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            if (val == .text) {
                const len: i64 = @intCast(val.text.len);
                self.allocator.free(val.text);
                return .{ .integer = len };
            }
            // For integers, convert to text first then measure length
            const text = self.valueToText(val);
            defer self.allocator.free(text);
            return .{ .integer = @intCast(text.len) };
        },
        .upper => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            defer if (val == .text) self.allocator.free(val.text);
            var buf = try self.allocator.alloc(u8, text.len);
            for (text, 0..) |ch, i| {
                buf[i] = std.ascii.toUpper(ch);
            }
            self.allocator.free(text);
            return .{ .text = buf };
        },
        .lower => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            defer if (val == .text) self.allocator.free(val.text);
            var buf = try self.allocator.alloc(u8, text.len);
            for (text, 0..) |ch, i| {
                buf[i] = std.ascii.toLower(ch);
            }
            self.allocator.free(text);
            return .{ .text = buf };
        },
        .trim => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            defer if (val == .text) self.allocator.free(val.text);
            const trimmed = std.mem.trim(u8, text, " ");
            if (trimmed.len == text.len) {
                return .{ .text = text };
            }
            const result = dupeStr(self.allocator, trimmed) catch return .null_val;
            self.allocator.free(text);
            return .{ .text = result };
        },
        .typeof_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            const type_name: []const u8 = switch (val) {
                .integer => "integer",
                .text => "text",
                .null_val => "null",
            };
            return .{ .text = try dupeStr(self.allocator, type_name) };
        },
        .coalesce => {
            for (args) |arg| {
                const val = try evalExpr(self, arg, tbl, row);
                if (val != .null_val) return val;
            }
            return .null_val;
        },
        .nullif => {
            if (args.len != 2) return .null_val;
            const v1 = try evalExpr(self, args[0], tbl, row);
            const v2 = try evalExpr(self, args[1], tbl, row);
            defer if (v2 == .text) self.allocator.free(v2.text);
            if (compareValuesOrder(v1, v2) == .eq) {
                if (v1 == .text) self.allocator.free(v1.text);
                return .null_val;
            }
            return v1;
        },
        .max_fn => {
            var best: ?Value = null;
            for (args) |arg| {
                const val = try evalExpr(self, arg, tbl, row);
                if (val == .null_val) {
                    continue;
                }
                if (best == null) {
                    best = val;
                } else {
                    if (compareValuesOrder(val, best.?) == .gt) {
                        if (best.? == .text) self.allocator.free(best.?.text);
                        best = val;
                    } else {
                        if (val == .text) self.allocator.free(val.text);
                    }
                }
            }
            return best orelse .null_val;
        },
        .min_fn => {
            var best: ?Value = null;
            for (args) |arg| {
                const val = try evalExpr(self, arg, tbl, row);
                if (val == .null_val) {
                    continue;
                }
                if (best == null) {
                    best = val;
                } else {
                    if (compareValuesOrder(val, best.?) == .lt) {
                        if (best.? == .text) self.allocator.free(best.?.text);
                        best = val;
                    } else {
                        if (val == .text) self.allocator.free(val.text);
                    }
                }
            }
            return best orelse .null_val;
        },
        .iif => {
            if (args.len != 3) return .null_val;
            const cond = try evalExpr(self, args[0], tbl, row);
            defer if (cond == .text) self.allocator.free(cond.text);
            if (self.valueToBool(cond)) {
                return try evalExpr(self, args[1], tbl, row);
            } else {
                return try evalExpr(self, args[2], tbl, row);
            }
        },
        .substr => {
            if (args.len < 2 or args.len > 3) return .null_val;
            const str_val = try evalExpr(self, args[0], tbl, row);
            if (str_val == .null_val) return .null_val;
            const text = self.valueToText(str_val);
            defer if (str_val == .text) self.allocator.free(str_val.text);
            const start_val = try evalExpr(self, args[1], tbl, row);
            defer if (start_val == .text) self.allocator.free(start_val.text);
            const start_raw = self.valueToI64(start_val) orelse {
                self.allocator.free(text);
                return .null_val;
            };
            // SQLite SUBSTR: 1-based positions, negative means from end
            // Negative length means chars before the position
            const text_len: i64 = @intCast(text.len);
            // Convert negative start: -N means Nth from end (1-based)
            const y: i64 = if (start_raw < 0) text_len + start_raw + 1 else start_raw;
            // Default length: rest of string from y
            var z: i64 = text_len - y + 1;
            if (args.len == 3) {
                const len_val = try evalExpr(self, args[2], tbl, row);
                defer if (len_val == .text) self.allocator.free(len_val.text);
                z = self.valueToI64(len_val) orelse {
                    self.allocator.free(text);
                    return .null_val;
                };
            }
            // Compute range [r_start, r_end) in 1-based coords
            var r_start: i64 = undefined;
            var r_end: i64 = undefined;
            if (z >= 0) {
                r_start = y;
                r_end = y + z;
            } else {
                r_start = y + z;
                r_end = y;
            }
            // Clamp to valid range [1, text_len+1)
            if (r_start < 1) r_start = 1;
            if (r_end > text_len + 1) r_end = text_len + 1;
            if (r_start >= r_end) {
                self.allocator.free(text);
                return .{ .text = try dupeStr(self.allocator, "") };
            }
            // Convert to 0-based
            const s: usize = @intCast(r_start - 1);
            const e: usize = @intCast(r_end - 1);
            const result = dupeStr(self.allocator, text[s..e]) catch return .null_val;
            self.allocator.free(text);
            return .{ .text = result };
        },
        .instr => {
            if (args.len != 2) return .null_val;
            const str_val = try evalExpr(self, args[0], tbl, row);
            if (str_val == .null_val) return .null_val;
            const text = self.valueToText(str_val);
            defer if (str_val == .text) self.allocator.free(str_val.text);
            const sub_val = try evalExpr(self, args[1], tbl, row);
            if (sub_val == .null_val) {
                self.allocator.free(text);
                return .null_val;
            }
            const sub = self.valueToText(sub_val);
            defer if (sub_val == .text) self.allocator.free(sub_val.text);
            if (std.mem.indexOf(u8, text, sub)) |pos| {
                self.allocator.free(text);
                self.allocator.free(sub);
                return .{ .integer = @as(i64, @intCast(pos)) + 1 }; // 1-based
            }
            self.allocator.free(text);
            self.allocator.free(sub);
            return .{ .integer = 0 };
        },
        .replace_fn => {
            if (args.len != 3) return .null_val;
            const str_val = try evalExpr(self, args[0], tbl, row);
            if (str_val == .null_val) return .null_val;
            const text = self.valueToText(str_val);
            defer if (str_val == .text) self.allocator.free(str_val.text);
            const from_val = try evalExpr(self, args[1], tbl, row);
            if (from_val == .null_val) {
                self.allocator.free(text);
                return .null_val;
            }
            const from = self.valueToText(from_val);
            defer if (from_val == .text) self.allocator.free(from_val.text);
            const to_val = try evalExpr(self, args[2], tbl, row);
            if (to_val == .null_val) {
                self.allocator.free(text);
                self.allocator.free(from);
                return .null_val;
            }
            const to = self.valueToText(to_val);
            defer if (to_val == .text) self.allocator.free(to_val.text);
            const result = std.mem.replaceOwned(u8, self.allocator, text, from, to) catch {
                self.allocator.free(text);
                self.allocator.free(from);
                self.allocator.free(to);
                return .null_val;
            };
            self.allocator.free(text);
            self.allocator.free(from);
            self.allocator.free(to);
            return .{ .text = result };
        },
        .hex => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            defer if (val == .text) self.allocator.free(val.text);
            // Convert each byte to 2-char hex
            var hex_buf = self.allocator.alloc(u8, text.len * 2) catch {
                self.allocator.free(text);
                return .null_val;
            };
            for (text, 0..) |byte, i| {
                const hex_chars = "0123456789ABCDEF";
                hex_buf[i * 2] = hex_chars[byte >> 4];
                hex_buf[i * 2 + 1] = hex_chars[byte & 0x0F];
            }
            self.allocator.free(text);
            return .{ .text = hex_buf };
        },
        .unicode_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            defer if (val == .text) self.allocator.free(val.text);
            defer self.allocator.free(text);
            if (text.len == 0) return .null_val;
            return .{ .integer = @intCast(text[0]) };
        },
        .char_fn => {
            var result: std.ArrayList(u8) = .{};
            for (args) |arg| {
                const val = try evalExpr(self, arg, tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                const n = self.valueToI64(val) orelse continue;
                if (n >= 0 and n <= 127) {
                    result.append(self.allocator, @intCast(@as(u64, @intCast(n)))) catch return .null_val;
                }
            }
            return .{ .text = result.toOwnedSlice(self.allocator) catch return .null_val };
        },
        .zeroblob => {
            // ZEROBLOB(n) returns n zero bytes - simplified as text of n zeroes
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            const n = self.valueToI64(val) orelse return .null_val;
            if (n <= 0) return .{ .text = try dupeStr(self.allocator, "") };
            const buf = self.allocator.alloc(u8, @intCast(n)) catch return .null_val;
            @memset(buf, 0);
            return .{ .text = buf };
        },
        .printf_fn => {
            // Simplified PRINTF: just evaluate and return as text
            if (args.len == 0) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .text) return val;
            if (val == .null_val) return .null_val;
            return .{ .text = self.valueToText(val) };
        },
        .ltrim => {
            if (args.len == 0) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            if (val != .text) return val;
            defer self.allocator.free(val.text);
            const trimmed = std.mem.trimLeft(u8, val.text, " ");
            return .{ .text = try dupeStr(self.allocator, trimmed) };
        },
        .rtrim => {
            if (args.len == 0) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            if (val == .null_val) return .null_val;
            if (val != .text) return val;
            defer self.allocator.free(val.text);
            const trimmed = std.mem.trimRight(u8, val.text, " ");
            return .{ .text = try dupeStr(self.allocator, trimmed) };
        },
        .round => {
            // ROUND(x) or ROUND(x, n) — always returns real in SQLite
            if (args.len == 0 or args.len > 2) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const float_val = self.valueToF64(val) orelse return .null_val;
            var precision: i64 = 0; // default: round to integer
            if (args.len == 2) {
                const prec_val = try evalExpr(self, args[1], tbl, row);
                defer if (prec_val == .text) self.allocator.free(prec_val.text);
                precision = self.valueToI64(prec_val) orelse 0;
            }
            const factor = std.math.pow(f64, 10.0, @as(f64, @floatFromInt(precision)));
            const rounded = @round(float_val * factor) / factor;
            return self.formatFloat(rounded);
        },
        .ifnull => {
            if (args.len != 2) return .null_val;
            const v1 = try evalExpr(self, args[0], tbl, row);
            if (v1 != .null_val) return v1;
            return try evalExpr(self, args[1], tbl, row);
        },
        .random => {
            // RANDOM() returns a random integer (simplified: use timestamp-based)
            const seed: u64 = @bitCast(std.time.milliTimestamp());
            var rng = std.Random.DefaultPrng.init(seed);
            const val = rng.random().int(i64);
            return .{ .integer = val };
        },
        .sign => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            if (self.valueToI64(val)) |n| {
                return .{ .integer = if (n > 0) @as(i64, 1) else if (n < 0) @as(i64, -1) else 0 };
            }
            const f = self.valueToF64(val) orelse return .null_val;
            return .{ .integer = if (f > 0) @as(i64, 1) else if (f < 0) @as(i64, -1) else 0 };
        },
        .date_fn => {
            return evalDateTimeFunc(self, args, tbl, row, .date_only);
        },
        .time_fn => {
            return evalDateTimeFunc(self, args, tbl, row, .time_only);
        },
        .datetime_fn => {
            return evalDateTimeFunc(self, args, tbl, row, .datetime);
        },
        .strftime_fn => {
            return evalStrftime(self, args, tbl, row);
        },
        .like_fn => {
            // LIKE(pattern, string) -> 1 if matches, 0 otherwise
            if (args.len < 2) return .null_val;
            const pattern_val = try evalExpr(self, args[0], tbl, row);
            defer if (pattern_val == .text) self.allocator.free(pattern_val.text);
            const string_val = try evalExpr(self, args[1], tbl, row);
            defer if (string_val == .text) self.allocator.free(string_val.text);
            if (pattern_val == .null_val or string_val == .null_val) return .null_val;
            const pattern_text = self.valueToText(pattern_val);
            defer self.allocator.free(pattern_text);
            const string_text = self.valueToText(string_val);
            defer self.allocator.free(string_text);
            const matches = try likeMatch(string_text, pattern_text);
            return .{ .integer = if (matches) 1 else 0 };
        },
        .glob_fn => {
            // GLOB(pattern, string) -> 1 if matches, 0 otherwise
            if (args.len < 2) return .null_val;
            const pattern_val = try evalExpr(self, args[0], tbl, row);
            defer if (pattern_val == .text) self.allocator.free(pattern_val.text);
            const string_val = try evalExpr(self, args[1], tbl, row);
            defer if (string_val == .text) self.allocator.free(string_val.text);
            if (pattern_val == .null_val or string_val == .null_val) return .null_val;
            const pattern_text = self.valueToText(pattern_val);
            defer self.allocator.free(pattern_text);
            const string_text = self.valueToText(string_val);
            defer self.allocator.free(string_text);
            const matches = try globMatch(string_text, pattern_text);
            return .{ .integer = if (matches) 1 else 0 };
        },
        .ceil_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            if (val == .integer) return val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@ceil(f));
        },
        .floor_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            if (val == .integer) return val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@floor(f));
        },
        .sqrt_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            if (f < 0) return .null_val;
            return self.formatFloat(@sqrt(f));
        },
        .power_fn => {
            if (args.len != 2) return .null_val;
            const base_val = try evalExpr(self, args[0], tbl, row);
            defer if (base_val == .text) self.allocator.free(base_val.text);
            const exp_val = try evalExpr(self, args[1], tbl, row);
            defer if (exp_val == .text) self.allocator.free(exp_val.text);
            if (base_val == .null_val or exp_val == .null_val) return .null_val;
            const base_f = self.valueToF64(base_val) orelse return .null_val;
            const exp_f = self.valueToF64(exp_val) orelse return .null_val;
            return self.formatFloat(std.math.pow(f64, base_f, exp_f));
        },
        .log_fn => {
            // LOG(x) = log10(x), LOG(base, x) = log_base(x)
            if (args.len == 1) {
                const val = try evalExpr(self, args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                if (f <= 0) return .null_val;
                return self.formatFloat(@log10(f));
            } else if (args.len == 2) {
                const base_val = try evalExpr(self, args[0], tbl, row);
                defer if (base_val == .text) self.allocator.free(base_val.text);
                const x_val = try evalExpr(self, args[1], tbl, row);
                defer if (x_val == .text) self.allocator.free(x_val.text);
                if (base_val == .null_val or x_val == .null_val) return .null_val;
                const base_f = self.valueToF64(base_val) orelse return .null_val;
                const x_f = self.valueToF64(x_val) orelse return .null_val;
                if (base_f <= 0 or base_f == 1 or x_f <= 0) return .null_val;
                return self.formatFloat(@log(x_f) / @log(base_f));
            }
            return .null_val;
        },
        .log2_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            if (f <= 0) return .null_val;
            return self.formatFloat(@log2(f));
        },
        .ln_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            if (f <= 0) return .null_val;
            return self.formatFloat(@log(f));
        },
        .exp_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@exp(f));
        },
        .acos_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            if (f < -1 or f > 1) return .null_val;
            return self.formatFloat(std.math.acos(f));
        },
        .asin_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            if (f < -1 or f > 1) return .null_val;
            return self.formatFloat(std.math.asin(f));
        },
        .atan_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(std.math.atan(f));
        },
        .atan2_fn => {
            if (args.len != 2) return .null_val;
            const y_val = try evalExpr(self, args[0], tbl, row);
            defer if (y_val == .text) self.allocator.free(y_val.text);
            const x_val = try evalExpr(self, args[1], tbl, row);
            defer if (x_val == .text) self.allocator.free(x_val.text);
            if (y_val == .null_val or x_val == .null_val) return .null_val;
            const y = self.valueToF64(y_val) orelse return .null_val;
            const x = self.valueToF64(x_val) orelse return .null_val;
            return self.formatFloat(std.math.atan2(y, x));
        },
        .cos_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@cos(f));
        },
        .sin_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@sin(f));
        },
        .tan_fn => {
            if (args.len != 1) return .null_val;
            const val = try evalExpr(self, args[0], tbl, row);
            defer if (val == .text) self.allocator.free(val.text);
            if (val == .null_val) return .null_val;
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@tan(f));
        },
        .pi_fn => {
            return self.formatFloat(std.math.pi);
        },
        .mod_fn => {
            if (args.len != 2) return .null_val;
            const a_val = try evalExpr(self, args[0], tbl, row);
            defer if (a_val == .text) self.allocator.free(a_val.text);
            const b_val = try evalExpr(self, args[1], tbl, row);
            defer if (b_val == .text) self.allocator.free(b_val.text);
            if (a_val == .null_val or b_val == .null_val) return .null_val;
            const a = self.valueToF64(a_val) orelse return .null_val;
            const b = self.valueToF64(b_val) orelse return .null_val;
            if (b == 0) return .null_val;
            return self.formatFloat(@mod(a, b));
        },
    }
}

/// Evaluate a scalar function with pre-resolved Value arguments.
/// Does NOT free the input values -- caller is responsible for cleanup.
/// Returns a new Value (caller owns returned text).
pub fn evalScalarFuncValues(self: *Database, func: ScalarFunc, vals: []const Value) !Value {
    switch (func) {
        .abs => {
            if (vals.len != 1) return .null_val;
            const val = vals[0];
            if (val == .null_val) return .null_val;
            if (self.valueToI64(val)) |n| {
                return .{ .integer = if (n < 0) -n else n };
            }
            const f = self.valueToF64(val) orelse return .null_val;
            return self.formatFloat(@abs(f));
        },
        .round => {
            if (vals.len == 0 or vals.len > 2) return .null_val;
            const val = vals[0];
            if (val == .null_val) return .null_val;
            const float_val = self.valueToF64(val) orelse return .null_val;
            var precision: i64 = 0;
            if (vals.len == 2) {
                precision = self.valueToI64(vals[1]) orelse 0;
            }
            const factor = std.math.pow(f64, 10.0, @as(f64, @floatFromInt(precision)));
            const rounded = @round(float_val * factor) / factor;
            return self.formatFloat(rounded);
        },
        .length => {
            if (vals.len != 1) return .null_val;
            const val = vals[0];
            if (val == .null_val) return .null_val;
            if (val == .text) return .{ .integer = @intCast(val.text.len) };
            const text = self.valueToText(val);
            defer self.allocator.free(text);
            return .{ .integer = @intCast(text.len) };
        },
        .upper => {
            if (vals.len != 1) return .null_val;
            const val = vals[0];
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            var buf = try self.allocator.alloc(u8, text.len);
            for (text, 0..) |ch, i| buf[i] = std.ascii.toUpper(ch);
            self.allocator.free(text);
            return .{ .text = buf };
        },
        .lower => {
            if (vals.len != 1) return .null_val;
            const val = vals[0];
            if (val == .null_val) return .null_val;
            const text = self.valueToText(val);
            var buf = try self.allocator.alloc(u8, text.len);
            for (text, 0..) |ch, i| buf[i] = std.ascii.toLower(ch);
            self.allocator.free(text);
            return .{ .text = buf };
        },
        .coalesce => {
            for (vals) |val| {
                if (val != .null_val) {
                    // Duplicate the value since caller will free it
                    if (val == .text) return .{ .text = try dupeStr(self.allocator, val.text) };
                    return val;
                }
            }
            return .null_val;
        },
        .nullif => {
            if (vals.len != 2) return .null_val;
            if (compareValuesOrder(vals[0], vals[1]) == .eq) return .null_val;
            if (vals[0] == .text) return .{ .text = try dupeStr(self.allocator, vals[0].text) };
            return vals[0];
        },
        .iif => {
            if (vals.len != 3) return .null_val;
            const idx: usize = if (self.valueToBool(vals[0])) 1 else 2;
            if (vals[idx] == .text) return .{ .text = try dupeStr(self.allocator, vals[idx].text) };
            return vals[idx];
        },
        .ifnull => {
            if (vals.len != 2) return .null_val;
            const idx: usize = if (vals[0] != .null_val) 0 else 1;
            if (vals[idx] == .text) return .{ .text = try dupeStr(self.allocator, vals[idx].text) };
            return vals[idx];
        },
        .typeof_fn => {
            if (vals.len != 1) return .null_val;
            const type_name: []const u8 = switch (vals[0]) {
                .integer => "integer",
                .text => "text",
                .null_val => "null",
            };
            return .{ .text = try dupeStr(self.allocator, type_name) };
        },
        .sign => {
            if (vals.len != 1) return .null_val;
            const val = vals[0];
            if (val == .null_val) return .null_val;
            if (self.valueToI64(val)) |n| {
                return .{ .integer = if (n > 0) @as(i64, 1) else if (n < 0) @as(i64, -1) else 0 };
            }
            const f = self.valueToF64(val) orelse return .null_val;
            return .{ .integer = if (f > 0) @as(i64, 1) else if (f < 0) @as(i64, -1) else 0 };
        },
        .max_fn => {
            var best: ?Value = null;
            for (vals) |val| {
                if (val == .null_val) continue;
                if (best == null or compareValuesOrder(val, best.?) == .gt) best = val;
            }
            if (best) |b| {
                if (b == .text) return .{ .text = try dupeStr(self.allocator, b.text) };
                return b;
            }
            return .null_val;
        },
        .min_fn => {
            var best: ?Value = null;
            for (vals) |val| {
                if (val == .null_val) continue;
                if (best == null or compareValuesOrder(val, best.?) == .lt) best = val;
            }
            if (best) |b| {
                if (b == .text) return .{ .text = try dupeStr(self.allocator, b.text) };
                return b;
            }
            return .null_val;
        },
        .trim => {
            if (vals.len != 1) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            const text = self.valueToText(vals[0]);
            const trimmed = std.mem.trim(u8, text, " ");
            if (trimmed.len == text.len) return .{ .text = text };
            const result = dupeStr(self.allocator, trimmed) catch {
                self.allocator.free(text);
                return .null_val;
            };
            self.allocator.free(text);
            return .{ .text = result };
        },
        .ltrim => {
            if (vals.len == 0) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            const text = self.valueToText(vals[0]);
            const trimmed = std.mem.trimLeft(u8, text, " ");
            const result = dupeStr(self.allocator, trimmed) catch {
                self.allocator.free(text);
                return .null_val;
            };
            self.allocator.free(text);
            return .{ .text = result };
        },
        .rtrim => {
            if (vals.len == 0) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            const text = self.valueToText(vals[0]);
            const trimmed = std.mem.trimRight(u8, text, " ");
            const result = dupeStr(self.allocator, trimmed) catch {
                self.allocator.free(text);
                return .null_val;
            };
            self.allocator.free(text);
            return .{ .text = result };
        },
        .ceil_fn => {
            if (vals.len != 1) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            if (vals[0] == .integer) return vals[0];
            const f = self.valueToF64(vals[0]) orelse return .null_val;
            return self.formatFloat(@ceil(f));
        },
        .floor_fn => {
            if (vals.len != 1) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            if (vals[0] == .integer) return vals[0];
            const f = self.valueToF64(vals[0]) orelse return .null_val;
            return self.formatFloat(@floor(f));
        },
        .sqrt_fn => {
            if (vals.len != 1) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            const f = self.valueToF64(vals[0]) orelse return .null_val;
            if (f < 0) return .null_val;
            return self.formatFloat(@sqrt(f));
        },
        .power_fn => {
            if (vals.len != 2) return .null_val;
            if (vals[0] == .null_val or vals[1] == .null_val) return .null_val;
            const base_f = self.valueToF64(vals[0]) orelse return .null_val;
            const exp_f = self.valueToF64(vals[1]) orelse return .null_val;
            return self.formatFloat(std.math.pow(f64, base_f, exp_f));
        },
        .substr => {
            if (vals.len < 2 or vals.len > 3) return .null_val;
            if (vals[0] == .null_val) return .null_val;
            const text = self.valueToText(vals[0]);
            const start_raw = self.valueToI64(vals[1]) orelse {
                self.allocator.free(text);
                return .null_val;
            };
            const text_len: i64 = @intCast(text.len);
            const y: i64 = if (start_raw < 0) text_len + start_raw + 1 else start_raw;
            var z: i64 = text_len - y + 1;
            if (vals.len == 3) z = self.valueToI64(vals[2]) orelse {
                self.allocator.free(text);
                return .null_val;
            };
            var r_start: i64 = undefined;
            var r_end: i64 = undefined;
            if (z >= 0) {
                r_start = y;
                r_end = y + z;
            } else {
                r_start = y + z;
                r_end = y;
            }
            if (r_start < 1) r_start = 1;
            if (r_end > text_len + 1) r_end = text_len + 1;
            if (r_start >= r_end) {
                self.allocator.free(text);
                return .{ .text = try dupeStr(self.allocator, "") };
            }
            const s: usize = @intCast(r_start - 1);
            const e: usize = @intCast(r_end - 1);
            const result = dupeStr(self.allocator, text[s..e]) catch {
                self.allocator.free(text);
                return .null_val;
            };
            self.allocator.free(text);
            return .{ .text = result };
        },
        .instr => {
            if (vals.len != 2) return .null_val;
            if (vals[0] == .null_val or vals[1] == .null_val) return .null_val;
            const text = self.valueToText(vals[0]);
            defer self.allocator.free(text);
            const sub = self.valueToText(vals[1]);
            defer self.allocator.free(sub);
            if (std.mem.indexOf(u8, text, sub)) |pos| return .{ .integer = @as(i64, @intCast(pos)) + 1 };
            return .{ .integer = 0 };
        },
        .replace_fn => {
            if (vals.len != 3) return .null_val;
            if (vals[0] == .null_val or vals[1] == .null_val or vals[2] == .null_val) return .null_val;
            const text = self.valueToText(vals[0]);
            const from = self.valueToText(vals[1]);
            const to = self.valueToText(vals[2]);
            const result = std.mem.replaceOwned(u8, self.allocator, text, from, to) catch {
                self.allocator.free(text);
                self.allocator.free(from);
                self.allocator.free(to);
                return .null_val;
            };
            self.allocator.free(text);
            self.allocator.free(from);
            self.allocator.free(to);
            return .{ .text = result };
        },
        .random => {
            const seed: u64 = @bitCast(std.time.milliTimestamp());
            var rng = std.Random.DefaultPrng.init(seed);
            return .{ .integer = rng.random().int(i64) };
        },
        .pi_fn => return self.formatFloat(std.math.pi),
        else => return .null_val, // Unsupported functions in aggregate context
    }
}

pub const DateTimePart = enum { date_only, time_only, datetime };

/// Components of a parsed date/time
pub const DateTimeComponents = struct {
    year: u32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
};

/// Parse a date/time string or 'now' into components
pub fn parseDateTimeInput(self: *Database, text: []const u8) ?DateTimeComponents {
    _ = self;
    // Handle 'now'
    if (std.ascii.eqlIgnoreCase(text, "now")) {
        const ts: u64 = @intCast(std.time.timestamp());
        const es = std.time.epoch.EpochSeconds{ .secs = ts };
        const day_secs = es.getDaySeconds();
        const epoch_day = es.getEpochDay();
        const year_day = epoch_day.calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        return .{
            .year = year_day.year,
            .month = @intFromEnum(month_day.month),
            .day = month_day.day_index + 1,
            .hour = day_secs.getHoursIntoDay(),
            .minute = day_secs.getMinutesIntoHour(),
            .second = day_secs.getSecondsIntoMinute(),
        };
    }
    // Parse 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
    if (text.len >= 10 and text[4] == '-' and text[7] == '-') {
        const year = std.fmt.parseInt(u32, text[0..4], 10) catch return null;
        const month = std.fmt.parseInt(u8, text[5..7], 10) catch return null;
        const day = std.fmt.parseInt(u8, text[8..10], 10) catch return null;
        var hour: u8 = 0;
        var minute: u8 = 0;
        var second: u8 = 0;
        if (text.len >= 19 and text[10] == ' ' and text[13] == ':' and text[16] == ':') {
            hour = std.fmt.parseInt(u8, text[11..13], 10) catch return null;
            minute = std.fmt.parseInt(u8, text[14..16], 10) catch return null;
            second = std.fmt.parseInt(u8, text[17..19], 10) catch return null;
        }
        return .{ .year = year, .month = month, .day = day, .hour = hour, .minute = minute, .second = second };
    }
    // Parse 'HH:MM:SS' (time-only input)
    if (text.len >= 8 and text[2] == ':' and text[5] == ':') {
        const hour = std.fmt.parseInt(u8, text[0..2], 10) catch return null;
        const minute = std.fmt.parseInt(u8, text[3..5], 10) catch return null;
        const second = std.fmt.parseInt(u8, text[6..8], 10) catch return null;
        return .{ .year = 2000, .month = 1, .day = 1, .hour = hour, .minute = minute, .second = second };
    }
    return null;
}

/// Format date/time components based on which part is needed
pub fn formatDateTime(self: *Database, dt: DateTimeComponents, part: DateTimePart) !Value {
    return switch (part) {
        .date_only => {
            const buf = try std.fmt.allocPrint(self.allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ dt.year, dt.month, dt.day });
            return .{ .text = buf };
        },
        .time_only => {
            const buf = try std.fmt.allocPrint(self.allocator, "{d:0>2}:{d:0>2}:{d:0>2}", .{ dt.hour, dt.minute, dt.second });
            return .{ .text = buf };
        },
        .datetime => {
            const buf = try std.fmt.allocPrint(self.allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second });
            return .{ .text = buf };
        },
    };
}

/// Evaluate date/time/datetime functions
pub fn evalDateTimeFunc(self: *Database, args: []const *const Expr, tbl: *const Table, row: Row, part: DateTimePart) !Value {
    if (args.len < 1) return .null_val;
    const val = try evalExpr(self, args[0], tbl, row);
    defer if (val == .text) self.allocator.free(val.text);
    if (val == .null_val) return .null_val;
    const text = self.valueToText(val);
    defer self.allocator.free(text);
    const dt = parseDateTimeInput(self, text) orelse return .null_val;
    return formatDateTime(self, dt, part);
}

/// Evaluate strftime(format, timevalue)
pub fn evalStrftime(self: *Database, args: []const *const Expr, tbl: *const Table, row: Row) !Value {
    if (args.len < 2) return .null_val;
    // First arg: format string
    const fmt_val = try evalExpr(self, args[0], tbl, row);
    defer if (fmt_val == .text) self.allocator.free(fmt_val.text);
    if (fmt_val == .null_val) return .null_val;
    const fmt_text = self.valueToText(fmt_val);
    defer self.allocator.free(fmt_text);
    // Second arg: time value
    const time_val = try evalExpr(self, args[1], tbl, row);
    defer if (time_val == .text) self.allocator.free(time_val.text);
    if (time_val == .null_val) return .null_val;
    const time_text = self.valueToText(time_val);
    defer self.allocator.free(time_text);
    const dt = parseDateTimeInput(self, time_text) orelse return .null_val;
    // Build output by replacing format specifiers
    var result: std.ArrayList(u8) = .{};
    var i: usize = 0;
    while (i < fmt_text.len) {
        if (fmt_text[i] == '%' and i + 1 < fmt_text.len) {
            const spec = fmt_text[i + 1];
            switch (spec) {
                'Y' => {
                    const buf = std.fmt.allocPrint(self.allocator, "{d:0>4}", .{dt.year}) catch return .null_val;
                    defer self.allocator.free(buf);
                    result.appendSlice(self.allocator, buf) catch return .null_val;
                },
                'm' => {
                    const buf = std.fmt.allocPrint(self.allocator, "{d:0>2}", .{dt.month}) catch return .null_val;
                    defer self.allocator.free(buf);
                    result.appendSlice(self.allocator, buf) catch return .null_val;
                },
                'd' => {
                    const buf = std.fmt.allocPrint(self.allocator, "{d:0>2}", .{dt.day}) catch return .null_val;
                    defer self.allocator.free(buf);
                    result.appendSlice(self.allocator, buf) catch return .null_val;
                },
                'H' => {
                    const buf = std.fmt.allocPrint(self.allocator, "{d:0>2}", .{dt.hour}) catch return .null_val;
                    defer self.allocator.free(buf);
                    result.appendSlice(self.allocator, buf) catch return .null_val;
                },
                'M' => {
                    const buf = std.fmt.allocPrint(self.allocator, "{d:0>2}", .{dt.minute}) catch return .null_val;
                    defer self.allocator.free(buf);
                    result.appendSlice(self.allocator, buf) catch return .null_val;
                },
                'S' => {
                    const buf = std.fmt.allocPrint(self.allocator, "{d:0>2}", .{dt.second}) catch return .null_val;
                    defer self.allocator.free(buf);
                    result.appendSlice(self.allocator, buf) catch return .null_val;
                },
                '%' => {
                    result.append(self.allocator, '%') catch return .null_val;
                },
                else => {
                    result.append(self.allocator, '%') catch return .null_val;
                    result.append(self.allocator, spec) catch return .null_val;
                },
            }
            i += 2;
        } else {
            result.append(self.allocator, fmt_text[i]) catch return .null_val;
            i += 1;
        }
    }
    return .{ .text = result.toOwnedSlice(self.allocator) catch return .null_val };
}
