const std = @import("std");
const tokenizer = @import("tokenizer");
const parser = @import("parser");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");

// Re-export value types for public API
pub const Value = value_mod.Value;
pub const Row = value_mod.Row;
pub const Column = value_mod.Column;
pub const TableSnapshot = value_mod.TableSnapshot;

// Re-export parser types for public API
pub const Tokenizer = tokenizer.Tokenizer;
pub const Parser = parser.Parser;
pub const Statement = parser.Statement;
pub const WhereClause = parser.WhereClause;
pub const WhereCondition = parser.WhereCondition;
pub const LogicOp = parser.LogicOp;
pub const CompOp = parser.CompOp;
pub const OrderByClause = parser.OrderByClause;
pub const OrderByItem = parser.OrderByItem;
pub const SortOrder = parser.SortOrder;
pub const NullOrder = parser.NullOrder;
pub const SelectExpr = parser.SelectExpr;
pub const AggFunc = parser.AggFunc;
pub const SetOp = parser.SetOp;
pub const JoinClause = parser.JoinClause;
pub const JoinType = parser.JoinType;
pub const HavingClause = parser.HavingClause;
pub const Expr = parser.Expr;
pub const BinOp = parser.BinOp;
pub const UnaryOp = parser.UnaryOp;
pub const ScalarFunc = parser.ScalarFunc;

// Re-export table type for internal use
pub const Table = table_mod.Table;

const dupeStr = value_mod.dupeStr;
const compareValuesOrder = value_mod.compareValuesOrder;
const rowsEqual = value_mod.rowsEqual;
const likeMatch = value_mod.likeMatch;
const globMatch = value_mod.globMatch;

pub const Database = struct {
    tables: std.StringHashMap(Table),
    allocator: std.mem.Allocator,
    // Temporary storage for projected rows (freed on next execute)
    projected_rows: ?[]Row = null,
    projected_values: ?[][]Value = null,
    projected_texts: ?[][]const u8 = null, // Text values allocated by aggregate functions
    projected_column_names: ?[][]const u8 = null, // Column names from last SELECT
    temp_cte_names: ?[][]const u8 = null, // CTE table names to clean up
    // Transaction state
    in_transaction: bool = false,
    transaction_snapshot: ?[]TableSnapshot = null,

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{
            .tables = std.StringHashMap(Table).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Database) void {
        self.freeProjected();
        self.freeSnapshot();
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
        if (self.projected_column_names) |pcn| {
            for (pcn) |name| {
                self.allocator.free(name);
            }
            self.allocator.free(pcn);
            self.projected_column_names = null;
        }
        if (self.projected_rows) |pr| {
            self.allocator.free(pr);
            self.projected_rows = null;
        }
        // Clean up CTE temporary tables
        if (self.temp_cte_names) |names| {
            for (names) |name| {
                // Must remove from hash map BEFORE deiniting table,
                // because table.name is the same allocation as the hash map key
                if (self.tables.fetchRemove(name)) |kv| {
                    var tbl = kv.value;
                    tbl.deinit();
                }
                self.allocator.free(name);
            }
            self.allocator.free(names);
            self.temp_cte_names = null;
        }
    }

    fn freeSnapshot(self: *Database) void {
        if (self.transaction_snapshot) |snapshots| {
            for (snapshots) |snap| {
                for (snap.rows) |row| {
                    for (row.values) |val| {
                        switch (val) {
                            .text => |t| self.allocator.free(t),
                            else => {},
                        }
                    }
                    self.allocator.free(row.values);
                }
                self.allocator.free(snap.rows);
                for (snap.columns) |col| {
                    self.allocator.free(col.name);
                }
                self.allocator.free(snap.columns);
                self.allocator.free(snap.name);
            }
            self.allocator.free(snapshots);
            self.transaction_snapshot = null;
        }
    }

    fn takeSnapshot(self: *Database) !void {
        var snapshots: std.ArrayList(TableSnapshot) = .{};
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const table = entry.value_ptr;
            // Deep copy columns
            var cols = try self.allocator.alloc(Column, table.columns.len);
            for (table.columns, 0..) |col, i| {
                cols[i] = .{
                    .name = try dupeStr(self.allocator, col.name),
                    .col_type = col.col_type,
                    .is_primary_key = col.is_primary_key,
                };
            }
            // Deep copy rows
            const src_rows = table.storage().scan();
            var rows = try self.allocator.alloc(Row, src_rows.len);
            for (src_rows, 0..) |row, ri| {
                var values = try self.allocator.alloc(Value, row.values.len);
                for (row.values, 0..) |val, vi| {
                    values[vi] = switch (val) {
                        .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                        .integer => |n| .{ .integer = n },
                        .null_val => .null_val,
                    };
                }
                rows[ri] = .{ .values = values };
            }
            try snapshots.append(self.allocator, .{
                .name = try dupeStr(self.allocator, table.name),
                .columns = cols,
                .rows = rows,
            });
        }
        self.transaction_snapshot = try snapshots.toOwnedSlice(self.allocator);
    }

    fn restoreSnapshot(self: *Database) !void {
        if (self.transaction_snapshot == null) return;
        const snapshots = self.transaction_snapshot.?;

        // Drop all current tables
        var it = self.tables.valueIterator();
        while (it.next()) |table| {
            table.deinit();
        }
        self.tables.clearRetainingCapacity();

        // Restore from snapshot
        for (snapshots) |snap| {
            const table_name = try dupeStr(self.allocator, snap.name);
            var cols = try self.allocator.alloc(Column, snap.columns.len);
            for (snap.columns, 0..) |col, i| {
                cols[i] = .{
                    .name = try dupeStr(self.allocator, col.name),
                    .col_type = col.col_type,
                    .is_primary_key = col.is_primary_key,
                };
            }
            var table = Table.init(self.allocator, table_name, cols);
            for (snap.rows) |row| {
                var values = try self.allocator.alloc(Value, row.values.len);
                for (row.values, 0..) |val, vi| {
                    values[vi] = switch (val) {
                        .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                        .integer => |n| .{ .integer = n },
                        .null_val => .null_val,
                    };
                }
                try table.storage().append(self.allocator, .{ .values = values });
            }
            try self.tables.put(table_name, table);
        }

        // Free the snapshot
        self.freeSnapshot();
    }

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
                const col_idx = tbl.findColumnIndex(name) orelse return .null_val;
                const val = row.values[col_idx];
                // Deep copy text values so caller can free them uniformly
                return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
            },
            .qualified_ref => |qr| {
                // For non-JOIN context, just use the column name
                const col_idx = tbl.findColumnIndex(qr.column) orelse return .null_val;
                const val = row.values[col_idx];
                return if (val == .text) .{ .text = try dupeStr(self.allocator, val.text) } else val;
            },
            .binary_op => |bin| {
                // AND/OR: short-circuit evaluation
                if (bin.op == .logical_and) {
                    const left_val = try self.evalExpr(bin.left, tbl, row);
                    defer if (left_val == .text) self.allocator.free(left_val.text);
                    const left_true = self.valueToBool(left_val);
                    if (!left_true) return .{ .integer = 0 };
                    const right_val = try self.evalExpr(bin.right, tbl, row);
                    defer if (right_val == .text) self.allocator.free(right_val.text);
                    return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
                }
                if (bin.op == .logical_or) {
                    const left_val = try self.evalExpr(bin.left, tbl, row);
                    defer if (left_val == .text) self.allocator.free(left_val.text);
                    const left_true = self.valueToBool(left_val);
                    if (left_true) return .{ .integer = 1 };
                    const right_val = try self.evalExpr(bin.right, tbl, row);
                    defer if (right_val == .text) self.allocator.free(right_val.text);
                    return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
                }

                const left_val = try self.evalExpr(bin.left, tbl, row);
                const right_val = try self.evalExpr(bin.right, tbl, row);
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
                    const cond_val = try self.evalExpr(cond_expr, tbl, row);
                    defer if (cond_val == .text) self.allocator.free(cond_val.text);

                    // Non-zero and non-NULL is true
                    const is_true = switch (cond_val) {
                        .integer => |n| n != 0,
                        else => false,
                    };

                    if (is_true) {
                        return try self.evalExpr(result_expr, tbl, row);
                    }
                }

                // No condition matched, evaluate ELSE if present
                if (cw.else_result) |else_expr| {
                    return try self.evalExpr(else_expr, tbl, row);
                }

                // No ELSE and no match -> NULL
                return .null_val;
            },
            .unary_op => |u| {
                const operand_val = try self.evalExpr(u.operand, tbl, row);
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
                const operand_val = try self.evalExpr(il.operand, tbl, row);
                defer if (operand_val == .text) self.allocator.free(operand_val.text);
                if (operand_val == .null_val) return .null_val;

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
                const operand_val = try self.evalExpr(iv.operand, tbl, row);
                defer if (operand_val == .text) self.allocator.free(operand_val.text);
                if (operand_val == .null_val) return .null_val;

                for (iv.values) |val_expr| {
                    const val = try self.evalExpr(val_expr, tbl, row);
                    defer if (val == .text) self.allocator.free(val.text);
                    if (self.valuesEqualUnion(operand_val, val)) return .{ .integer = 1 };
                }
                return .{ .integer = 0 };
            },
            .scalar_subquery => |sq| {
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
                return self.evalScalarFunc(sf.func, sf.args, tbl, row);
            },
            .cast => |c| {
                const val = try self.evalExpr(c.operand, tbl, row);
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
    fn evalGroupExpr(self: *Database, expr: *const Expr, tbl: *const Table, group_rows: []const Row) !Value {
        switch (expr.*) {
            .aggregate => |agg| {
                const arg_name: []const u8 = switch (agg.arg.*) {
                    .column_ref => |name| name,
                    .star => "*",
                    else => return .null_val,
                };
                return self.computeAgg(agg.func, arg_name, tbl, group_rows, agg.separator, agg.distinct);
            },
            .binary_op => |bin| {
                // Short-circuit for logical ops
                if (bin.op == .logical_and) {
                    const left_val = try self.evalGroupExpr(bin.left, tbl, group_rows);
                    defer if (left_val == .text) self.allocator.free(left_val.text);
                    if (!self.valueToBool(left_val)) return .{ .integer = 0 };
                    const right_val = try self.evalGroupExpr(bin.right, tbl, group_rows);
                    defer if (right_val == .text) self.allocator.free(right_val.text);
                    return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
                }
                if (bin.op == .logical_or) {
                    const left_val = try self.evalGroupExpr(bin.left, tbl, group_rows);
                    defer if (left_val == .text) self.allocator.free(left_val.text);
                    if (self.valueToBool(left_val)) return .{ .integer = 1 };
                    const right_val = try self.evalGroupExpr(bin.right, tbl, group_rows);
                    defer if (right_val == .text) self.allocator.free(right_val.text);
                    return .{ .integer = if (self.valueToBool(right_val)) @as(i64, 1) else 0 };
                }

                const left_val = try self.evalGroupExpr(bin.left, tbl, group_rows);
                const right_val = try self.evalGroupExpr(bin.right, tbl, group_rows);
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
            else => {
                // For non-aggregate expressions, evaluate against the first row
                if (group_rows.len > 0) {
                    return self.evalExpr(expr, tbl, group_rows[0]);
                }
                return .null_val;
            },
        }
    }

    fn evalScalarFunc(self: *Database, func: ScalarFunc, args: []const *const Expr, tbl: *const Table, row: Row) !Value {
        switch (func) {
            .abs => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const n = self.valueToI64(val) orelse return .null_val;
                return .{ .integer = if (n < 0) -n else n };
            },
            .length => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                    const val = try self.evalExpr(arg, tbl, row);
                    if (val != .null_val) return val;
                }
                return .null_val;
            },
            .nullif => {
                if (args.len != 2) return .null_val;
                const v1 = try self.evalExpr(args[0], tbl, row);
                const v2 = try self.evalExpr(args[1], tbl, row);
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
                    const val = try self.evalExpr(arg, tbl, row);
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
                    const val = try self.evalExpr(arg, tbl, row);
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
                const cond = try self.evalExpr(args[0], tbl, row);
                defer if (cond == .text) self.allocator.free(cond.text);
                if (self.valueToBool(cond)) {
                    return try self.evalExpr(args[1], tbl, row);
                } else {
                    return try self.evalExpr(args[2], tbl, row);
                }
            },
            .substr => {
                if (args.len < 2 or args.len > 3) return .null_val;
                const str_val = try self.evalExpr(args[0], tbl, row);
                if (str_val == .null_val) return .null_val;
                const text = self.valueToText(str_val);
                defer if (str_val == .text) self.allocator.free(str_val.text);
                const start_val = try self.evalExpr(args[1], tbl, row);
                defer if (start_val == .text) self.allocator.free(start_val.text);
                const start_raw = self.valueToI64(start_val) orelse {
                    self.allocator.free(text);
                    return .null_val;
                };
                // SQLite SUBSTR is 1-based; negative means from end
                const text_len: i64 = @intCast(text.len);
                var start: i64 = if (start_raw > 0) start_raw - 1 else if (start_raw < 0) text_len + start_raw else 0;
                if (start < 0) start = 0;
                var length: i64 = text_len - start;
                if (args.len == 3) {
                    const len_val = try self.evalExpr(args[2], tbl, row);
                    defer if (len_val == .text) self.allocator.free(len_val.text);
                    length = self.valueToI64(len_val) orelse {
                        self.allocator.free(text);
                        return .null_val;
                    };
                    if (length < 0) length = 0;
                }
                const s: usize = @intCast(@min(start, text_len));
                const e: usize = @intCast(@min(start + length, text_len));
                const result = dupeStr(self.allocator, text[s..e]) catch return .null_val;
                self.allocator.free(text);
                return .{ .text = result };
            },
            .instr => {
                if (args.len != 2) return .null_val;
                const str_val = try self.evalExpr(args[0], tbl, row);
                if (str_val == .null_val) return .null_val;
                const text = self.valueToText(str_val);
                defer if (str_val == .text) self.allocator.free(str_val.text);
                const sub_val = try self.evalExpr(args[1], tbl, row);
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
                const str_val = try self.evalExpr(args[0], tbl, row);
                if (str_val == .null_val) return .null_val;
                const text = self.valueToText(str_val);
                defer if (str_val == .text) self.allocator.free(str_val.text);
                const from_val = try self.evalExpr(args[1], tbl, row);
                if (from_val == .null_val) {
                    self.allocator.free(text);
                    return .null_val;
                }
                const from = self.valueToText(from_val);
                defer if (from_val == .text) self.allocator.free(from_val.text);
                const to_val = try self.evalExpr(args[2], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                    const val = try self.evalExpr(arg, tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
                if (val == .text) return val;
                if (val == .null_val) return .null_val;
                return .{ .text = self.valueToText(val) };
            },
            .ltrim => {
                if (args.len == 0) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                if (val == .null_val) return .null_val;
                if (val != .text) return val;
                defer self.allocator.free(val.text);
                const trimmed = std.mem.trimLeft(u8, val.text, " ");
                return .{ .text = try dupeStr(self.allocator, trimmed) };
            },
            .rtrim => {
                if (args.len == 0) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                if (val == .null_val) return .null_val;
                if (val != .text) return val;
                defer self.allocator.free(val.text);
                const trimmed = std.mem.trimRight(u8, val.text, " ");
                return .{ .text = try dupeStr(self.allocator, trimmed) };
            },
            .round => {
                // ROUND always returns real in SQLite
                if (args.len == 0 or args.len > 2) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const n = self.valueToI64(val) orelse return .null_val;
                const float_val: f64 = @floatFromInt(n);
                var buf: [64]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{float_val}) catch return .null_val;
                var has_dot = false;
                for (slice) |ch| {
                    if (ch == '.') { has_dot = true; break; }
                }
                if (has_dot) {
                    return .{ .text = try dupeStr(self.allocator, slice) };
                } else {
                    var dot_buf: [66]u8 = undefined;
                    @memcpy(dot_buf[0..slice.len], slice);
                    dot_buf[slice.len] = '.';
                    dot_buf[slice.len + 1] = '0';
                    return .{ .text = try dupeStr(self.allocator, dot_buf[0 .. slice.len + 2]) };
                }
            },
            .ifnull => {
                if (args.len != 2) return .null_val;
                const v1 = try self.evalExpr(args[0], tbl, row);
                if (v1 != .null_val) return v1;
                return try self.evalExpr(args[1], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const n = self.valueToI64(val) orelse return .null_val;
                return .{ .integer = if (n > 0) @as(i64, 1) else if (n < 0) @as(i64, -1) else 0 };
            },
            .date_fn => {
                return self.evalDateTimeFunc(args, tbl, row, .date_only);
            },
            .time_fn => {
                return self.evalDateTimeFunc(args, tbl, row, .time_only);
            },
            .datetime_fn => {
                return self.evalDateTimeFunc(args, tbl, row, .datetime);
            },
            .strftime_fn => {
                return self.evalStrftime(args, tbl, row);
            },
            .like_fn => {
                // LIKE(pattern, string) → 1 if matches, 0 otherwise
                if (args.len < 2) return .null_val;
                const pattern_val = try self.evalExpr(args[0], tbl, row);
                defer if (pattern_val == .text) self.allocator.free(pattern_val.text);
                const string_val = try self.evalExpr(args[1], tbl, row);
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
                // GLOB(pattern, string) → 1 if matches, 0 otherwise
                if (args.len < 2) return .null_val;
                const pattern_val = try self.evalExpr(args[0], tbl, row);
                defer if (pattern_val == .text) self.allocator.free(pattern_val.text);
                const string_val = try self.evalExpr(args[1], tbl, row);
                defer if (string_val == .text) self.allocator.free(string_val.text);
                if (pattern_val == .null_val or string_val == .null_val) return .null_val;
                const pattern_text = self.valueToText(pattern_val);
                defer self.allocator.free(pattern_text);
                const string_text = self.valueToText(string_val);
                defer self.allocator.free(string_text);
                const matches = try globMatch(string_text, pattern_text);
                return .{ .integer = if (matches) 1 else 0 };
            },
        }
    }

    const DateTimePart = enum { date_only, time_only, datetime };

    /// Components of a parsed date/time
    const DateTimeComponents = struct {
        year: u32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    };

    /// Parse a date/time string or 'now' into components
    fn parseDateTimeInput(self: *Database, text: []const u8) ?DateTimeComponents {
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
    fn formatDateTime(self: *Database, dt: DateTimeComponents, part: DateTimePart) !Value {
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
    fn evalDateTimeFunc(self: *Database, args: []const *const Expr, tbl: *const Table, row: Row, part: DateTimePart) !Value {
        if (args.len < 1) return .null_val;
        const val = try self.evalExpr(args[0], tbl, row);
        defer if (val == .text) self.allocator.free(val.text);
        if (val == .null_val) return .null_val;
        const text = self.valueToText(val);
        defer self.allocator.free(text);
        const dt = self.parseDateTimeInput(text) orelse return .null_val;
        return self.formatDateTime(dt, part);
    }

    /// Evaluate strftime(format, timevalue)
    fn evalStrftime(self: *Database, args: []const *const Expr, tbl: *const Table, row: Row) !Value {
        if (args.len < 2) return .null_val;
        // First arg: format string
        const fmt_val = try self.evalExpr(args[0], tbl, row);
        defer if (fmt_val == .text) self.allocator.free(fmt_val.text);
        if (fmt_val == .null_val) return .null_val;
        const fmt_text = self.valueToText(fmt_val);
        defer self.allocator.free(fmt_text);
        // Second arg: time value
        const time_val = try self.evalExpr(args[1], tbl, row);
        defer if (time_val == .text) self.allocator.free(time_val.text);
        if (time_val == .null_val) return .null_val;
        const time_text = self.valueToText(time_val);
        defer self.allocator.free(time_text);
        const dt = self.parseDateTimeInput(time_text) orelse return .null_val;
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

    /// Execute WITH (Common Table Expressions)
    fn executeWithCTE(self: *Database, wc: Statement.WithCTE) !ExecuteResult {
        // Track CTE table names for deferred cleanup (in freeProjected)
        var cte_names: std.ArrayList([]const u8) = .{};

        // Execute each CTE and create temporary tables
        for (wc.ctes) |cte| {
            // Save projected state
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
                    // Create table with inferred columns
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
                    const tname = try dupeStr(self.allocator, cte.name);
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
                    try cte_names.append(self.allocator, try dupeStr(self.allocator, cte.name));
                },
                .err => |e| {
                    self.freeProjected();
                    self.projected_rows = saved_rows;
                    self.projected_values = saved_values;
                    self.projected_texts = saved_texts;
                    self.projected_column_names = saved_col_names;
                    for (cte_names.items) |name| {
                        if (self.tables.getPtr(name)) |tbl| {
                            tbl.deinit();
                            _ = self.tables.remove(name);
                        }
                        self.allocator.free(name);
                    }
                    cte_names.deinit(self.allocator);
                    return .{ .err = e };
                },
                .ok => {},
            }

            self.freeProjected();
            self.projected_rows = saved_rows;
            self.projected_values = saved_values;
            self.projected_texts = saved_texts;
            self.projected_column_names = saved_col_names;
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

    /// Execute CREATE TABLE ... AS SELECT
    fn executeCreateTableAsSelect(self: *Database, table_name: []const u8, select_sql: []const u8) !ExecuteResult {
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

    /// Evaluate window functions and fill in values in projected rows
    fn evaluateWindowFunctions(
        self: *Database,
        result_exprs: []const *const Expr,
        proj_rows: []Row,
        proj_values: [][]Value,
        tbl: *const Table,
        src_rows: []const Row,
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
                    try self.computeWindowAggregates(wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows);
                },
                .lag, .lead, .ntile, .first_value, .last_value => {
                    try self.computeWindowValueFunctions(wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows);
                },
                else => {},
            }
        }
    }

    fn computeWindowAggregates(
        self: *Database,
        wf: anytype,
        indices: []const usize,
        col_idx: usize,
        tbl: *const Table,
        src_rows: []const Row,
        proj_values: [][]Value,
        proj_rows: []Row,
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

            // Compute aggregate over this partition
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

            // Determine the result value
            const result: Value = switch (wf.func) {
                .win_count => .{ .integer = agg_count },
                .win_sum => if (has_value) blk: {
                    // Return integer if all values were integers and result fits
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

            // Assign to all rows in this partition
            for (part_start..part_end) |i| {
                const row_idx = indices[i];
                proj_values[row_idx][col_idx] = result;
                proj_rows[row_idx] = .{ .values = proj_values[row_idx] };
            }

            part_start = part_end;
        }
    }

    fn computeWindowValueFunctions(
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

    /// Expand partial INSERT values to full row using column list and defaults
    fn expandInsertValues(self: *Database, table: *Table, col_names: []const []const u8, values: []const []const u8) ![]const []const u8 {
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
    fn hasPkConflict(self: *Database, table: *Table, raw_values: []const []const u8) bool {
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

    /// Validate CHECK constraints for a row about to be inserted
    fn validateCheckConstraints(self: *Database, table: *Table, raw_values: []const []const u8) !bool {
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
                        defer if (sel.group_by) |gb| self.allocator.free(gb);
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
    fn replaceRow(self: *Database, table: *Table, raw_values: []const []const u8) void {
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
    fn executeUpsertRow(self: *Database, table: *Table, raw_values: []const []const u8, on_conflict: Statement.OnConflict) !void {
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

    /// Convert Value to boolean (SQLite3 semantics: 0 and NULL are false)
    fn valueToBool(_: *Database, val: Value) bool {
        return switch (val) {
            .integer => |n| n != 0,
            .text => true,
            .null_val => false,
        };
    }

    /// Free a where_expr tree including subquery SQL buffers
    fn freeWhereExpr(self: *Database, expr: *const Expr) void {
        self.freeExprDeep(expr);
    }

    fn freeExprDeep(self: *Database, expr: *const Expr) void {
        switch (expr.*) {
            .binary_op => |bin| {
                self.freeExprDeep(bin.left);
                self.freeExprDeep(bin.right);
            },
            .aggregate => |agg| {
                self.freeExprDeep(agg.arg);
            },
            .case_when => |cw| {
                for (cw.conditions) |cond| self.freeExprDeep(cond);
                self.allocator.free(cw.conditions);
                for (cw.results) |res| self.freeExprDeep(res);
                self.allocator.free(cw.results);
                if (cw.else_result) |er| self.freeExprDeep(er);
            },
            .unary_op => |u| {
                self.freeExprDeep(u.operand);
            },
            .in_list => |il| {
                self.freeExprDeep(il.operand);
                self.allocator.free(@constCast(il.subquery_sql));
            },
            .in_values => |iv| {
                self.freeExprDeep(iv.operand);
                for (iv.values) |v| self.freeExprDeep(v);
                self.allocator.free(iv.values);
            },
            .scalar_subquery => |sq| {
                self.allocator.free(@constCast(sq.subquery_sql));
            },
            .exists => |ex| {
                self.allocator.free(@constCast(ex.subquery_sql));
            },
            .scalar_func => |sf| {
                for (sf.args) |arg| self.freeExprDeep(arg);
                self.allocator.free(sf.args);
            },
            .cast => |c| {
                self.freeExprDeep(c.operand);
            },
            else => {},
        }
        self.allocator.destroy(@constCast(expr));
    }

    fn valueToText(self: *Database, val: Value) []const u8 {
        switch (val) {
            .text => |t| return dupeStr(self.allocator, t) catch "",
            .integer => |n| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{n}) catch return "";
                return dupeStr(self.allocator, slice) catch "";
            },
            .null_val => return dupeStr(self.allocator, "") catch "",
        }
    }

    fn valueToI64(_: *Database, val: Value) ?i64 {
        switch (val) {
            .integer => |n| return n,
            .text => |t| return std.fmt.parseInt(i64, t, 10) catch null,
            .null_val => return null,
        }
    }

    fn valueToF64(_: *Database, val: Value) ?f64 {
        switch (val) {
            .integer => |n| return @floatFromInt(n),
            .text => |t| return std.fmt.parseFloat(f64, t) catch null,
            .null_val => return null,
        }
    }

    fn isFloatValue(_: *Database, val: Value) bool {
        switch (val) {
            .text => |t| {
                // Check if it contains a decimal point (float value)
                for (t) |c| {
                    if (c == '.') return true;
                }
                return false;
            },
            else => return false,
        }
    }

    fn formatFloat(self: *Database, f: f64) Value {
        // Use {d} format and ensure decimal point is present
        const txt = std.fmt.allocPrint(self.allocator, "{d}", .{f}) catch return .null_val;
        // Check if result has a decimal point
        for (txt) |c| {
            if (c == '.' or c == 'e' or c == 'E') return .{ .text = txt };
        }
        // Append ".0" to make it look like a float
        const with_dot = std.fmt.allocPrint(self.allocator, "{s}.0", .{txt}) catch {
            self.allocator.free(txt);
            return .null_val;
        };
        self.allocator.free(txt);
        return .{ .text = with_dot };
    }

    fn computeAgg(self: *Database, func: AggFunc, arg: []const u8, tbl: *const Table, rows: []const Row, separator: []const u8, is_distinct: bool) !Value {
        if (func == .count) {
            if (std.mem.eql(u8, arg, "*")) {
                return .{ .integer = @intCast(rows.len) };
            }
            const col_idx = tbl.findColumnIndex(arg) orelse return error.UnexpectedToken;
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

        const col_idx = tbl.findColumnIndex(arg) orelse return error.UnexpectedToken;

        switch (func) {
            .sum => {
                var total: i64 = 0;
                var has_value = false;
                if (is_distinct) {
                    var seen: std.ArrayList(i64) = .{};
                    defer seen.deinit(self.allocator);
                    for (rows) |row| {
                        if (row.values[col_idx] == .integer) {
                            const v = row.values[col_idx].integer;
                            var found = false;
                            for (seen.items) |s| {
                                if (s == v) { found = true; break; }
                            }
                            if (!found) {
                                seen.append(self.allocator, v) catch {};
                                total += v;
                                has_value = true;
                            }
                        }
                    }
                } else {
                    for (rows) |row| {
                        if (row.values[col_idx] == .integer) {
                            total += row.values[col_idx].integer;
                            has_value = true;
                        }
                    }
                }
                if (!has_value) return .null_val;
                return .{ .integer = total };
            },
            .avg => {
                var total: f64 = 0;
                var cnt: usize = 0;
                if (is_distinct) {
                    var seen: std.ArrayList(i64) = .{};
                    defer seen.deinit(self.allocator);
                    for (rows) |row| {
                        if (row.values[col_idx] == .integer) {
                            const v = row.values[col_idx].integer;
                            var found = false;
                            for (seen.items) |s| {
                                if (s == v) { found = true; break; }
                            }
                            if (!found) {
                                seen.append(self.allocator, v) catch {};
                                total += @floatFromInt(v);
                                cnt += 1;
                            }
                        }
                    }
                } else {
                    for (rows) |row| {
                        if (row.values[col_idx] == .integer) {
                            total += @floatFromInt(row.values[col_idx].integer);
                            cnt += 1;
                        }
                    }
                }
                if (cnt == 0) return .null_val;
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
                    if (row.values[col_idx] == .integer) {
                        sum += @floatFromInt(row.values[col_idx].integer);
                    }
                }
                // TOTAL always returns real, even for empty set (0.0)
                var buf: [64]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{sum}) catch return error.OutOfMemory;
                var has_dot = false;
                for (slice) |ch| {
                    if (ch == '.') { has_dot = true; break; }
                }
                if (has_dot) {
                    return .{ .text = try dupeStr(self.allocator, slice) };
                } else {
                    var dot_buf: [66]u8 = undefined;
                    @memcpy(dot_buf[0..slice.len], slice);
                    dot_buf[slice.len] = '.';
                    dot_buf[slice.len + 1] = '0';
                    return .{ .text = try dupeStr(self.allocator, dot_buf[0 .. slice.len + 2]) };
                }
            },
            else => return .null_val,
        }
    }

    fn collectAggTexts(self: *Database, sel_exprs: []const SelectExpr, all_values: []const []Value) !void {
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

    fn executeAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row) !ExecuteResult {
        if (sel.group_by) |gb_cols| {
            return self.executeGroupByAggregate(tbl, sel, rows, gb_cols);
        }

        var values = try self.allocator.alloc(Value, sel.select_exprs.len);
        for (sel.select_exprs, 0..) |expr, i| {
            switch (expr) {
                .aggregate => |agg| {
                    values[i] = try self.computeAgg(agg.func, agg.arg, tbl, rows, agg.separator, agg.distinct);
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
                        values[i] = try self.computeAgg(agg.func, agg.arg, tbl, rows, agg.separator, agg.distinct);
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
        try self.collectAggTexts(sel.select_exprs, proj_values);
        return .{ .rows = proj_rows };
    }

    fn executeGroupByAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row, gb_cols: []const []const u8) !ExecuteResult {
        // Resolve GROUP BY column indices
        var gb_indices = try self.allocator.alloc(usize, gb_cols.len);
        defer self.allocator.free(gb_indices);
        for (gb_cols, 0..) |col, i| {
            gb_indices[i] = tbl.findColumnIndex(col) orelse return .{ .err = "column not found" };
        }

        // Group rows: use first GROUP BY column as primary key, compare all columns for multi-column grouping
        var group_keys: std.ArrayList([]Value) = .{};
        defer {
            for (group_keys.items) |gk| self.allocator.free(gk);
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
            var found_idx: ?usize = null;
            for (group_keys.items, 0..) |gk, gi| {
                var all_eq = true;
                for (gb_indices, 0..) |idx, ki| {
                    if (compareValuesOrder(row.values[idx], gk[ki]) != .eq) {
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
                try group_rows.items[idx].append(self.allocator, row);
            } else {
                var key = try self.allocator.alloc(Value, gb_indices.len);
                for (gb_indices, 0..) |idx, ki| key[ki] = row.values[idx];
                try group_keys.append(self.allocator, key);
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
                        values[ei] = try self.computeAgg(agg.func, agg.arg, tbl, grp, agg.separator, agg.distinct);
                    },
                    .column => |col_name| {
                        const col_idx = tbl.findColumnIndex(col_name) orelse return .{ .err = "column not found" };
                        values[ei] = grp[0].values[col_idx];
                    },
                    .expr => |e| {
                        if (parser.Parser.exprAsAggregate(e)) |agg| {
                            values[ei] = try self.computeAgg(agg.func, agg.arg, tbl, grp, agg.separator, agg.distinct);
                        } else if (grp.len > 0) {
                            values[ei] = try self.evalExpr(e, tbl, grp[0]);
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
        // Find the indices of GROUP BY columns in select_exprs
        var gb_sort_indices: std.ArrayList(usize) = .{};
        defer gb_sort_indices.deinit(self.allocator);
        for (gb_cols) |gb_col| {
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

        self.projected_rows = proj_rows;
        self.projected_values = proj_values;
        try self.collectAggTexts(sel.select_exprs, proj_values);
        return .{ .rows = proj_rows };
    }

    fn executeSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
        // Handle table-less SELECT (e.g., SELECT ABS(-10);)
        if (sel.table_name.len == 0) {
            return self.executeTablelessSelect(sel);
        }

        // Handle JOIN queries
        if (sel.joins.len > 0) {
            return self.executeJoin(sel);
        }

        if (self.tables.get(sel.table_name)) |table| {
            // Collect matching rows (with optional WHERE filter)
            var matching_rows: std.ArrayList(Row) = .{};
            defer matching_rows.deinit(self.allocator);
            const src_rows = table.storage().scan();
            for (src_rows) |row| {
                if (sel.where_expr) |we| {
                    const val = try self.evalExpr(we, &table, row);
                    defer if (val == .text) self.allocator.free(val.text);
                    if (!self.valueToBool(val)) continue;
                } else if (sel.where) |where| {
                    if (!(try self.matchesWhereWithSubquery(&table, row, where))) continue;
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
                return self.executeAggregate(&table, sel, matching_rows.items);
            }

            // Apply ORDER BY (multi-column)
            if (sel.order_by) |order_by| {
                try self.sortRowsByOrderBy(&table, matching_rows.items, order_by.items);
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
                return self.evaluateExprSelect(sel, &table, result_rows);
            }

            return self.projectColumns(sel, &table, result_rows);
        }
        return .{ .err = "table not found" };
    }

    /// Handle SELECT without FROM (e.g., SELECT ABS(-10); SELECT 1+2;)
    fn executeTablelessSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
        const expr_count = sel.result_exprs.len;
        // Create a dummy table with no columns for evalExpr context
        var dummy_table = Table.init(self.allocator, "", &.{});
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
    fn evaluateExprSelect(self: *Database, sel: Statement.Select, tbl: *const Table, result_rows: []const Row) !ExecuteResult {
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
        try self.evaluateWindowFunctions(sel.result_exprs, proj_rows, proj_values, tbl, result_rows);

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
                if (item.column.len > 0) {
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

    fn projectColumns(self: *Database, sel: Statement.Select, table: *const Table, result_rows: []const Row) !ExecuteResult {
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

    fn executeJoin(self: *Database, sel: Statement.Select) !ExecuteResult {
        const first_table = self.tables.get(sel.table_name) orelse return .{ .err = "table not found" };

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
        var alias_offsets: std.ArrayList(struct { name: []const u8, alias: []const u8, offset: usize, table: *const Table }) = .{};
        defer alias_offsets.deinit(self.allocator);
        try alias_offsets.append(self.allocator, .{
            .name = sel.table_name,
            .alias = sel.table_alias orelse sel.table_name,
            .offset = 0,
            .table = &first_table,
        });

        // Process each JOIN
        for (sel.joins) |join| {
            const right_table = self.tables.get(join.table_name) orelse return .{ .err = "table not found" };
            const left_col_count = combined_cols_list.items.len;
            const right_col_count = right_table.columns.len;
            const total_cols = left_col_count + right_col_count;

            // Track this table's offset
            try alias_offsets.append(self.allocator, .{
                .name = join.table_name,
                .alias = join.table_alias orelse join.table_name,
                .offset = left_col_count,
                .table = &right_table,
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

        // SELECT * for JOIN
        if (sel.columns.len == 0 and sel.select_exprs.len == 0) {
            const proj_rows = try self.allocator.alloc(Row, result_rows.len);
            @memcpy(proj_rows, result_rows);
            self.projected_rows = proj_rows;
            const pv = try self.allocator.alloc([]Value, joined_values.items.len);
            @memcpy(pv, joined_values.items);
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

        self.projected_rows = proj_rows;
        self.projected_values = proj_values;
        return .{ .rows = proj_rows };
    }

    fn executeSubquery(self: *Database, subquery_sql: []const u8) !?[]const Value {
        // Save and restore projected state
        const saved_rows = self.projected_rows;
        const saved_values = self.projected_values;
        const saved_texts = self.projected_texts;
        const saved_col_names = self.projected_column_names;
        self.projected_rows = null;
        self.projected_values = null;
        self.projected_texts = null;
        self.projected_column_names = null;

        const result = try self.execute(subquery_sql);

        // Collect first column values from result (deep copy text values before freeing)
        var values: ?[]Value = null;
        switch (result) {
            .rows => |rows| {
                if (rows.len > 0) {
                    var vals = try self.allocator.alloc(Value, rows.len);
                    for (rows, 0..) |row, i| {
                        const v = row.values[0];
                        vals[i] = if (v == .text)
                            .{ .text = try dupeStr(self.allocator, v.text) }
                        else
                            v;
                    }
                    values = vals;
                }
            },
            else => {},
        }

        // Free subquery projected data
        self.freeProjected();
        // Restore outer query projected state
        self.projected_rows = saved_rows;
        self.projected_values = saved_values;
        self.projected_texts = saved_texts;
        self.projected_column_names = saved_col_names;

        return values;
    }

    fn executeInsertSelect(self: *Database, table: *Table, select_sql: []const u8) !ExecuteResult {
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
                    try table.storage().append(self.allocator, .{ .values = new_values });
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

    fn executeSetOperands(self: *Database, selects: []const []const u8, saved_rows: ?[]Row, saved_values: ?[][]Value, saved_texts: ?[][]const u8) !?std.ArrayList(std.ArrayList(Row)) {
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

    fn executeUnion(self: *Database, union_sel: Statement.UnionSelect) !ExecuteResult {
        const saved_rows = self.projected_rows;
        const saved_values = self.projected_values;
        const saved_texts = self.projected_texts;

        const operand_results_opt = try self.executeSetOperands(union_sel.selects, saved_rows, saved_values, saved_texts);
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
                try self.removeDuplicateRows(&all_rows);
            },
            .intersect => {
                // Return only rows present in ALL operands
                if (operand_results.items.len >= 1) {
                    // Start with first operand (deduplicated)
                    for (operand_results.items[0].items) |row| {
                        try all_rows.append(self.allocator, row);
                    }
                    try self.removeDuplicateRows(&all_rows);

                    // Keep only rows that also appear in each subsequent operand
                    for (operand_results.items[1..]) |*op_rows| {
                        var write_idx: usize = 0;
                        for (0..all_rows.items.len) |ri| {
                            var found = false;
                            for (op_rows.items) |other_row| {
                                if (self.rowsEqualUnion(all_rows.items[ri], other_row)) {
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
                    try self.removeDuplicateRows(&all_rows);

                    // Remove rows that appear in any subsequent operand
                    for (operand_results.items[1..]) |*op_rows| {
                        var write_idx: usize = 0;
                        for (0..all_rows.items.len) |ri| {
                            var found = false;
                            for (op_rows.items) |other_row| {
                                if (self.rowsEqualUnion(all_rows.items[ri], other_row)) {
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

        // Store results
        self.projected_rows = try self.allocator.dupe(Row, final_rows);
        self.projected_values = null;
        self.projected_texts = null;

        return .{ .rows = self.projected_rows.? };
    }

    fn removeDuplicateRows(self: *Database, rows: *std.ArrayList(Row)) !void {
        var i: usize = 0;
        while (i < rows.items.len) {
            var j = i + 1;
            while (j < rows.items.len) {
                if (self.rowsEqualUnion(rows.items[i], rows.items[j])) {
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

    fn rowsEqualUnion(self: *Database, row1: Row, row2: Row) bool {
        if (row1.values.len != row2.values.len) return false;
        for (row1.values, row2.values) |v1, v2| {
            if (!self.valuesEqualUnion(v1, v2)) return false;
        }
        return true;
    }

    fn valuesEqualUnion(self: *Database, v1: Value, v2: Value) bool {
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
    fn sortRowsByOrderBy(self: *Database, tbl: *const Table, rows: []Row, items: []const OrderByItem) !void {
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
                        keys[ki] = try self.evalExpr(expr, tbl, row);
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

    fn freeRow(self: *Database, row: Row) void {
        for (row.values) |val| {
            if (val == .text) {
                self.allocator.free(val.text);
            }
        }
        self.allocator.free(row.values);
    }

    fn matchesConditionWithSubquery(self: *Database, table: *const Table, row: Row, column: []const u8, op: CompOp, value: []const u8, subquery_sql: ?[]const u8) !bool {
        if (subquery_sql) |sq| {
            const sub_values = try self.executeSubquery(sq);
            defer if (sub_values) |sv| {
                // Free duplicated text values
                for (sv) |v| {
                    if (v == .text) self.allocator.free(v.text);
                }
                self.allocator.free(sv);
            };

            if (sub_values == null) return false;
            const sv = sub_values.?;

            if (op == .in_subquery) {
                // IN: check if row value matches any subquery result
                const col_idx = table.findColumnIndex(column) orelse return false;
                const row_val = row.values[col_idx];
                for (sv) |sub_val| {
                    if (Table.compareValues(row_val, sub_val, .eq)) return true;
                }
                return false;
            } else {
                // Scalar subquery: use first result value
                if (sv.len == 0) return false;
                const col_idx = table.findColumnIndex(column) orelse return false;
                return Table.compareValues(row.values[col_idx], sv[0], op);
            }
        }
        return table.matchesSingleCondition(row, column, op, value);
    }

    fn matchesWhereWithSubquery(self: *Database, table: *const Table, row: Row, where: WhereClause) !bool {
        const has_subquery = where.subquery_sql != null or blk: {
            for (where.extra) |cond| {
                if (cond.subquery_sql != null) break :blk true;
            }
            break :blk false;
        };

        if (!has_subquery) {
            return table.matchesWhere(row, where);
        }

        var result = try self.matchesConditionWithSubquery(table, row, where.column, where.op, where.value, where.subquery_sql);
        for (where.extra, 0..) |cond, i| {
            const cond_result = try self.matchesConditionWithSubquery(table, row, cond.column, cond.op, cond.value, cond.subquery_sql);
            switch (where.connectors[i]) {
                .and_op => result = result and cond_result,
                .or_op => result = result or cond_result,
            }
        }
        return result;
    }

    pub fn execute(self: *Database, sql: []const u8) anyerror!ExecuteResult {
        self.freeProjected();
        var tok = Tokenizer.init(sql);
        const tokens = try tok.tokenize(self.allocator);
        defer self.allocator.free(tokens);

        // Skip empty input (e.g., comment-only lines)
        if (tokens.len == 0 or (tokens.len == 1 and tokens[0].type == .eof)) {
            return .ok;
        }

        var p = Parser.init(self.allocator, tokens);
        const stmt = try p.parse();

        switch (stmt) {
            .create_table => |ct| {
                defer self.allocator.free(ct.columns);
                if (ct.if_not_exists and self.tables.contains(ct.table_name)) {
                    return .ok;
                }
                // CREATE TABLE ... AS SELECT
                if (ct.as_select_sql) |select_sql| {
                    defer self.allocator.free(@constCast(select_sql));
                    return self.executeCreateTableAsSelect(ct.table_name, select_sql);
                }
                const table_name = try dupeStr(self.allocator, ct.table_name);
                var columns = try self.allocator.alloc(Column, ct.columns.len);
                for (ct.columns, 0..) |col, i| {
                    columns[i] = .{
                        .name = try dupeStr(self.allocator, col.name),
                        .col_type = col.col_type,
                        .is_primary_key = col.is_primary_key,
                        .default_value = col.default_value,
                        .not_null = col.not_null,
                        .autoincrement = col.autoincrement,
                        .check_expr_sql = col.check_expr_sql,
                    };
                }
                var table = Table.init(self.allocator, table_name, columns);
                _ = &table;
                try self.tables.put(table_name, table);
                return .ok;
            },
            .create_index => {
                // Syntax accepted, metadata-only (no actual index acceleration yet)
                return .ok;
            },
            .with_cte => |wc| {
                defer {
                    for (wc.ctes) |cte| {
                        self.allocator.free(@constCast(cte.query_sql));
                    }
                    self.allocator.free(wc.ctes);
                    self.allocator.free(@constCast(wc.main_sql));
                }
                return self.executeWithCTE(wc);
            },
            .insert => |ins| {
                defer if (ins.select_sql == null) self.allocator.free(ins.values);
                defer if (ins.select_sql) |s| self.allocator.free(@constCast(s));
                defer {
                    for (ins.extra_rows) |row_values| {
                        self.allocator.free(row_values);
                    }
                    self.allocator.free(ins.extra_rows);
                }
                defer if (ins.on_conflict) |oc| {
                    self.allocator.free(oc.conflict_columns);
                    for (oc.updates) |upd| {
                        // Free allocated "excluded.xxx" strings
                        if (std.mem.startsWith(u8, upd.value_sql, "excluded.")) {
                            self.allocator.free(upd.value_sql);
                        }
                    }
                    self.allocator.free(oc.updates);
                };
                if (self.tables.getPtr(ins.table_name)) |table| {
                    if (ins.select_sql) |select_sql| {
                        return self.executeInsertSelect(table, select_sql);
                    }
                    if (ins.default_values) {
                        // INSERT INTO t DEFAULT VALUES: use defaults for all columns
                        var def_values = try self.allocator.alloc([]const u8, table.columns.len);
                        defer self.allocator.free(def_values);
                        for (table.columns, 0..) |col, i| {
                            def_values[i] = col.default_value orelse "NULL";
                        }
                        try table.insertRow(def_values);
                        return .ok;
                    }
                    // Expand partial column lists with default values
                    const values = if (ins.column_names.len > 0)
                        self.expandInsertValues(table, ins.column_names, ins.values) catch ins.values
                    else
                        ins.values;
                    defer if (ins.column_names.len > 0 and values.ptr != ins.values.ptr) self.allocator.free(values);
                    // Validate CHECK constraints
                    if (!try self.validateCheckConstraints(table, values)) {
                        return .{ .err = "CHECK constraint failed" };
                    }
                    if (ins.on_conflict) |oc| {
                        try self.executeUpsertRow(table, values, oc);
                    } else if (ins.replace_mode) {
                        self.replaceRow(table, values);
                    } else if (ins.ignore_mode) {
                        if (!self.hasPkConflict(table, values)) {
                            try table.insertRow(values);
                        }
                    } else {
                        try table.insertRow(values);
                    }
                    for (ins.extra_rows) |row_values| {
                        const expanded = if (ins.column_names.len > 0)
                            self.expandInsertValues(table, ins.column_names, row_values) catch row_values
                        else
                            row_values;
                        defer if (ins.column_names.len > 0 and expanded.ptr != row_values.ptr) self.allocator.free(expanded);
                        if (!try self.validateCheckConstraints(table, expanded)) {
                            return .{ .err = "CHECK constraint failed" };
                        }
                        if (ins.on_conflict) |oc| {
                            try self.executeUpsertRow(table, expanded, oc);
                        } else if (ins.replace_mode) {
                            self.replaceRow(table, expanded);
                        } else if (ins.ignore_mode) {
                            if (!self.hasPkConflict(table, expanded)) {
                                try table.insertRow(expanded);
                            }
                        } else {
                            try table.insertRow(expanded);
                        }
                    }
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
            .select_stmt => |sel| {
                defer self.allocator.free(sel.columns);
                defer self.allocator.free(sel.select_exprs);
                defer self.allocator.free(sel.aliases);
                defer if (sel.order_by) |ob| {
                    for (ob.items) |item| {
                        if (item.expr) |e| self.freeExprDeep(e);
                    }
                    self.allocator.free(ob.items);
                };
                defer {
                    for (sel.result_exprs) |e| self.freeExprDeep(e);
                    self.allocator.free(sel.result_exprs);
                }
                defer if (sel.where_expr) |we| self.freeWhereExpr(we);
                defer if (sel.group_by) |gb| self.allocator.free(gb);
                return self.executeSelect(sel);
            },
            .delete => |del| {
                defer if (del.where_expr) |we| self.freeWhereExpr(we);
                if (self.tables.getPtr(del.table_name)) |table| {
                    if (del.where_expr) |we| {
                        var i: usize = table.storage().len();
                        while (i > 0) {
                            i -= 1;
                            const rows = table.storage().scan();
                            const val = try self.evalExpr(we, table, rows[i]);
                            defer if (val == .text) self.allocator.free(val.text);
                            if (self.valueToBool(val)) {
                                table.freeRow(rows[i]);
                                _ = table.storage().orderedRemove(i);
                            }
                        }
                    } else if (del.where) |where| {
                        var i: usize = table.storage().len();
                        while (i > 0) {
                            i -= 1;
                            const rows = table.storage().scan();
                            if (table.matchesWhere(rows[i], where)) {
                                table.freeRow(rows[i]);
                                _ = table.storage().orderedRemove(i);
                            }
                        }
                    } else {
                        // DELETE without WHERE: delete all rows
                        const rows = table.storage().scan();
                        for (rows) |row| {
                            table.freeRow(row);
                        }
                        table.storage().clearRetainingCapacity();
                    }
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
            .update => |upd| {
                defer self.allocator.free(upd.set_columns);
                defer self.allocator.free(upd.set_values);
                defer {
                    for (upd.set_exprs) |e| self.freeExprDeep(e);
                    self.allocator.free(upd.set_exprs);
                }
                defer if (upd.where_expr) |we| self.freeWhereExpr(we);
                if (self.tables.getPtr(upd.table_name)) |table| {
                    // Validate all columns exist
                    var col_indices: std.ArrayList(usize) = .{};
                    defer col_indices.deinit(self.allocator);
                    for (upd.set_columns) |col_name| {
                        const idx = table.findColumnIndex(col_name) orelse {
                            return .{ .err = "column not found" };
                        };
                        col_indices.append(self.allocator, idx) catch return .{ .err = "out of memory" };
                    }

                    for (table.storage().scanMut()) |*row| {
                        const matches = if (upd.where_expr) |we| blk: {
                            const val = try self.evalExpr(we, table, row.*);
                            defer if (val == .text) self.allocator.free(val.text);
                            break :blk self.valueToBool(val);
                        } else if (upd.where) |where| table.matchesWhere(row.*, where) else true;
                        if (matches) {
                            if (upd.set_exprs.len > 0) {
                                // Expression-based SET: evaluate each expression
                                for (upd.set_exprs, col_indices.items) |expr, col_idx| {
                                    const new_val = try self.evalExpr(expr, table, row.*);
                                    // Free old text value if needed
                                    switch (row.values[col_idx]) {
                                        .text => |t| self.allocator.free(t),
                                        else => {},
                                    }
                                    row.values[col_idx] = new_val;
                                }
                            } else {
                                // Legacy literal-based SET
                                for (upd.set_values, col_indices.items) |val_str, col_idx| {
                                    switch (row.values[col_idx]) {
                                        .text => |t| self.allocator.free(t),
                                        else => {},
                                    }
                                    if (val_str.len >= 2 and val_str[0] == '\'') {
                                        row.values[col_idx] = .{ .text = try dupeStr(self.allocator, val_str[1 .. val_str.len - 1]) };
                                    } else {
                                        const num = std.fmt.parseInt(i64, val_str, 10) catch {
                                            row.values[col_idx] = .{ .text = try dupeStr(self.allocator, val_str) };
                                            continue;
                                        };
                                        row.values[col_idx] = .{ .integer = num };
                                    }
                                }
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
                if (dt.if_exists) return .ok;
                return .{ .err = "table not found" };
            },
            .begin => {
                if (self.in_transaction) return .{ .err = "cannot start a transaction within a transaction" };
                try self.takeSnapshot();
                self.in_transaction = true;
                return .ok;
            },
            .commit => {
                if (!self.in_transaction) return .{ .err = "cannot commit - no transaction is active" };
                self.freeSnapshot();
                self.in_transaction = false;
                return .ok;
            },
            .rollback => {
                if (!self.in_transaction) return .{ .err = "cannot rollback - no transaction is active" };
                try self.restoreSnapshot();
                self.in_transaction = false;
                return .ok;
            },
            .alter_table => |alt| {
                switch (alt) {
                    .add_column => |ac| {
                        if (self.tables.getPtr(ac.table_name)) |table| {
                            // Extend columns array
                            const old_cols = table.columns;
                            const new_col_count = old_cols.len + 1;
                            var new_cols = self.allocator.alloc(Column, new_col_count) catch return .{ .err = "out of memory" };
                            @memcpy(new_cols[0..old_cols.len], old_cols);
                            new_cols[old_cols.len] = .{
                                .name = dupeStr(self.allocator, ac.column.name) catch return .{ .err = "out of memory" },
                                .col_type = ac.column.col_type,
                                .is_primary_key = false,
                            };
                            self.allocator.free(old_cols);
                            table.columns = new_cols;

                            // Extend each existing row with null_val
                            for (table.storage().scanMut()) |*row| {
                                const old_vals = row.values;
                                var new_vals = self.allocator.alloc(Value, new_col_count) catch return .{ .err = "out of memory" };
                                @memcpy(new_vals[0..old_vals.len], old_vals);
                                new_vals[old_vals.len] = .null_val;
                                self.allocator.free(old_vals);
                                row.values = new_vals;
                            }
                            return .ok;
                        }
                        return .{ .err = "table not found" };
                    },
                    .rename_to => |rn| {
                        if (self.tables.fetchRemove(rn.table_name)) |entry| {
                            var table = entry.value;
                            // Free old name and set new one
                            self.allocator.free(table.name);
                            table.name = dupeStr(self.allocator, rn.new_name) catch return .{ .err = "out of memory" };
                            self.tables.put(table.name, table) catch return .{ .err = "out of memory" };
                            return .ok;
                        }
                        return .{ .err = "table not found" };
                    },
                }
            },
            .union_select => |union_sel| {
                defer {
                    for (union_sel.selects) |select_sql| {
                        self.allocator.free(@constCast(select_sql));
                    }
                    self.allocator.free(union_sel.selects);
                }
                defer if (union_sel.order_by) |ob| {
                    for (ob.items) |item| {
                        if (item.expr) |e| self.freeExprDeep(e);
                    }
                    self.allocator.free(ob.items);
                };
                return self.executeUnion(union_sel);
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

test "CHECK constraint rejects invalid values" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE t1 (id INTEGER, score INTEGER CHECK(score >= 0));");
    const r1 = try db.execute("INSERT INTO t1 VALUES (1, 50);");
    try std.testing.expectEqual(ExecuteResult.ok, r1);

    const r2 = try db.execute("INSERT INTO t1 VALUES (2, -5);");
    switch (r2) {
        .err => |msg| try std.testing.expectEqualStrings("CHECK constraint failed", msg),
        else => return error.UnexpectedToken,
    }

    // Verify only valid row was inserted
    const r3 = try db.execute("SELECT * FROM t1;");
    switch (r3) {
        .rows => |rows| try std.testing.expectEqual(@as(usize, 1), rows.len),
        else => return error.UnexpectedToken,
    }
}
