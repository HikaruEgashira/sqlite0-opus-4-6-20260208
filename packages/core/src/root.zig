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

const convert = @import("convert.zig");

const dupeStr = value_mod.dupeStr;
const compareValuesOrder = value_mod.compareValuesOrder;
const rowsEqual = value_mod.rowsEqual;
const likeMatch = value_mod.likeMatch;
const globMatch = value_mod.globMatch;

const AliasOffset = struct { name: []const u8, alias: []const u8, offset: usize, table: *const Table };

pub const Database = struct {
    tables: std.StringHashMap(Table),
    views: std.StringHashMap([]const u8), // view_name -> SELECT SQL
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
    // Outer context for correlated subqueries
    outer_context: ?struct {
        table: *const Table,
        row: Row,
        table_name: []const u8,
        table_alias: ?[]const u8,
    } = null,
    // Current table alias (for correlated subquery context)
    current_table_alias: ?[]const u8 = null,
    // JOIN alias offsets for resolving qualified aggregate column refs
    join_alias_offsets: ?[]const AliasOffset = null,

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{
            .tables = std.StringHashMap(Table).init(allocator),
            .views = std.StringHashMap([]const u8).init(allocator),
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
        // Free views
        var vit = self.views.iterator();
        while (vit.next()) |entry| {
            self.allocator.free(@constCast(entry.key_ptr.*));
            self.allocator.free(@constCast(entry.value_ptr.*));
        }
        self.views.deinit();
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

    const ProjectedState = struct {
        rows: ?[]Row,
        values: ?[][]Value,
        texts: ?[][]const u8,
        col_names: ?[][]const u8,
    };

    fn saveProjectedState(self: *Database) ProjectedState {
        const state = ProjectedState{
            .rows = self.projected_rows,
            .values = self.projected_values,
            .texts = self.projected_texts,
            .col_names = self.projected_column_names,
        };
        self.projected_rows = null;
        self.projected_values = null;
        self.projected_texts = null;
        self.projected_column_names = null;
        return state;
    }

    fn restoreProjectedState(self: *Database, state: ProjectedState) void {
        self.projected_rows = state.rows;
        self.projected_values = state.values;
        self.projected_texts = state.texts;
        self.projected_column_names = state.col_names;
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
                    .is_unique = col.is_unique,
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
                    .is_unique = col.is_unique,
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
                    resolved_args[i] = try self.evalGroupExpr(arg, tbl, group_rows);
                    n_resolved = i + 1;
                }
                // Evaluate scalar function with resolved values
                return self.evalScalarFuncValues(sf.func, resolved_args[0..sf.args.len]);
            },
            .case_when => |cw| {
                for (cw.conditions, 0..) |cond, i| {
                    const cond_val = try self.evalGroupExpr(cond, tbl, group_rows);
                    defer if (cond_val == .text) self.allocator.free(cond_val.text);
                    if (self.valueToBool(cond_val)) {
                        return self.evalGroupExpr(cw.results[i], tbl, group_rows);
                    }
                }
                if (cw.else_result) |else_expr| {
                    return self.evalGroupExpr(else_expr, tbl, group_rows);
                }
                return .null_val;
            },
            .unary_op => |un| {
                const operand_val = try self.evalGroupExpr(un.operand, tbl, group_rows);
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
                // Try integer first, then float
                if (self.valueToI64(val)) |n| {
                    return .{ .integer = if (n < 0) -n else n };
                }
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(@abs(f));
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
                // SQLite SUBSTR: 1-based positions, negative means from end
                // Negative length means chars before the position
                const text_len: i64 = @intCast(text.len);
                // Convert negative start: -N means Nth from end (1-based)
                const y: i64 = if (start_raw < 0) text_len + start_raw + 1 else start_raw;
                // Default length: rest of string from y
                var z: i64 = text_len - y + 1;
                if (args.len == 3) {
                    const len_val = try self.evalExpr(args[2], tbl, row);
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
                // ROUND(x) or ROUND(x, n) — always returns real in SQLite
                if (args.len == 0 or args.len > 2) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const float_val = self.valueToF64(val) orelse return .null_val;
                var precision: i64 = 0; // default: round to integer
                if (args.len == 2) {
                    const prec_val = try self.evalExpr(args[1], tbl, row);
                    defer if (prec_val == .text) self.allocator.free(prec_val.text);
                    precision = self.valueToI64(prec_val) orelse 0;
                }
                const factor = std.math.pow(f64, 10.0, @as(f64, @floatFromInt(precision)));
                const rounded = @round(float_val * factor) / factor;
                return self.formatFloat(rounded);
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
                if (self.valueToI64(val)) |n| {
                    return .{ .integer = if (n > 0) @as(i64, 1) else if (n < 0) @as(i64, -1) else 0 };
                }
                const f = self.valueToF64(val) orelse return .null_val;
                return .{ .integer = if (f > 0) @as(i64, 1) else if (f < 0) @as(i64, -1) else 0 };
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
            .ceil_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                if (val == .integer) return val;
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(@ceil(f));
            },
            .floor_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                if (val == .integer) return val;
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(@floor(f));
            },
            .sqrt_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                if (f < 0) return .null_val;
                return self.formatFloat(@sqrt(f));
            },
            .power_fn => {
                if (args.len != 2) return .null_val;
                const base_val = try self.evalExpr(args[0], tbl, row);
                defer if (base_val == .text) self.allocator.free(base_val.text);
                const exp_val = try self.evalExpr(args[1], tbl, row);
                defer if (exp_val == .text) self.allocator.free(exp_val.text);
                if (base_val == .null_val or exp_val == .null_val) return .null_val;
                const base_f = self.valueToF64(base_val) orelse return .null_val;
                const exp_f = self.valueToF64(exp_val) orelse return .null_val;
                return self.formatFloat(std.math.pow(f64, base_f, exp_f));
            },
            .log_fn => {
                // LOG(x) = log10(x), LOG(base, x) = log_base(x)
                if (args.len == 1) {
                    const val = try self.evalExpr(args[0], tbl, row);
                    defer if (val == .text) self.allocator.free(val.text);
                    if (val == .null_val) return .null_val;
                    const f = self.valueToF64(val) orelse return .null_val;
                    if (f <= 0) return .null_val;
                    return self.formatFloat(@log10(f));
                } else if (args.len == 2) {
                    const base_val = try self.evalExpr(args[0], tbl, row);
                    defer if (base_val == .text) self.allocator.free(base_val.text);
                    const x_val = try self.evalExpr(args[1], tbl, row);
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
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                if (f <= 0) return .null_val;
                return self.formatFloat(@log2(f));
            },
            .ln_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                if (f <= 0) return .null_val;
                return self.formatFloat(@log(f));
            },
            .exp_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(@exp(f));
            },
            .acos_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                if (f < -1 or f > 1) return .null_val;
                return self.formatFloat(std.math.acos(f));
            },
            .asin_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                if (f < -1 or f > 1) return .null_val;
                return self.formatFloat(std.math.asin(f));
            },
            .atan_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(std.math.atan(f));
            },
            .atan2_fn => {
                if (args.len != 2) return .null_val;
                const y_val = try self.evalExpr(args[0], tbl, row);
                defer if (y_val == .text) self.allocator.free(y_val.text);
                const x_val = try self.evalExpr(args[1], tbl, row);
                defer if (x_val == .text) self.allocator.free(x_val.text);
                if (y_val == .null_val or x_val == .null_val) return .null_val;
                const y = self.valueToF64(y_val) orelse return .null_val;
                const x = self.valueToF64(x_val) orelse return .null_val;
                return self.formatFloat(std.math.atan2(y, x));
            },
            .cos_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(@cos(f));
            },
            .sin_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
                defer if (val == .text) self.allocator.free(val.text);
                if (val == .null_val) return .null_val;
                const f = self.valueToF64(val) orelse return .null_val;
                return self.formatFloat(@sin(f));
            },
            .tan_fn => {
                if (args.len != 1) return .null_val;
                const val = try self.evalExpr(args[0], tbl, row);
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
                const a_val = try self.evalExpr(args[0], tbl, row);
                defer if (a_val == .text) self.allocator.free(a_val.text);
                const b_val = try self.evalExpr(args[1], tbl, row);
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
    /// Does NOT free the input values — caller is responsible for cleanup.
    /// Returns a new Value (caller owns returned text).
    fn evalScalarFuncValues(self: *Database, func: ScalarFunc, vals: []const Value) !Value {
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
                const result = dupeStr(self.allocator, trimmed) catch { self.allocator.free(text); return .null_val; };
                self.allocator.free(text);
                return .{ .text = result };
            },
            .ltrim => {
                if (vals.len == 0) return .null_val;
                if (vals[0] == .null_val) return .null_val;
                const text = self.valueToText(vals[0]);
                const trimmed = std.mem.trimLeft(u8, text, " ");
                const result = dupeStr(self.allocator, trimmed) catch { self.allocator.free(text); return .null_val; };
                self.allocator.free(text);
                return .{ .text = result };
            },
            .rtrim => {
                if (vals.len == 0) return .null_val;
                if (vals[0] == .null_val) return .null_val;
                const text = self.valueToText(vals[0]);
                const trimmed = std.mem.trimRight(u8, text, " ");
                const result = dupeStr(self.allocator, trimmed) catch { self.allocator.free(text); return .null_val; };
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
                const start_raw = self.valueToI64(vals[1]) orelse { self.allocator.free(text); return .null_val; };
                const text_len: i64 = @intCast(text.len);
                const y: i64 = if (start_raw < 0) text_len + start_raw + 1 else start_raw;
                var z: i64 = text_len - y + 1;
                if (vals.len == 3) z = self.valueToI64(vals[2]) orelse { self.allocator.free(text); return .null_val; };
                var r_start: i64 = undefined;
                var r_end: i64 = undefined;
                if (z >= 0) { r_start = y; r_end = y + z; } else { r_start = y + z; r_end = y; }
                if (r_start < 1) r_start = 1;
                if (r_end > text_len + 1) r_end = text_len + 1;
                if (r_start >= r_end) { self.allocator.free(text); return .{ .text = try dupeStr(self.allocator, "") }; }
                const s: usize = @intCast(r_start - 1);
                const e: usize = @intCast(r_end - 1);
                const result = dupeStr(self.allocator, text[s..e]) catch { self.allocator.free(text); return .null_val; };
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
                    self.allocator.free(text); self.allocator.free(from); self.allocator.free(to); return .null_val;
                };
                self.allocator.free(text); self.allocator.free(from); self.allocator.free(to);
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
            if (wc.is_recursive) {
                try self.executeRecursiveCTE(cte, &cte_names);
            } else {
                try self.executeNonRecursiveCTE(cte, &cte_names);
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

    fn executeNonRecursiveCTE(self: *Database, cte: Statement.CteDef, cte_names: *std.ArrayList([]const u8)) !void {
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
                try self.createCTETable(cte.name, rows, cte.col_names);
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

    fn executeRecursiveCTE(self: *Database, cte: Statement.CteDef, cte_names: *std.ArrayList([]const u8)) !void {
        // Split query on UNION ALL to separate anchor and recursive parts
        const query = cte.query_sql;
        const split = findUnionAllSplit(query);
        if (split == null) {
            return self.executeNonRecursiveCTE(cte, cte_names);
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
                try self.createCTETable(cte.name, rows, cte.col_names);
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

    fn createCTETable(self: *Database, name: []const u8, rows: []const Row, explicit_col_names: ?[]const []const u8) !void {
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

    fn findUnionAllSplit(sql: []const u8) ?[2]usize {
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
                    try self.computeWindowAggregates(wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows, alloc_texts);
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

    /// Check if a row with matching UNIQUE column already exists
    fn hasUniqueConflict(self: *Database, table: *Table, raw_values: []const []const u8) bool {
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
    // Delegated to convert.zig
    fn valueToBool(_: *Database, val: Value) bool {
        return convert.valueToBool(val);
    }
    fn freeWhereExpr(self: *Database, expr: *const Expr) void {
        convert.freeExprDeep(self.allocator, expr);
    }
    fn freeExprDeep(self: *Database, expr: *const Expr) void {
        convert.freeExprDeep(self.allocator, expr);
    }
    fn valueToText(self: *Database, val: Value) []const u8 {
        return convert.valueToText(self.allocator, val);
    }
    fn valueToI64(_: *Database, val: Value) ?i64 {
        return convert.valueToI64(val);
    }
    fn valueToF64(_: *Database, val: Value) ?f64 {
        return convert.valueToF64(val);
    }
    fn isFloatValue(_: *Database, val: Value) bool {
        return convert.isFloatValue(val);
    }
    fn formatFloat(self: *Database, f_in: f64) Value {
        return convert.formatFloat(self.allocator, f_in);
    }

    /// Resolve a column argument for aggregate (handles both "col" and "table.col" formats)
    fn resolveAggColIdx(self: *const Database, tbl: *const Table, arg: []const u8) ?usize {
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

    fn computeAgg(self: *Database, func: AggFunc, arg: []const u8, tbl: *const Table, rows: []const Row, separator: []const u8, is_distinct: bool) !Value {
        if (func == .count) {
            if (std.mem.eql(u8, arg, "*")) {
                return .{ .integer = @intCast(rows.len) };
            }
            const col_idx = self.resolveAggColIdx(tbl, arg) orelse return error.UnexpectedToken;
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

        const col_idx = self.resolveAggColIdx(tbl, arg) orelse return error.UnexpectedToken;

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

    fn executeGroupByAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row, gb_exprs: []const *const Expr) !ExecuteResult {
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
                        values[ei] = try self.computeAgg(agg.func, agg.arg, tbl, grp, agg.separator, agg.distinct);
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
            try self.collectAggTexts(sel.select_exprs, proj_values);
            self.allocator.free(proj_rows);
            return .{ .rows = sliced_rows };
        }

        self.projected_rows = proj_rows;
        self.projected_values = proj_values;
        try self.collectAggTexts(sel.select_exprs, proj_values);

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

    fn executeSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
        // Handle table-less SELECT (e.g., SELECT ABS(-10);)
        if (sel.table_name.len == 0) {
            return self.executeTablelessSelect(sel);
        }

        // Handle sqlite_master / sqlite_schema virtual table
        if (std.mem.eql(u8, sel.table_name, "sqlite_master") or std.mem.eql(u8, sel.table_name, "sqlite_schema")) {
            return self.executeSqliteMaster(sel);
        }

        // Resolve derived table (FROM subquery)
        if (sel.subquery_sql) |sq_sql| {
            defer self.allocator.free(@constCast(sq_sql));
            try self.materializeView(sel.table_name, sq_sql);
        }

        // Resolve view: if table_name is a view, materialize it as a temp table
        if (!self.tables.contains(sel.table_name)) {
            if (self.views.get(sel.table_name)) |view_sql_str| {
                try self.materializeView(sel.table_name, view_sql_str);
            }
        }

        // Handle JOIN queries
        if (sel.joins.len > 0) {
            return self.executeJoin(sel);
        }

        if (self.tables.get(sel.table_name)) |table| {
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

    /// Materialize a view as a temporary table for querying.
    /// Cleanup is deferred to freeProjected via temp_cte_names.
    fn materializeView(self: *Database, view_name: []const u8, view_sql: []const u8) !void {
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
                    tbl.storage().append(self.allocator, .{ .values = new_values }) catch {};
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
        var alias_offsets: std.ArrayList(AliasOffset) = .{};
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

    /// Build RETURNING result from source rows
    fn buildReturningResult(self: *Database, table: *const Table, src_rows: []const Row, returning_cols: []const []const u8) !ExecuteResult {
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
                        .is_unique = col.is_unique,
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
                defer if (ins.select_sql == null and ins.value_exprs.len == 0) self.allocator.free(ins.values);
                defer if (ins.select_sql) |s| self.allocator.free(@constCast(s));
                defer {
                    for (ins.extra_rows) |row_values| {
                        self.allocator.free(row_values);
                    }
                    self.allocator.free(ins.extra_rows);
                }
                defer {
                    for (ins.value_exprs) |expr_row| {
                        for (expr_row) |e| self.freeExprDeep(e);
                        self.allocator.free(expr_row);
                    }
                    if (ins.value_exprs.len > 0) self.allocator.free(ins.value_exprs);
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
                defer if (ins.returning) |ret| self.allocator.free(ret);
                if (self.tables.getPtr(ins.table_name)) |table| {
                    if (ins.select_sql) |select_sql| {
                        return self.executeInsertSelect(table, select_sql);
                    }
                    const initial_len = table.storage().len();
                    if (ins.default_values) {
                        // INSERT INTO t DEFAULT VALUES: use defaults for all columns
                        var def_values = try self.allocator.alloc([]const u8, table.columns.len);
                        defer self.allocator.free(def_values);
                        for (table.columns, 0..) |col, i| {
                            def_values[i] = col.default_value orelse "NULL";
                        }
                        try table.insertRow(def_values);
                        if (ins.returning) |ret| {
                            const rows = table.storage().scan();
                            return self.buildReturningResult(table, rows[initial_len..], ret);
                        }
                        return .ok;
                    }

                    // Expression-based VALUES: evaluate each row's expressions
                    if (ins.value_exprs.len > 0) {
                        var dummy_table = Table.init(self.allocator, "", &.{});
                        const dummy_row = Row{ .values = &.{} };
                        for (ins.value_exprs) |expr_row| {
                            var str_values = try self.allocator.alloc([]const u8, expr_row.len);
                            defer self.allocator.free(str_values);
                            var alloc_strs: std.ArrayList([]const u8) = .{};
                            defer {
                                for (alloc_strs.items) |s| self.allocator.free(s);
                                alloc_strs.deinit(self.allocator);
                            }
                            for (expr_row, 0..) |expr, i| {
                                const val = try self.evalExpr(expr, &dummy_table, dummy_row);
                                switch (val) {
                                    .integer => |n| {
                                        const s = try std.fmt.allocPrint(self.allocator, "{d}", .{n});
                                        str_values[i] = s;
                                        alloc_strs.append(self.allocator, s) catch {};
                                    },
                                    .text => |t| {
                                        // Wrap text in quotes for insertRow to recognize as text
                                        const s = try std.fmt.allocPrint(self.allocator, "'{s}'", .{t});
                                        self.allocator.free(t);
                                        str_values[i] = s;
                                        alloc_strs.append(self.allocator, s) catch {};
                                    },
                                    .null_val => {
                                        str_values[i] = "NULL";
                                    },
                                }
                            }
                            // Expand partial column lists with default values
                            const row_values = if (ins.column_names.len > 0)
                                self.expandInsertValues(table, ins.column_names, str_values) catch str_values
                            else
                                str_values;
                            defer if (ins.column_names.len > 0 and row_values.ptr != str_values.ptr) self.allocator.free(row_values);
                            if (!try self.validateCheckConstraints(table, row_values)) {
                                return .{ .err = "CHECK constraint failed" };
                            }
                            if (ins.on_conflict) |oc| {
                                try self.executeUpsertRow(table, row_values, oc);
                            } else if (ins.replace_mode) {
                                self.replaceRow(table, row_values);
                            } else if (ins.ignore_mode) {
                                if (!self.hasPkConflict(table, row_values) and !self.hasUniqueConflict(table, row_values)) {
                                    try table.insertRow(row_values);
                                }
                            } else {
                                if (self.hasUniqueConflict(table, row_values)) {
                                    return .{ .err = "UNIQUE constraint failed" };
                                }
                                try table.insertRow(row_values);
                            }
                        }
                        if (ins.returning) |ret| {
                            const rows = table.storage().scan();
                            return self.buildReturningResult(table, rows[initial_len..], ret);
                        }
                        return .ok;
                    }

                    // Legacy string-based VALUES path
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
                        if (!self.hasPkConflict(table, values) and !self.hasUniqueConflict(table, values)) {
                            try table.insertRow(values);
                        }
                    } else {
                        if (self.hasUniqueConflict(table, values)) {
                            return .{ .err = "UNIQUE constraint failed" };
                        }
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
                            if (!self.hasPkConflict(table, expanded) and !self.hasUniqueConflict(table, expanded)) {
                                try table.insertRow(expanded);
                            }
                        } else {
                            if (self.hasUniqueConflict(table, expanded)) {
                                return .{ .err = "UNIQUE constraint failed" };
                            }
                            try table.insertRow(expanded);
                        }
                    }
                    if (ins.returning) |ret| {
                        const rows = table.storage().scan();
                        return self.buildReturningResult(table, rows[initial_len..], ret);
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
                defer if (sel.group_by) |gb| {
                            for (gb) |e| self.freeExprDeep(e);
                            self.allocator.free(gb);
                        };
                return self.executeSelect(sel);
            },
            .delete => |del| {
                defer if (del.where_expr) |we| self.freeWhereExpr(we);
                defer if (del.returning) |ret| self.allocator.free(ret);
                if (self.tables.getPtr(del.table_name)) |table| {
                    // For RETURNING, capture matching rows before deleting
                    var captured_rows: std.ArrayList(Row) = .{};
                    defer captured_rows.deinit(self.allocator);

                    if (del.where_expr) |we| {
                        var i: usize = table.storage().len();
                        while (i > 0) {
                            i -= 1;
                            const rows = table.storage().scan();
                            const val = try self.evalExpr(we, table, rows[i]);
                            defer if (val == .text) self.allocator.free(val.text);
                            if (self.valueToBool(val)) {
                                if (del.returning != null) {
                                    captured_rows.append(self.allocator, rows[i]) catch {};
                                } else {
                                    table.freeRow(rows[i]);
                                }
                                _ = table.storage().orderedRemove(i);
                            }
                        }
                    } else if (del.where) |where| {
                        var i: usize = table.storage().len();
                        while (i > 0) {
                            i -= 1;
                            const rows = table.storage().scan();
                            if (table.matchesWhere(rows[i], where)) {
                                if (del.returning != null) {
                                    captured_rows.append(self.allocator, rows[i]) catch {};
                                } else {
                                    table.freeRow(rows[i]);
                                }
                                _ = table.storage().orderedRemove(i);
                            }
                        }
                    } else {
                        // DELETE without WHERE: delete all rows
                        const rows = table.storage().scan();
                        if (del.returning != null) {
                            for (rows) |row| {
                                captured_rows.append(self.allocator, row) catch {};
                            }
                        } else {
                            for (rows) |row| {
                                table.freeRow(row);
                            }
                        }
                        table.storage().clearRetainingCapacity();
                    }
                    if (del.returning) |ret| {
                        const result = try self.buildReturningResult(table, captured_rows.items, ret);
                        // Now free the captured rows
                        for (captured_rows.items) |row| {
                            table.freeRow(row);
                        }
                        return result;
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
                defer if (upd.returning) |ret| self.allocator.free(ret);
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

                    // Track updated row indices for RETURNING
                    var updated_indices: std.ArrayList(usize) = .{};
                    defer updated_indices.deinit(self.allocator);

                    for (table.storage().scanMut(), 0..) |*row, row_idx| {
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
                            if (upd.returning != null) {
                                updated_indices.append(self.allocator, row_idx) catch {};
                            }
                        }
                    }
                    if (upd.returning) |ret| {
                        const all_rows = table.storage().scan();
                        var updated_rows = try self.allocator.alloc(Row, updated_indices.items.len);
                        defer self.allocator.free(updated_rows);
                        for (updated_indices.items, 0..) |idx, i| {
                            updated_rows[i] = all_rows[idx];
                        }
                        return self.buildReturningResult(table, updated_rows, ret);
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
            .create_view => |cv| {
                defer self.allocator.free(@constCast(cv.select_sql));
                if (self.views.contains(cv.view_name)) {
                    if (cv.if_not_exists) return .ok;
                    return .{ .err = "view already exists" };
                }
                const name = try dupeStr(self.allocator, cv.view_name);
                const view_sql = try dupeStr(self.allocator, cv.select_sql);
                self.views.put(name, view_sql) catch return .{ .err = "out of memory" };
                return .ok;
            },
            .drop_view => |dv| {
                if (self.views.fetchRemove(dv.view_name)) |entry| {
                    self.allocator.free(@constCast(entry.key));
                    self.allocator.free(@constCast(entry.value));
                    return .ok;
                }
                if (dv.if_exists) return .ok;
                return .{ .err = "view not found" };
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
            .pragma => |pragma| {
                return self.executePragma(pragma);
            },
        }
    }

    fn executeSqliteMaster(self: *Database, sel: Statement.Select) !ExecuteResult {
        // Build sqlite_master rows: type, name, tbl_name, rootpage, sql
        var all_rows: std.ArrayList(Row) = .{};
        var texts: std.ArrayList([]const u8) = .{};
        defer all_rows.deinit(self.allocator);

        // Helper to track text allocations
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
            // Reconstruct CREATE TABLE sql
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

        // Create a temp table with sqlite_master schema for eval
        var master_cols = [_]Column{
            .{ .name = "type", .col_type = "TEXT", .is_primary_key = false },
            .{ .name = "name", .col_type = "TEXT", .is_primary_key = false },
            .{ .name = "tbl_name", .col_type = "TEXT", .is_primary_key = false },
            .{ .name = "rootpage", .col_type = "INTEGER", .is_primary_key = false },
            .{ .name = "sql", .col_type = "TEXT", .is_primary_key = false },
        };
        var tmp_table = Table.init(self.allocator, "sqlite_master", &master_cols);
        _ = &tmp_table;

        // Apply WHERE filter
        var filtered_rows: std.ArrayList(Row) = .{};
        defer filtered_rows.deinit(self.allocator);

        for (all_rows.items) |row| {
            var keep = true;
            if (sel.where_expr) |we| {
                const val = try self.evalExpr(we, &tmp_table, row);
                defer if (val == .text) self.allocator.free(val.text);
                keep = self.valueToBool(val);
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
        // Free value arrays not included in result
        for (0..start) |i| self.allocator.free(filtered_rows.items[i].values);
        for (start + count..filtered_rows.items.len) |i| self.allocator.free(filtered_rows.items[i].values);

        // Set column names
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

    fn executePragma(self: *Database, pragma: Statement.Pragma) !ExecuteResult {
        if (std.ascii.eqlIgnoreCase(pragma.name, "table_info")) {
            const table_name = pragma.arg orelse return .{ .err = "PRAGMA table_info requires a table name" };
            const table = self.tables.get(table_name) orelse return .{ .err = "no such table" };

            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            const col_count = table.columns.len;
            var result_rows = self.allocator.alloc(Row, col_count) catch return .{ .err = "out of memory" };
            var result_values = self.allocator.alloc([]Value, col_count) catch return .{ .err = "out of memory" };
            var texts: std.ArrayList([]const u8) = .{};

            for (table.columns, 0..) |col, i| {
                var vals = self.allocator.alloc(Value, 6) catch return .{ .err = "out of memory" };
                vals[0] = .{ .integer = @intCast(i) }; // cid
                const name_t = dupeStr(self.allocator, col.name) catch return .{ .err = "out of memory" };
                vals[1] = .{ .text = name_t };
                texts.append(self.allocator, name_t) catch {};
                const type_t = dupeStr(self.allocator, col.col_type) catch return .{ .err = "out of memory" };
                vals[2] = .{ .text = type_t };
                texts.append(self.allocator, type_t) catch {};
                vals[3] = .{ .integer = if (col.not_null) 1 else 0 }; // notnull
                if (col.default_value) |dv| {
                    const dv_t = dupeStr(self.allocator, dv) catch return .{ .err = "out of memory" };
                    vals[4] = .{ .text = dv_t };
                    texts.append(self.allocator, dv_t) catch {};
                } else {
                    vals[4] = .null_val;
                }
                vals[5] = .{ .integer = if (col.is_primary_key) 1 else 0 }; // pk
                result_values[i] = vals;
                result_rows[i] = .{ .values = vals };
            }

            // Set column names for display
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
            // PRAGMA table_list returns: schema, name, type
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
            // Unknown PRAGMAs - just return ok (SQLite silently ignores unknown pragmas)
            return .ok;
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
