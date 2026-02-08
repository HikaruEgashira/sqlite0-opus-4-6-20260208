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
        if (self.projected_rows) |pr| {
            self.allocator.free(pr);
            self.projected_rows = null;
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
                    .eq, .ne, .lt, .le, .gt, .ge, .like, .glob, .logical_and, .logical_or => unreachable,
                }
            },
            .aggregate => {
                // Aggregate expressions are evaluated differently (in computeAgg)
                return .null_val;
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
        }
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
                for (rows) |row| {
                    if (row.values[col_idx] == .integer) {
                        total += row.values[col_idx].integer;
                        has_value = true;
                    }
                }
                if (!has_value) return .null_val;
                return .{ .integer = total };
            },
            .avg => {
                var total: f64 = 0;
                var cnt: usize = 0;
                for (rows) |row| {
                    if (row.values[col_idx] == .integer) {
                        total += @floatFromInt(row.values[col_idx].integer);
                        cnt += 1;
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
                var first = true;
                for (rows) |row| {
                    if (row.values[col_idx] == .null_val) continue;
                    if (!first) {
                        result.appendSlice(self.allocator, separator) catch return .null_val;
                    }
                    const text = self.valueToText(row.values[col_idx]);
                    result.appendSlice(self.allocator, text) catch {
                        self.allocator.free(text);
                        return .null_val;
                    };
                    self.allocator.free(text);
                    first = false;
                }
                if (first) return .null_val; // all NULL
                return .{ .text = result.toOwnedSlice(self.allocator) catch return .null_val };
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
        var proj_values = try self.allocator.alloc([]Value, n_groups);
        var proj_rows = try self.allocator.alloc(Row, n_groups);

        for (0..n_groups) |gi| {
            var values = try self.allocator.alloc(Value, sel.select_exprs.len);
            const grp = group_rows.items[gi].items;
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
            proj_values[gi] = values;
            proj_rows[gi] = .{ .values = values };
        }

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

        // Apply HAVING filter
        if (sel.having) |having| {
            var write_idx: usize = 0;
            const count = proj_rows.len;
            for (0..count) |ri| {
                // Evaluate HAVING on already-computed projected values
                var having_val: ?Value = null;
                for (sel.select_exprs, 0..) |expr, ei| {
                    if (expr == .aggregate) {
                        const agg = expr.aggregate;
                        if (agg.func == having.func and std.mem.eql(u8, agg.arg, having.arg)) {
                            having_val = proj_rows[ri].values[ei];
                            break;
                        }
                    }
                }
                if (having_val == null) {
                    // HAVING aggregate not in SELECT list — skip filtering for this row
                    proj_rows[write_idx] = proj_rows[ri];
                    proj_values[write_idx] = proj_values[ri];
                    write_idx += 1;
                    continue;
                }
                const hv = having_val.?;
                const compare_val = Table.parseRawValue(having.value);
                if (Table.compareValues(hv, compare_val, having.op)) {
                    proj_rows[write_idx] = proj_rows[ri];
                    proj_values[write_idx] = proj_values[ri];
                    write_idx += 1;
                } else {
                    // Free filtered-out row's values
                    self.allocator.free(proj_values[ri]);
                }
            }
            proj_rows = self.allocator.realloc(proj_rows, write_idx) catch proj_rows;
            proj_values = self.allocator.realloc(proj_values, write_idx) catch proj_values;
        }

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
        if (sel.join != null) {
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

            if (has_agg) {
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
        return .{ .rows = proj_rows };
    }

    fn executeJoin(self: *Database, sel: Statement.Select) !ExecuteResult {
        const join = sel.join.?;
        const left_table = self.tables.get(sel.table_name) orelse return .{ .err = "table not found" };
        const right_table = self.tables.get(join.table_name) orelse return .{ .err = "table not found" };

        // Build alias-to-table mapping for ON clause resolution
        const left_alias = sel.table_alias orelse sel.table_name;
        const right_alias = join.table_alias orelse join.table_name;

        const left_col_count = left_table.columns.len;
        const right_col_count = right_table.columns.len;
        const total_cols = left_col_count + right_col_count;

        var joined_rows: std.ArrayList(Row) = .{};
        var joined_values: std.ArrayList([]Value) = .{};

        const left_rows = left_table.storage().scan();
        const right_rows = right_table.storage().scan();

        if (join.join_type == .cross) {
            // CROSS JOIN: Cartesian product (no ON condition)
            for (left_rows) |left_row| {
                for (right_rows) |right_row| {
                    var values = try self.allocator.alloc(Value, total_cols);
                    @memcpy(values[0..left_col_count], left_row.values[0..left_col_count]);
                    @memcpy(values[left_col_count..total_cols], right_row.values[0..right_col_count]);
                    try joined_values.append(self.allocator, values);
                    try joined_rows.append(self.allocator, .{ .values = values });
                }
            }
        } else {
            // Resolve join columns using alias mapping
            var left_join_col: ?usize = null;
            var right_join_col: ?usize = null;

            if (std.mem.eql(u8, join.left_table, left_alias) or std.mem.eql(u8, join.left_table, sel.table_name)) {
                left_join_col = left_table.findColumnIndex(join.left_column);
                right_join_col = right_table.findColumnIndex(join.right_column);
            } else if (std.mem.eql(u8, join.left_table, right_alias) or std.mem.eql(u8, join.left_table, join.table_name)) {
                left_join_col = left_table.findColumnIndex(join.right_column);
                right_join_col = right_table.findColumnIndex(join.left_column);
            } else {
                left_join_col = left_table.findColumnIndex(join.left_column) orelse left_table.findColumnIndex(join.right_column);
                right_join_col = right_table.findColumnIndex(join.right_column) orelse right_table.findColumnIndex(join.left_column);
            }

            const left_jc = left_join_col orelse return .{ .err = "join column not found" };
            const right_jc = right_join_col orelse return .{ .err = "join column not found" };

            // Nested loop join
            for (left_rows) |left_row| {
                var matched = false;
                for (right_rows) |right_row| {
                    if (compareValuesOrder(left_row.values[left_jc], right_row.values[right_jc]) == .eq) {
                        matched = true;
                    var values = try self.allocator.alloc(Value, total_cols);
                    @memcpy(values[0..left_col_count], left_row.values[0..left_col_count]);
                    @memcpy(values[left_col_count..total_cols], right_row.values[0..right_col_count]);
                    try joined_values.append(self.allocator, values);
                    try joined_rows.append(self.allocator, .{ .values = values });
                }
            }
            // LEFT JOIN: output left row with NULLs for right columns
            if (!matched and join.join_type == .left) {
                var values = try self.allocator.alloc(Value, total_cols);
                @memcpy(values[0..left_col_count], left_row.values[0..left_col_count]);
                for (left_col_count..total_cols) |i| {
                    values[i] = .{ .null_val = {} };
                }
                try joined_values.append(self.allocator, values);
                try joined_rows.append(self.allocator, .{ .values = values });
            }
        }
        } // end else (non-cross join)

        // Apply WHERE filter using where_expr
        if (sel.where_expr) |we| {
            // Build a temporary combined-column Table for evalExpr
            var combined_cols = try self.allocator.alloc(Column, total_cols);
            defer self.allocator.free(combined_cols);
            @memcpy(combined_cols[0..left_col_count], left_table.columns);
            @memcpy(combined_cols[left_col_count..total_cols], right_table.columns);
            var tmp_table = Table.init(self.allocator, "joined", combined_cols);
            // Filter joined rows using evalExpr
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
            // Resolve all ORDER BY columns to joined-row indices
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
                            if (std.mem.eql(u8, qr.table, left_alias) or std.mem.eql(u8, qr.table, sel.table_name)) {
                                col_idx = left_table.findColumnIndex(qr.column);
                            } else if (std.mem.eql(u8, qr.table, right_alias) or std.mem.eql(u8, qr.table, join.table_name)) {
                                if (right_table.findColumnIndex(qr.column)) |ri| col_idx = left_col_count + ri;
                            }
                        },
                        .column_ref => |name| {
                            col_idx = left_table.findColumnIndex(name);
                            if (col_idx == null) {
                                if (right_table.findColumnIndex(name)) |ri| col_idx = left_col_count + ri;
                            }
                        },
                        else => {},
                    }
                } else if (item.column.len > 0) {
                    col_idx = left_table.findColumnIndex(item.column);
                    if (col_idx == null) {
                        if (right_table.findColumnIndex(item.column)) |ri| col_idx = left_col_count + ri;
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
                    if (std.mem.eql(u8, qr.table, left_alias) or std.mem.eql(u8, qr.table, sel.table_name)) {
                        if (left_table.findColumnIndex(qr.column)) |ci| {
                            col_indices[i] = ci;
                            found = true;
                        }
                    } else if (std.mem.eql(u8, qr.table, right_alias) or std.mem.eql(u8, qr.table, join.table_name)) {
                        if (right_table.findColumnIndex(qr.column)) |ci| {
                            col_indices[i] = left_col_count + ci;
                            found = true;
                        }
                    }
                }
            }

            if (!found) {
                for (left_table.columns, 0..) |tbl_col, j| {
                    if (std.mem.eql(u8, sel_col, tbl_col.name)) {
                        col_indices[i] = j;
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                for (right_table.columns, 0..) |tbl_col, j| {
                    if (std.mem.eql(u8, sel_col, tbl_col.name)) {
                        col_indices[i] = left_col_count + j;
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                // Free allocated join values
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

        // Free original join values (projection made copies of needed values)
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
        self.projected_rows = null;
        self.projected_values = null;
        self.projected_texts = null;

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

        return values;
    }

    fn executeInsertSelect(self: *Database, table: *Table, select_sql: []const u8) !ExecuteResult {
        // Save and restore projected state
        const saved_rows = self.projected_rows;
        const saved_values = self.projected_values;
        const saved_texts = self.projected_texts;
        self.projected_rows = null;
        self.projected_values = null;
        self.projected_texts = null;

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
                return .{ .err = e };
            },
            .ok => {},
        }

        self.freeProjected();
        self.projected_rows = saved_rows;
        self.projected_values = saved_values;
        self.projected_texts = saved_texts;
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

            for (items, 0..) |item, i| {
                col_indices[i] = tbl.findColumnIndex(item.column) orelse return;
                desc_flags[i] = item.order == .desc;
            }

            const MultiSortCtx = struct {
                indices: []const usize,
                descs: []const bool,

                fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                    for (ctx.indices, ctx.descs) |col_idx, is_desc| {
                        const va = a.values[col_idx];
                        const vb = b.values[col_idx];
                        const cmp = compareValuesOrder(va, vb);
                        if (cmp == .eq) continue;
                        if (is_desc) return cmp == .gt;
                        return cmp == .lt;
                    }
                    return false;
                }
            };

            std.mem.sort(Row, rows, MultiSortCtx{ .indices = col_indices, .descs = desc_flags }, MultiSortCtx.lessThan);
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
                const table_name = try dupeStr(self.allocator, ct.table_name);
                var columns = try self.allocator.alloc(Column, ct.columns.len);
                for (ct.columns, 0..) |col, i| {
                    columns[i] = .{
                        .name = try dupeStr(self.allocator, col.name),
                        .col_type = col.col_type,
                        .is_primary_key = col.is_primary_key,
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
            .insert => |ins| {
                defer if (ins.select_sql == null) self.allocator.free(ins.values);
                defer if (ins.select_sql) |s| self.allocator.free(@constCast(s));
                defer {
                    for (ins.extra_rows) |row_values| {
                        self.allocator.free(row_values);
                    }
                    self.allocator.free(ins.extra_rows);
                }
                if (self.tables.getPtr(ins.table_name)) |table| {
                    if (ins.select_sql) |select_sql| {
                        return self.executeInsertSelect(table, select_sql);
                    }
                    if (ins.replace_mode) {
                        self.replaceRow(table, ins.values);
                    } else if (ins.ignore_mode) {
                        if (!self.hasPkConflict(table, ins.values)) {
                            try table.insertRow(ins.values);
                        }
                    } else {
                        try table.insertRow(ins.values);
                    }
                    for (ins.extra_rows) |row_values| {
                        if (ins.replace_mode) {
                            self.replaceRow(table, row_values);
                        } else if (ins.ignore_mode) {
                            if (!self.hasPkConflict(table, row_values)) {
                                try table.insertRow(row_values);
                            }
                        } else {
                            try table.insertRow(row_values);
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
