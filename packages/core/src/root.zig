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
const pragma_mod = @import("pragma.zig");
const state_mod = @import("state.zig");
const window_mod = @import("window.zig");
const aggregate_mod = @import("aggregate.zig");
const select_mod = @import("select.zig");
const join_mod = @import("join.zig");
const union_mod = @import("union_ops.zig");
const cte_mod = @import("cte.zig");
const modify_mod = @import("modify.zig");
const eval_mod = @import("eval.zig");

const dupeStr = value_mod.dupeStr;
const compareValuesOrder = value_mod.compareValuesOrder;
const rowsEqual = value_mod.rowsEqual;
const likeMatch = value_mod.likeMatch;
const globMatch = value_mod.globMatch;

pub const AliasOffset = struct { name: []const u8, alias: []const u8, offset: usize, table: *const Table };

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
    // Last insert rowid and change counters
    last_insert_rowid: i64 = 0,
    last_changes: i64 = 0,
    total_changes_count: i64 = 0,

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

    pub fn freeProjected(self: *Database) void {
        state_mod.freeProjected(self);
    }

    const ProjectedState = state_mod.ProjectedState;

    pub fn saveProjectedState(self: *Database) ProjectedState {
        return state_mod.saveProjectedState(self);
    }

    pub fn restoreProjectedState(self: *Database, state: ProjectedState) void {
        state_mod.restoreProjectedState(self, state);
    }

    pub fn freeSnapshot(self: *Database) void {
        state_mod.freeSnapshot(self);
    }

    pub fn takeSnapshot(self: *Database) !void {
        return state_mod.takeSnapshot(self);
    }

    pub fn restoreSnapshot(self: *Database) !void {
        return state_mod.restoreSnapshot(self);
    }

    /// Evaluate an expression AST against a row
    pub fn evalExpr(self: *Database, expr: *const Expr, tbl: *const Table, row: Row) !Value {
        return eval_mod.evalExpr(self, expr, tbl, row);
    }

    const DateTimePart = eval_mod.DateTimePart;
    const DateTimeComponents = eval_mod.DateTimeComponents;

    pub fn evalGroupExpr(self: *Database, expr: *const Expr, tbl: *const Table, group_rows: []const Row) !Value {
        return eval_mod.evalGroupExpr(self, expr, tbl, group_rows);
    }

    fn evalScalarFunc(self: *Database, func: ScalarFunc, args: []const *const Expr, tbl: *const Table, row: Row) !Value {
        return eval_mod.evalScalarFunc(self, func, args, tbl, row);
    }

    fn evalScalarFuncValues(self: *Database, func: ScalarFunc, vals: []const Value) !Value {
        return eval_mod.evalScalarFuncValues(self, func, vals);
    }

    fn parseDateTimeInput(self: *Database, text: []const u8) ?DateTimeComponents {
        return eval_mod.parseDateTimeInput(self, text);
    }

    fn formatDateTime(self: *Database, dt: DateTimeComponents, part: DateTimePart) !Value {
        return eval_mod.formatDateTime(self, dt, part);
    }

    fn evalDateTimeFunc(self: *Database, args: []const *const Expr, tbl: *const Table, row: Row, part: DateTimePart) !Value {
        return eval_mod.evalDateTimeFunc(self, args, tbl, row, part);
    }

    fn evalStrftime(self: *Database, args: []const *const Expr, tbl: *const Table, row: Row) !Value {
        return eval_mod.evalStrftime(self, args, tbl, row);
    }


    /// Execute WITH (Common Table Expressions)
    pub fn executeWithCTE(self: *Database, wc: Statement.WithCTE) !ExecuteResult {
        return cte_mod.executeWithCTE(self, wc);
    }

    pub fn executeNonRecursiveCTE(self: *Database, cte: Statement.CteDef, cte_names: *std.ArrayList([]const u8)) !void {
        return cte_mod.executeNonRecursiveCTE(self, cte, cte_names);
    }

    pub fn executeRecursiveCTE(self: *Database, cte: Statement.CteDef, cte_names: *std.ArrayList([]const u8)) !void {
        return cte_mod.executeRecursiveCTE(self, cte, cte_names);
    }

    pub fn createCTETable(self: *Database, name: []const u8, rows: []const Row, explicit_col_names: ?[]const []const u8) !void {
        return cte_mod.createCTETable(self, name, rows, explicit_col_names);
    }

    pub fn findUnionAllSplit(sql: []const u8) ?[2]usize {
        return cte_mod.findUnionAllSplit(sql);
    }

    pub fn executeCreateTableAsSelect(self: *Database, table_name: []const u8, select_sql: []const u8) !ExecuteResult {
        return cte_mod.executeCreateTableAsSelect(self, table_name, select_sql);
    }

    pub fn evaluateWindowFunctions(
        self: *Database,
        result_exprs: []const *const Expr,
        proj_rows: []Row,
        proj_values: [][]Value,
        tbl: *const Table,
        src_rows: []const Row,
        alloc_texts: *std.ArrayList([]const u8),
    ) !void {
        return window_mod.evaluateWindowFunctions(self, result_exprs, proj_rows, proj_values, tbl, src_rows, alloc_texts);
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
        return window_mod.computeWindowAggregates(self, wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows, alloc_texts);
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
        return window_mod.computeWindowValueFunctions(self, wf, indices, col_idx, tbl, src_rows, proj_values, proj_rows);
    }

    pub fn expandInsertValues(self: *Database, table: *Table, col_names: []const []const u8, values: []const []const u8) ![]const []const u8 {
        return modify_mod.expandInsertValues(self, table, col_names, values);
    }

    pub fn hasPkConflict(self: *Database, table: *Table, raw_values: []const []const u8) bool {
        return modify_mod.hasPkConflict(self, table, raw_values);
    }

    pub fn hasUniqueConflict(self: *Database, table: *Table, raw_values: []const []const u8) bool {
        return modify_mod.hasUniqueConflict(self, table, raw_values);
    }

    pub fn validateCheckConstraints(self: *Database, table: *Table, raw_values: []const []const u8) !bool {
        return modify_mod.validateCheckConstraints(self, table, raw_values);
    }

    pub fn replaceRow(self: *Database, table: *Table, raw_values: []const []const u8) void {
        return modify_mod.replaceRow(self, table, raw_values);
    }

    pub fn executeUpsertRow(self: *Database, table: *Table, raw_values: []const []const u8, on_conflict: Statement.OnConflict) !void {
        return modify_mod.executeUpsertRow(self, table, raw_values, on_conflict);
    }

    pub fn valueToBool(_: *Database, val: Value) bool {
        return convert.valueToBool(val);
    }
    pub fn freeWhereExpr(self: *Database, expr: *const Expr) void {
        convert.freeExprDeep(self.allocator, expr);
    }
    pub fn freeExprDeep(self: *Database, expr: *const Expr) void {
        convert.freeExprDeep(self.allocator, expr);
    }
    pub fn valueToText(self: *Database, val: Value) []const u8 {
        return convert.valueToText(self.allocator, val);
    }
    pub fn valueToI64(_: *Database, val: Value) ?i64 {
        return convert.valueToI64(val);
    }
    pub fn valueToF64(_: *Database, val: Value) ?f64 {
        return convert.valueToF64(val);
    }
    pub fn isFloatValue(_: *Database, val: Value) bool {
        return convert.isFloatValue(val);
    }
    pub fn formatFloat(self: *Database, f_in: f64) Value {
        return convert.formatFloat(self.allocator, f_in);
    }

    /// Resolve a column argument for aggregate (handles both "col" and "table.col" formats)
    pub fn resolveAggColIdx(self: *const Database, tbl: *const Table, arg: []const u8) ?usize {
        return aggregate_mod.resolveAggColIdx(self, tbl, arg);
    }

    pub fn computeAgg(self: *Database, func: AggFunc, arg: []const u8, tbl: *const Table, rows: []const Row, separator: []const u8, is_distinct: bool) !Value {
        return aggregate_mod.computeAgg(self, func, arg, tbl, rows, separator, is_distinct);
    }

    pub fn collectAggTexts(self: *Database, sel_exprs: []const SelectExpr, all_values: []const []Value) !void {
        return aggregate_mod.collectAggTexts(self, sel_exprs, all_values);
    }

    pub fn executeAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row) !ExecuteResult {
        return aggregate_mod.executeAggregate(self, tbl, sel, rows);
    }

    pub fn executeGroupByAggregate(self: *Database, tbl: *const Table, sel: Statement.Select, rows: []const Row, gb_exprs: []const *const Expr) !ExecuteResult {
        return aggregate_mod.executeGroupByAggregate(self, tbl, sel, rows, gb_exprs);
    }

    pub fn executeSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
        return select_mod.executeSelect(self, sel);
    }

    pub fn materializeView(self: *Database, view_name: []const u8, view_sql: []const u8) !void {
        return select_mod.materializeView(self, view_name, view_sql);
    }

    pub fn executeTablelessSelect(self: *Database, sel: Statement.Select) !ExecuteResult {
        return select_mod.executeTablelessSelect(self, sel);
    }

    pub fn evaluateExprSelect(self: *Database, sel: Statement.Select, tbl: *const Table, result_rows: []const Row) !ExecuteResult {
        return select_mod.evaluateExprSelect(self, sel, tbl, result_rows);
    }

    pub fn projectColumns(self: *Database, sel: Statement.Select, table: *const Table, result_rows: []const Row) !ExecuteResult {
        return select_mod.projectColumns(self, sel, table, result_rows);
    }

    pub fn executeJoin(self: *Database, sel: Statement.Select) !ExecuteResult {
        return join_mod.executeJoin(self, sel);
    }

    pub fn executeSubquery(self: *Database, subquery_sql: []const u8) !?[]const Value {
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
    pub fn buildReturningResult(self: *Database, table: *const Table, src_rows: []const Row, returning_cols: []const []const u8) !ExecuteResult {
        return modify_mod.buildReturningResult(self, table, src_rows, returning_cols);
    }

    pub fn trackInsertChanges(self: *Database, table: *const Table, initial_len: usize) void {
        const new_len = table.storage().len();
        const changes = @as(i64, @intCast(new_len)) - @as(i64, @intCast(initial_len));
        self.last_changes = changes;
        self.total_changes_count += changes;
        if (changes > 0) self.last_insert_rowid = table.next_rowid - 1;
    }

    pub fn executeInsertSelect(self: *Database, table: *Table, select_sql: []const u8) !ExecuteResult {
        return modify_mod.executeInsertSelect(self, table, select_sql);
    }

    pub fn executeSetOperands(self: *Database, selects: []const []const u8, saved_rows: ?[]Row, saved_values: ?[][]Value, saved_texts: ?[][]const u8) !?std.ArrayList(std.ArrayList(Row)) {
        return union_mod.executeSetOperands(self, selects, saved_rows, saved_values, saved_texts);
    }

    pub fn executeUnion(self: *Database, union_sel: Statement.UnionSelect) !ExecuteResult {
        return union_mod.executeUnion(self, union_sel);
    }

    pub fn removeDuplicateRows(self: *Database, rows: *std.ArrayList(Row)) !void {
        return union_mod.removeDuplicateRows(self, rows);
    }

    pub fn rowsEqualUnion(self: *Database, row1: Row, row2: Row) bool {
        return union_mod.rowsEqualUnion(self, row1, row2);
    }

    pub fn valuesEqualUnion(self: *Database, v1: Value, v2: Value) bool {
        return union_mod.valuesEqualUnion(self, v1, v2);
    }

    pub fn sortRowsByOrderBy(self: *Database, tbl: *const Table, rows: []Row, items: []const OrderByItem) !void {
        return union_mod.sortRowsByOrderBy(self, tbl, rows, items);
    }

    pub fn rowComparator(_: OrderByClause, row1: Row, row2: Row) bool {
        return union_mod.rowComparator(row1, row2);
    }

    pub fn freeRow(self: *Database, row: Row) void {
        for (row.values) |val| {
            if (val == .text) {
                self.allocator.free(val.text);
            }
        }
        self.allocator.free(row.values);
    }

    pub fn matchesConditionWithSubquery(self: *Database, table: *const Table, row: Row, column: []const u8, op: CompOp, value: []const u8, subquery_sql: ?[]const u8) !bool {
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

    pub fn matchesWhereWithSubquery(self: *Database, table: *const Table, row: Row, where: WhereClause) !bool {
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
                        self.trackInsertChanges(table, initial_len);
                        if (ins.returning) |ret| {
                            const rows = table.storage().scan();
                            return self.buildReturningResult(table, rows[initial_len..], ret);
                        }
                        return .ok;
                    }

                    // Expression-based VALUES: evaluate each row's expressions
                    if (ins.value_exprs.len > 0) {
                        var dummy_table = Table.init(self.allocator, "", &.{});
                        defer dummy_table.row_storage.storage().deinit(self.allocator);
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
                        self.trackInsertChanges(table, initial_len);
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
                    self.trackInsertChanges(table, initial_len);
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
                    const pre_len = table.storage().len();
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
                    const post_len = table.storage().len();
                    const del_changes = @as(i64, @intCast(pre_len)) - @as(i64, @intCast(post_len));
                    self.last_changes = del_changes;
                    self.total_changes_count += del_changes;
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
                            updated_indices.append(self.allocator, row_idx) catch {};
                        }
                    }
                    self.last_changes = @intCast(updated_indices.items.len);
                    self.total_changes_count += @intCast(updated_indices.items.len);
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

    pub fn executeSqliteMaster(self: *Database, sel: Statement.Select) !ExecuteResult {
        return pragma_mod.executeSqliteMaster(self, sel);
    }

    pub fn executePragma(self: *Database, pragma: Statement.Pragma) !ExecuteResult {
        return pragma_mod.executePragma(self, pragma);
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
