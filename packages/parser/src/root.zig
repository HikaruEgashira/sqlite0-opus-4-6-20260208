const std = @import("std");
const tokenizer = @import("tokenizer");

pub const TokenType = tokenizer.TokenType;
pub const Token = tokenizer.Token;
pub const Tokenizer = tokenizer.Tokenizer;

pub const ColumnDef = struct {
    name: []const u8,
    col_type: []const u8,
    is_primary_key: bool,
};

pub const SortOrder = enum {
    asc,
    desc,
};

pub const OrderByClause = struct {
    column: []const u8,
    order: SortOrder,
};

pub const CompOp = enum {
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    is_null,
    is_not_null,
    in_subquery, // WHERE col IN (SELECT ...)
    like, // LIKE pattern matching
};

pub const LogicOp = enum {
    and_op,
    or_op,
};

pub const WhereCondition = struct {
    column: []const u8,
    op: CompOp,
    value: []const u8,
    subquery_sql: ?[]const u8 = null, // SQL text for subquery (IN / scalar)
};

pub const WhereClause = struct {
    column: []const u8,
    op: CompOp,
    value: []const u8,
    subquery_sql: ?[]const u8 = null, // SQL text for subquery
    // Additional conditions connected by AND/OR
    extra: []const WhereCondition,
    connectors: []const LogicOp,
};

pub const AggFunc = enum {
    count,
    sum,
    avg,
    min,
    max,
};

pub const BinOp = enum {
    add, // +
    sub, // -
    mul, // *
    div, // /
    concat, // ||
    eq, // =
    ne, // !=
    lt, // <
    le, // <=
    gt, // >
    ge, // >=
    like, // LIKE pattern matching
    glob, // GLOB pattern matching (case-sensitive)
    logical_and, // AND
    logical_or, // OR
};

pub const UnaryOp = enum {
    is_null,
    is_not_null,
    not,
};

pub const Expr = union(enum) {
    integer_literal: i64,
    string_literal: []const u8,
    column_ref: []const u8,
    null_literal: void,
    star: void, // for COUNT(*)
    binary_op: struct {
        op: BinOp,
        left: *const Expr,
        right: *const Expr,
    },
    aggregate: struct {
        func: AggFunc,
        arg: *const Expr, // inner expression (column_ref or star)
    },
    case_when: struct {
        conditions: []const *const Expr,  // WHEN conditions
        results: []const *const Expr,     // THEN results
        else_result: ?*const Expr,        // ELSE result (optional)
    },
    unary_op: struct {
        op: UnaryOp,
        operand: *const Expr,
    },
    in_list: struct {
        operand: *const Expr,
        subquery_sql: []const u8, // SQL text for IN (SELECT ...)
    },
    scalar_subquery: struct {
        subquery_sql: []const u8, // SQL text for (SELECT ...)
    },
};

pub const SelectExpr = union(enum) {
    column: []const u8,
    aggregate: struct {
        func: AggFunc,
        arg: []const u8, // column name or "*"
    },
    expr: *const Expr,
};

pub const SetOp = enum {
    union_all,
    union_distinct,
    intersect,
    except,
};

pub const JoinType = enum {
    inner,
    left,
};

pub const JoinClause = struct {
    join_type: JoinType,
    table_name: []const u8,
    left_table: []const u8,
    left_column: []const u8,
    right_table: []const u8,
    right_column: []const u8,
};

pub const HavingClause = struct {
    func: AggFunc,
    arg: []const u8,
    op: CompOp,
    value: []const u8,
};

pub const Statement = union(enum) {
    create_table: CreateTable,
    create_index: CreateIndex,
    insert: Insert,
    select_stmt: Select,
    union_select: UnionSelect,
    delete: Delete,
    update: Update,
    drop_table: DropTable,
    alter_table: AlterTable,
    begin: void,
    commit: void,
    rollback: void,

    pub const CreateTable = struct {
        table_name: []const u8,
        columns: []const ColumnDef,
    };

    pub const Insert = struct {
        table_name: []const u8,
        values: []const []const u8, // first row values (or empty for INSERT SELECT)
        extra_rows: []const []const []const u8, // additional rows for multi-row INSERT
        select_sql: ?[]const u8 = null, // INSERT INTO ... SELECT (raw SQL)
    };

    pub const Select = struct {
        table_name: []const u8,
        columns: []const []const u8, // empty = * (plain column names for backward compat)
        select_exprs: []const SelectExpr, // full expression list (includes aggregates)
        result_exprs: []const *const Expr, // parsed expression ASTs for each SELECT item
        distinct: bool,
        join: ?JoinClause,
        where: ?WhereClause,
        where_expr: ?*const Expr = null, // Expr-based WHERE (Phase 6c)
        group_by: ?[]const u8, // column name
        having: ?HavingClause,
        order_by: ?OrderByClause,
        limit: ?i64,
        offset: ?i64,
    };

    pub const UnionSelect = struct {
        selects: []const []const u8, // raw SQL for each SELECT (to be parsed recursively)
        set_op: SetOp, // UNION ALL, UNION, INTERSECT, or EXCEPT
        order_by: ?OrderByClause,
        limit: ?i64,
        offset: ?i64,
    };

    pub const Delete = struct {
        table_name: []const u8,
        where: ?WhereClause,
        where_expr: ?*const Expr = null,
    };

    pub const Update = struct {
        table_name: []const u8,
        set_columns: []const []const u8,
        set_values: []const []const u8,
        where: ?WhereClause,
        where_expr: ?*const Expr = null,
    };

    pub const DropTable = struct {
        table_name: []const u8,
    };

    pub const AlterTable = union(enum) {
        add_column: struct {
            table_name: []const u8,
            column: ColumnDef,
        },
        rename_to: struct {
            table_name: []const u8,
            new_name: []const u8,
        },
    };

    pub const CreateIndex = struct {
        index_name: []const u8,
        table_name: []const u8,
        column_name: []const u8,
    };
};

pub const ParseError = error{
    UnexpectedToken,
    UnexpectedEof,
    OutOfMemory,
};

pub const Parser = struct {
    tokens: []const Token,
    pos: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, tokens: []const Token) Parser {
        return .{
            .tokens = tokens,
            .pos = 0,
            .allocator = allocator,
        };
    }

    pub fn parse(self: *Parser) ParseError!Statement {
        const tok = self.peek();
        return switch (tok.type) {
            .kw_create => self.parseCreate(),
            .kw_insert => self.parseInsert(),
            .kw_select => self.parseSelect(),
            .kw_delete => self.parseDelete(),
            .kw_update => self.parseUpdate(),
            .kw_drop => self.parseDropTable(),
            .kw_alter => self.parseAlterTable(),
            .kw_begin => self.parseBegin(),
            .kw_commit => self.parseCommit(),
            .kw_rollback => self.parseRollback(),
            else => ParseError.UnexpectedToken,
        };
    }

    fn peek(self: *Parser) Token {
        if (self.pos >= self.tokens.len) {
            return .{ .type = .eof, .lexeme = "" };
        }
        return self.tokens[self.pos];
    }

    fn advance(self: *Parser) Token {
        const tok = self.peek();
        if (self.pos < self.tokens.len) {
            self.pos += 1;
        }
        return tok;
    }

    fn expect(self: *Parser, expected: TokenType) ParseError!Token {
        const tok = self.advance();
        if (tok.type != expected) {
            return ParseError.UnexpectedToken;
        }
        return tok;
    }

    fn parseCreate(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_create);
        if (self.peek().type == .kw_index) {
            return self.parseCreateIndex();
        }
        // Default: CREATE TABLE
        _ = try self.expect(.kw_table);
        return self.parseCreateTableBody();
    }

    fn parseCreateTableBody(self: *Parser) ParseError!Statement {
        // CREATE and TABLE already consumed by parseCreate
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.lparen);

        var columns: std.ArrayList(ColumnDef) = .{};

        while (true) {
            const col_name = try self.expect(.identifier);
            const col_type_tok = self.advance();
            const col_type_str: []const u8 = switch (col_type_tok.type) {
                .kw_integer => "INTEGER",
                .kw_text => "TEXT",
                else => return ParseError.UnexpectedToken,
            };

            var is_pk = false;
            if (self.peek().type == .kw_primary) {
                _ = self.advance();
                _ = try self.expect(.kw_key);
                is_pk = true;
            }

            columns.append(self.allocator, .{
                .name = col_name.lexeme,
                .col_type = col_type_str,
                .is_primary_key = is_pk,
            }) catch return ParseError.OutOfMemory;

            if (self.peek().type == .comma) {
                _ = self.advance();
            } else {
                break;
            }
        }

        _ = try self.expect(.rparen);
        _ = try self.expect(.semicolon);

        return Statement{
            .create_table = .{
                .table_name = name_tok.lexeme,
                .columns = columns.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            },
        };
    }

    fn parseCreateIndex(self: *Parser) ParseError!Statement {
        // CREATE already consumed, INDEX is next
        _ = try self.expect(.kw_index);
        const idx_name = try self.expect(.identifier);
        _ = try self.expect(.kw_on);
        const tbl_name = try self.expect(.identifier);
        _ = try self.expect(.lparen);
        const col_name = try self.expect(.identifier);
        _ = try self.expect(.rparen);
        _ = try self.expect(.semicolon);
        return Statement{ .create_index = .{
            .index_name = idx_name.lexeme,
            .table_name = tbl_name.lexeme,
            .column_name = col_name.lexeme,
        } };
    }

    fn parseInsert(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_insert);
        _ = try self.expect(.kw_into);
        const name_tok = try self.expect(.identifier);

        // INSERT INTO ... SELECT or INSERT INTO ... VALUES
        if (self.peek().type == .kw_select) {
            const select_sql = try self.extractInsertSelectSQL();
            return Statement{
                .insert = .{
                    .table_name = name_tok.lexeme,
                    .values = &.{},
                    .extra_rows = &.{},
                    .select_sql = select_sql,
                },
            };
        }

        _ = try self.expect(.kw_values);

        const first_row = try self.parseValueTuple();

        // Parse additional rows: , (val1, val2, ...)
        var extra_rows: std.ArrayList([]const []const u8) = .{};
        while (self.peek().type == .comma) {
            _ = self.advance();
            const row = try self.parseValueTuple();
            extra_rows.append(self.allocator, row) catch return ParseError.OutOfMemory;
        }

        _ = try self.expect(.semicolon);

        return Statement{
            .insert = .{
                .table_name = name_tok.lexeme,
                .values = first_row,
                .extra_rows = extra_rows.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            },
        };
    }

    fn parseValueTuple(self: *Parser) ParseError![]const []const u8 {
        _ = try self.expect(.lparen);
        var values: std.ArrayList([]const u8) = .{};

        while (true) {
            const val_tok = self.advance();
            switch (val_tok.type) {
                .integer_literal, .string_literal, .identifier => {
                    values.append(self.allocator, val_tok.lexeme) catch return ParseError.OutOfMemory;
                },
                .minus => {
                    // Handle negative numbers: - followed by integer
                    const num_tok = self.advance();
                    if (num_tok.type != .integer_literal) return ParseError.UnexpectedToken;
                    // Construct negative number string
                    const neg_str = std.fmt.allocPrint(self.allocator, "-{s}", .{num_tok.lexeme}) catch return ParseError.OutOfMemory;
                    values.append(self.allocator, neg_str) catch return ParseError.OutOfMemory;
                },
                .kw_null => {
                    values.append(self.allocator, "NULL") catch return ParseError.OutOfMemory;
                },
                else => return ParseError.UnexpectedToken,
            }

            if (self.peek().type == .comma) {
                _ = self.advance();
            } else {
                break;
            }
        }

        _ = try self.expect(.rparen);
        return values.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory;
    }

    fn extractInsertSelectSQL(self: *Parser) ParseError![]const u8 {
        // Positioned at SELECT token; capture everything until ';'
        const start_tok = self.peek();
        if (start_tok.type != .kw_select) return ParseError.UnexpectedToken;
        const start_ptr = start_tok.lexeme.ptr;

        while (self.peek().type != .semicolon and self.peek().type != .eof) {
            _ = self.advance();
        }
        if (self.peek().type != .semicolon) return ParseError.UnexpectedToken;

        const prev_tok = self.tokens[self.pos - 1];
        const end_ptr = prev_tok.lexeme.ptr + prev_tok.lexeme.len;
        const sql_len = @intFromPtr(end_ptr) - @intFromPtr(start_ptr);
        const sql = start_ptr[0..sql_len];

        _ = self.advance(); // consume ';'

        var buf = self.allocator.alloc(u8, sql.len + 1) catch return ParseError.OutOfMemory;
        @memcpy(buf[0..sql.len], sql);
        buf[sql.len] = ';';
        return buf;
    }

    fn isAggFunc(tt: TokenType) ?AggFunc {
        return switch (tt) {
            .kw_count => .count,
            .kw_sum => .sum,
            .kw_avg => .avg,
            .kw_min => .min,
            .kw_max => .max,
            else => null,
        };
    }

    fn parseSelect(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_select);

        // Parse optional DISTINCT
        const distinct = self.peek().type == .kw_distinct;
        if (distinct) _ = self.advance();

        var columns: std.ArrayList([]const u8) = .{};
        var select_exprs: std.ArrayList(SelectExpr) = .{};
        var result_exprs: std.ArrayList(*const Expr) = .{};

        if (self.peek().type == .star) {
            _ = self.advance();
        } else {
            while (true) {
                // Parse each SELECT item as a full expression
                const expr = try self.parseExpr();
                result_exprs.append(self.allocator, expr) catch return ParseError.OutOfMemory;

                // Also populate legacy columns/select_exprs for backward compat
                if (exprAsAggregate(expr)) |agg| {
                    select_exprs.append(self.allocator, .{ .aggregate = .{ .func = agg.func, .arg = agg.arg } }) catch return ParseError.OutOfMemory;
                } else if (exprAsColumnName(expr)) |name| {
                    columns.append(self.allocator, name) catch return ParseError.OutOfMemory;
                    select_exprs.append(self.allocator, .{ .column = name }) catch return ParseError.OutOfMemory;
                } else {
                    // Complex expression — store as expr variant in SelectExpr
                    select_exprs.append(self.allocator, .{ .expr = expr }) catch return ParseError.OutOfMemory;
                }

                if (self.peek().type == .comma) {
                    _ = self.advance();
                } else {
                    break;
                }
            }
        }

        _ = try self.expect(.kw_from);
        const table_tok = try self.expect(.identifier);

        // Parse optional JOIN
        const join = if (self.peek().type == .kw_inner or self.peek().type == .kw_left or self.peek().type == .kw_join)
            try self.parseJoinClause()
        else
            null;

        // Parse WHERE clause: use Expr-based parsing (Phase 6c)
        // JOINありの場合は旧WhereClauseを使う（テーブル修飾の解決が必要なため）
        var where: ?WhereClause = null;
        var where_expr: ?*const Expr = null;
        if (self.peek().type == .kw_where) {
            if (join != null) {
                where = try self.parseWhereClause();
            } else {
                _ = self.advance(); // consume WHERE
                where_expr = try self.parseWhereExpr();
            }
        }

        // Parse optional GROUP BY
        var group_by: ?[]const u8 = null;
        if (self.peek().type == .kw_group) {
            _ = self.advance();
            _ = try self.expect(.kw_by);
            const gb_tok = try self.expect(.identifier);
            group_by = gb_tok.lexeme;
        }

        // Parse optional HAVING
        var having: ?HavingClause = null;
        if (self.peek().type == .kw_having) {
            _ = self.advance();
            const func = isAggFunc(self.peek().type) orelse return ParseError.UnexpectedToken;
            _ = self.advance();
            _ = try self.expect(.lparen);
            const having_arg_tok = self.advance();
            const having_arg: []const u8 = switch (having_arg_tok.type) {
                .star => "*",
                .identifier => having_arg_tok.lexeme,
                else => return ParseError.UnexpectedToken,
            };
            _ = try self.expect(.rparen);
            const having_op = try self.parseCompOp();
            const having_val = self.advance();
            switch (having_val.type) {
                .integer_literal, .string_literal, .identifier => {},
                else => return ParseError.UnexpectedToken,
            }
            having = .{
                .func = func,
                .arg = having_arg,
                .op = having_op,
                .value = having_val.lexeme,
            };
        }

        // Parse optional ORDER BY
        const order_by = if (self.peek().type == .kw_order) try self.parseOrderByClause() else null;

        // Parse optional LIMIT
        var limit: ?i64 = null;
        if (self.peek().type == .kw_limit) {
            _ = self.advance();
            const limit_tok = self.advance();
            if (limit_tok.type != .integer_literal) return ParseError.UnexpectedToken;
            limit = std.fmt.parseInt(i64, limit_tok.lexeme, 10) catch return ParseError.UnexpectedToken;
        }

        // Parse optional OFFSET
        var offset: ?i64 = null;
        if (self.peek().type == .kw_offset) {
            _ = self.advance();
            const offset_tok = self.advance();
            if (offset_tok.type != .integer_literal) return ParseError.UnexpectedToken;
            offset = std.fmt.parseInt(i64, offset_tok.lexeme, 10) catch return ParseError.UnexpectedToken;
        }

        // Check for UNION/INTERSECT/EXCEPT before semicolon
        if (self.peek().type == .kw_union or self.peek().type == .kw_intersect or self.peek().type == .kw_except) {
            // Extract raw SQL for first SELECT - from first SELECT keyword to current position
            var select_start: usize = 0;
            for (self.tokens, 0..) |token, i| {
                if (token.type == .kw_select) {
                    select_start = i;
                    break;
                }
            }

            // Reconstruct first SELECT SQL (from SELECT to current position, before set op)
            const first_select_sql = try self.reconstructSQL(self.tokens[select_start..self.pos]);
            return try self.parseSetOp(first_select_sql);
        }

        _ = try self.expect(.semicolon);

        return Statement{
            .select_stmt = .{
                .table_name = table_tok.lexeme,
                .columns = columns.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .select_exprs = select_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .result_exprs = result_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .distinct = distinct,
                .join = join,
                .where = where,
                .where_expr = where_expr,
                .group_by = group_by,
                .having = having,
                .order_by = order_by,
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseJoinClause(self: *Parser) ParseError!JoinClause {
        // Parse: [INNER|LEFT] JOIN table ON t1.col = t2.col
        var join_type: JoinType = .inner;
        if (self.peek().type == .kw_inner) {
            _ = self.advance();
            join_type = .inner;
        } else if (self.peek().type == .kw_left) {
            _ = self.advance();
            join_type = .left;
        }
        _ = try self.expect(.kw_join);
        const join_table = try self.expect(.identifier);

        _ = try self.expect(.kw_on);
        // Parse: t1.col = t2.col
        const left_table = try self.expect(.identifier);
        _ = try self.expect(.dot);
        const left_col = try self.expect(.identifier);
        _ = try self.expect(.equals);
        const right_table = try self.expect(.identifier);
        _ = try self.expect(.dot);
        const right_col = try self.expect(.identifier);

        return .{
            .join_type = join_type,
            .table_name = join_table.lexeme,
            .left_table = left_table.lexeme,
            .left_column = left_col.lexeme,
            .right_table = right_table.lexeme,
            .right_column = right_col.lexeme,
        };
    }

    fn parseOrderByClause(self: *Parser) ParseError!OrderByClause {
        _ = try self.expect(.kw_order);
        _ = try self.expect(.kw_by);
        const col_tok = try self.expect(.identifier);
        var order: SortOrder = .asc;
        if (self.peek().type == .kw_asc) {
            _ = self.advance();
            order = .asc;
        } else if (self.peek().type == .kw_desc) {
            _ = self.advance();
            order = .desc;
        }
        return .{
            .column = col_tok.lexeme,
            .order = order,
        };
    }

    // ---- Expression parser (recursive descent with precedence) ----

    fn allocExpr(self: *Parser, expr: Expr) ParseError!*const Expr {
        const ptr = self.allocator.create(Expr) catch return ParseError.OutOfMemory;
        ptr.* = expr;
        return ptr;
    }

    /// Parse an expression: AND/OR (lowest) > comparison > add/sub > mul/div > primary
    pub fn parseExpr(self: *Parser) ParseError!*const Expr {
        return self.parseLogical();
    }

    /// Parse WHERE expression (alias for parseExpr, which now includes AND/OR)
    pub fn parseWhereExpr(self: *Parser) ParseError!*const Expr {
        return self.parseExpr();
    }

    /// Parse logical AND/OR (lowest precedence)
    fn parseLogical(self: *Parser) ParseError!*const Expr {
        var left = try self.parseNot();

        while (true) {
            const op_opt: ?BinOp = switch (self.peek().type) {
                .kw_and => .logical_and,
                .kw_or => .logical_or,
                else => null,
            };

            if (op_opt == null) break;

            _ = self.advance();
            const right = try self.parseNot();
            left = try self.allocExpr(.{ .binary_op = .{ .op = op_opt.?, .left = left, .right = right } });
        }

        return left;
    }

    /// Parse NOT prefix operator (between AND/OR and comparison)
    fn parseNot(self: *Parser) ParseError!*const Expr {
        if (self.peek().type == .kw_not) {
            _ = self.advance();
            const operand = try self.parseNot(); // right-associative
            return self.allocExpr(.{ .unary_op = .{ .op = .not, .operand = operand } });
        }
        return self.parseComparison();
    }

    /// Parse comparison operators: =, !=, <, <=, >, >=, LIKE, IS NULL, IS NOT NULL, IN
    fn parseComparison(self: *Parser) ParseError!*const Expr {
        var left = try self.parseAddSub();

        while (true) {
            // IS NULL / IS NOT NULL
            if (self.peek().type == .kw_is) {
                _ = self.advance();
                if (self.peek().type == .kw_not) {
                    _ = self.advance();
                    _ = try self.expect(.kw_null);
                    left = try self.allocExpr(.{ .unary_op = .{ .op = .is_not_null, .operand = left } });
                    continue;
                }
                _ = try self.expect(.kw_null);
                left = try self.allocExpr(.{ .unary_op = .{ .op = .is_null, .operand = left } });
                continue;
            }

            // NOT LIKE / NOT GLOB / NOT IN / NOT BETWEEN
            if (self.peek().type == .kw_not) {
                const next_pos = self.pos + 1;
                if (next_pos < self.tokens.len) {
                    const next_tt = self.tokens[next_pos].type;
                    if (next_tt == .kw_like or next_tt == .kw_glob or next_tt == .kw_in or next_tt == .kw_between) {
                        _ = self.advance(); // consume NOT
                        // Continue the loop - the next iteration will handle the operator
                        // Then we wrap the result in NOT
                        // For simplicity, parse the operator directly here
                        if (self.peek().type == .kw_between) {
                            _ = self.advance();
                            const low = try self.parseAddSub();
                            _ = try self.expect(.kw_and);
                            const high = try self.parseAddSub();
                            const ge_expr = try self.allocExpr(.{ .binary_op = .{ .op = .ge, .left = left, .right = low } });
                            const le_expr = try self.allocExpr(.{ .binary_op = .{ .op = .le, .left = left, .right = high } });
                            const between_expr = try self.allocExpr(.{ .binary_op = .{ .op = .logical_and, .left = ge_expr, .right = le_expr } });
                            left = try self.allocExpr(.{ .unary_op = .{ .op = .not, .operand = between_expr } });
                            continue;
                        } else if (self.peek().type == .kw_in) {
                            _ = self.advance();
                            _ = try self.expect(.lparen);
                            const sql = try self.extractSubquerySQL();
                            const in_expr = try self.allocExpr(.{ .in_list = .{ .operand = left, .subquery_sql = sql } });
                            left = try self.allocExpr(.{ .unary_op = .{ .op = .not, .operand = in_expr } });
                            continue;
                        } else {
                            // NOT LIKE or NOT GLOB
                            const op: BinOp = if (self.peek().type == .kw_like) .like else .glob;
                            _ = self.advance();
                            const right = try self.parseAddSub();
                            const like_expr = try self.allocExpr(.{ .binary_op = .{ .op = op, .left = left, .right = right } });
                            left = try self.allocExpr(.{ .unary_op = .{ .op = .not, .operand = like_expr } });
                            continue;
                        }
                    }
                }
            }

            // BETWEEN a AND b → left >= a AND left <= b
            if (self.peek().type == .kw_between) {
                _ = self.advance();
                const low = try self.parseAddSub();
                _ = try self.expect(.kw_and);
                const high = try self.parseAddSub();
                const ge_expr = try self.allocExpr(.{ .binary_op = .{ .op = .ge, .left = left, .right = low } });
                const le_expr = try self.allocExpr(.{ .binary_op = .{ .op = .le, .left = left, .right = high } });
                left = try self.allocExpr(.{ .binary_op = .{ .op = .logical_and, .left = ge_expr, .right = le_expr } });
                continue;
            }

            // IN (SELECT ...)
            if (self.peek().type == .kw_in) {
                _ = self.advance();
                _ = try self.expect(.lparen);
                const sql = try self.extractSubquerySQL();
                left = try self.allocExpr(.{ .in_list = .{ .operand = left, .subquery_sql = sql } });
                continue;
            }

            const op_opt: ?BinOp = switch (self.peek().type) {
                .equals => .eq,
                .not_equals => .ne,
                .less_than => .lt,
                .less_equal => .le,
                .greater_than => .gt,
                .greater_equal => .ge,
                .kw_like => .like,
                .kw_glob => .glob,
                else => null,
            };

            if (op_opt == null) break;

            _ = self.advance();
            const right = try self.parseAddSub();
            left = try self.allocExpr(.{ .binary_op = .{ .op = op_opt.?, .left = left, .right = right } });
        }

        return left;
    }

    /// Parse +, -, || (medium precedence)
    fn parseAddSub(self: *Parser) ParseError!*const Expr {
        var left = try self.parseMulDiv();

        while (true) {
            const op: BinOp = switch (self.peek().type) {
                .plus => .add,
                .minus => .sub,
                .concat => .concat,
                else => break,
            };
            _ = self.advance();
            const right = try self.parseMulDiv();
            left = try self.allocExpr(.{ .binary_op = .{ .op = op, .left = left, .right = right } });
        }

        return left;
    }

    /// Parse *, / (higher precedence)
    fn parseMulDiv(self: *Parser) ParseError!*const Expr {
        var left = try self.parsePrimary();

        while (true) {
            const op: BinOp = switch (self.peek().type) {
                .star => .mul,
                .divide => .div,
                else => break,
            };
            _ = self.advance();
            const right = try self.parsePrimary();
            left = try self.allocExpr(.{ .binary_op = .{ .op = op, .left = left, .right = right } });
        }

        return left;
    }

    /// Parse primary expressions: literals, column refs, aggregates, parenthesized exprs
    fn parsePrimary(self: *Parser) ParseError!*const Expr {
        const tok = self.peek();

        // CASE WHEN expression
        if (tok.type == .kw_case) {
            _ = self.advance();
            var conditions: std.ArrayList(*const Expr) = .{};
            defer conditions.deinit(self.allocator);
            var results: std.ArrayList(*const Expr) = .{};
            defer results.deinit(self.allocator);

            // Parse WHEN ... THEN ... pairs
            while (self.peek().type == .kw_when) {
                _ = self.advance();
                const cond = try self.parseExpr();
                try conditions.append(self.allocator, cond);
                _ = try self.expect(.kw_then);
                const result = try self.parseExpr();
                try results.append(self.allocator, result);
            }

            // Parse optional ELSE
            const else_result = if (self.peek().type == .kw_else) blk: {
                _ = self.advance();
                break :blk try self.parseExpr();
            } else null;

            _ = try self.expect(.kw_end);

            return self.allocExpr(.{ .case_when = .{
                .conditions = try conditions.toOwnedSlice(self.allocator),
                .results = try results.toOwnedSlice(self.allocator),
                .else_result = else_result,
            } });
        }

        // Aggregate function
        if (isAggFunc(tok.type)) |func| {
            _ = self.advance();
            _ = try self.expect(.lparen);
            const arg_tok = self.peek();
            const arg_expr = if (arg_tok.type == .star) blk: {
                _ = self.advance();
                break :blk try self.allocExpr(.{ .star = {} });
            } else try self.parseExpr();
            _ = try self.expect(.rparen);
            return self.allocExpr(.{ .aggregate = .{ .func = func, .arg = arg_expr } });
        }

        // Parenthesized expression or scalar subquery
        if (tok.type == .lparen) {
            // Check for scalar subquery: (SELECT ...)
            if (self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .kw_select) {
                _ = self.advance(); // consume '('
                const sql = try self.extractSubquerySQL();
                return self.allocExpr(.{ .scalar_subquery = .{ .subquery_sql = sql } });
            }
            _ = self.advance();
            const inner = try self.parseExpr();
            _ = try self.expect(.rparen);
            return inner;
        }

        // Integer literal
        if (tok.type == .integer_literal) {
            _ = self.advance();
            const num = std.fmt.parseInt(i64, tok.lexeme, 10) catch return ParseError.UnexpectedToken;
            return self.allocExpr(.{ .integer_literal = num });
        }

        // String literal
        if (tok.type == .string_literal) {
            _ = self.advance();
            // Strip quotes
            if (tok.lexeme.len >= 2) {
                return self.allocExpr(.{ .string_literal = tok.lexeme[1 .. tok.lexeme.len - 1] });
            }
            return self.allocExpr(.{ .string_literal = tok.lexeme });
        }

        // NULL
        if (tok.type == .kw_null) {
            _ = self.advance();
            return self.allocExpr(.{ .null_literal = {} });
        }

        // Star (for SELECT *)
        if (tok.type == .star) {
            _ = self.advance();
            return self.allocExpr(.{ .star = {} });
        }

        // Identifier (column reference)
        if (tok.type == .identifier) {
            _ = self.advance();
            return self.allocExpr(.{ .column_ref = tok.lexeme });
        }

        return ParseError.UnexpectedToken;
    }

    /// Helper: check if Expr is a simple column reference and return its name
    pub fn exprAsColumnName(expr: *const Expr) ?[]const u8 {
        return switch (expr.*) {
            .column_ref => |name| name,
            else => null,
        };
    }

    /// Helper: check if Expr is a simple aggregate and extract func/arg
    pub fn exprAsAggregate(expr: *const Expr) ?struct { func: AggFunc, arg: []const u8 } {
        return switch (expr.*) {
            .aggregate => |agg| {
                const arg_name: []const u8 = switch (agg.arg.*) {
                    .column_ref => |name| name,
                    .star => "*",
                    else => return null,
                };
                return .{ .func = agg.func, .arg = arg_name };
            },
            else => null,
        };
    }

    /// Recursively free an Expr tree
    pub fn freeExpr(allocator: std.mem.Allocator, expr: *const Expr) void {
        switch (expr.*) {
            .binary_op => |bin| {
                freeExpr(allocator, bin.left);
                freeExpr(allocator, bin.right);
            },
            .aggregate => |agg| {
                freeExpr(allocator, agg.arg);
            },
            .case_when => |cw| {
                for (cw.conditions) |cond| {
                    freeExpr(allocator, cond);
                }
                allocator.free(cw.conditions);
                for (cw.results) |res| {
                    freeExpr(allocator, res);
                }
                allocator.free(cw.results);
                if (cw.else_result) |else_res| {
                    freeExpr(allocator, else_res);
                }
            },
            .unary_op => |u| {
                freeExpr(allocator, u.operand);
            },
            .in_list => |il| {
                freeExpr(allocator, il.operand);
            },
            else => {},
        }
        allocator.destroy(@constCast(expr));
    }

    /// Free a slice of Expr pointers
    pub fn freeExprSlice(allocator: std.mem.Allocator, exprs: []const *const Expr) void {
        for (exprs) |expr| {
            freeExpr(allocator, expr);
        }
        allocator.free(exprs);
    }

    fn parseCompOp(self: *Parser) ParseError!CompOp {
        const op_tok = self.advance();
        return switch (op_tok.type) {
            .equals => .eq,
            .not_equals => .ne,
            .less_than => .lt,
            .less_equal => .le,
            .greater_than => .gt,
            .greater_equal => .ge,
            else => ParseError.UnexpectedToken,
        };
    }

    fn extractSubquerySQL(self: *Parser) ParseError![]const u8 {
        // We're positioned after '(' and expect SELECT ... )
        // Capture token range as source text
        const start_tok = self.peek();
        if (start_tok.type != .kw_select) return ParseError.UnexpectedToken;

        // Find the source range: from SELECT to the token before ')'
        const start_ptr = start_tok.lexeme.ptr;
        // Skip tokens until matching ')'
        var depth: usize = 0;
        while (self.peek().type != .eof) {
            if (self.peek().type == .lparen) depth += 1;
            if (self.peek().type == .rparen) {
                if (depth == 0) break;
                depth -= 1;
            }
            _ = self.advance();
        }
        // Current token should be ')'
        if (self.peek().type != .rparen) return ParseError.UnexpectedToken;
        // Get the last token before ')'
        const prev_tok = self.tokens[self.pos - 1];
        const end_ptr = prev_tok.lexeme.ptr + prev_tok.lexeme.len;
        const sql_len = @intFromPtr(end_ptr) - @intFromPtr(start_ptr);
        const sql = start_ptr[0..sql_len];
        _ = self.advance(); // consume ')'
        // Append semicolon for the subquery SQL to be parseable
        var buf = self.allocator.alloc(u8, sql.len + 1) catch return ParseError.OutOfMemory;
        @memcpy(buf[0..sql.len], sql);
        buf[sql.len] = ';';
        return buf;
    }

    fn parseWhereCondition(self: *Parser) ParseError!WhereCondition {
        const col_tok = try self.expect(.identifier);
        // Check for IS NULL / IS NOT NULL
        if (self.peek().type == .kw_is) {
            _ = self.advance();
            if (self.peek().type == .kw_not) {
                _ = self.advance();
                _ = try self.expect(.kw_null);
                return .{ .column = col_tok.lexeme, .op = .is_not_null, .value = "" };
            }
            _ = try self.expect(.kw_null);
            return .{ .column = col_tok.lexeme, .op = .is_null, .value = "" };
        }
        // Check for IN (SELECT ...)
        if (self.peek().type == .kw_in) {
            _ = self.advance();
            _ = try self.expect(.lparen);
            const sql = try self.extractSubquerySQL();
            return .{ .column = col_tok.lexeme, .op = .in_subquery, .value = "", .subquery_sql = sql };
        }
        // Check for LIKE
        if (self.peek().type == .kw_like) {
            _ = self.advance();
            const pattern_tok = self.advance();
            if (pattern_tok.type != .string_literal and pattern_tok.type != .identifier) {
                return ParseError.UnexpectedToken;
            }
            return .{ .column = col_tok.lexeme, .op = .like, .value = pattern_tok.lexeme };
        }
        const op = try self.parseCompOp();
        // Check for scalar subquery: op (SELECT ...)
        if (self.peek().type == .lparen) {
            const next_pos = self.pos + 1;
            if (next_pos < self.tokens.len and self.tokens[next_pos].type == .kw_select) {
                _ = self.advance(); // consume '('
                const sql = try self.extractSubquerySQL();
                return .{ .column = col_tok.lexeme, .op = op, .value = "", .subquery_sql = sql };
            }
        }
        const val_tok = self.advance();
        switch (val_tok.type) {
            .integer_literal, .string_literal, .identifier => {},
            else => return ParseError.UnexpectedToken,
        }
        return .{ .column = col_tok.lexeme, .op = op, .value = val_tok.lexeme };
    }

    fn parseWhereClause(self: *Parser) ParseError!WhereClause {
        _ = try self.expect(.kw_where);

        // Parse first condition
        const first = try self.parseWhereCondition();

        // Parse additional AND/OR conditions
        var extra: std.ArrayList(WhereCondition) = .{};
        var connectors: std.ArrayList(LogicOp) = .{};

        while (self.peek().type == .kw_and or self.peek().type == .kw_or) {
            const logic_op: LogicOp = if (self.peek().type == .kw_and) .and_op else .or_op;
            _ = self.advance();
            connectors.append(self.allocator, logic_op) catch return ParseError.OutOfMemory;

            const cond = try self.parseWhereCondition();
            extra.append(self.allocator, cond) catch return ParseError.OutOfMemory;
        }

        return .{
            .column = first.column,
            .op = first.op,
            .value = first.value,
            .subquery_sql = first.subquery_sql,
            .extra = extra.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            .connectors = connectors.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
        };
    }

    fn parseDelete(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_delete);
        _ = try self.expect(.kw_from);
        const name_tok = try self.expect(.identifier);

        var where_expr: ?*const Expr = null;
        if (self.peek().type == .kw_where) {
            _ = self.advance(); // consume WHERE
            where_expr = try self.parseWhereExpr();
        }
        _ = try self.expect(.semicolon);

        return Statement{ .delete = .{
            .table_name = name_tok.lexeme,
            .where = null,
            .where_expr = where_expr,
        } };
    }

    fn parseUpdate(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_update);
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.kw_set);

        var set_columns: std.ArrayList([]const u8) = .{};
        defer set_columns.deinit(self.allocator);
        var set_values: std.ArrayList([]const u8) = .{};
        defer set_values.deinit(self.allocator);

        // Parse first assignment
        const col_tok = try self.expect(.identifier);
        try set_columns.append(self.allocator, col_tok.lexeme);
        _ = try self.expect(.equals);
        const val_tok = self.advance();
        switch (val_tok.type) {
            .integer_literal, .string_literal, .identifier => {},
            else => return ParseError.UnexpectedToken,
        }
        try set_values.append(self.allocator, val_tok.lexeme);

        // Parse additional assignments (comma-separated)
        while (self.peek().type == .comma) {
            _ = self.advance(); // consume comma
            const next_col = try self.expect(.identifier);
            try set_columns.append(self.allocator, next_col.lexeme);
            _ = try self.expect(.equals);
            const next_val = self.advance();
            switch (next_val.type) {
                .integer_literal, .string_literal, .identifier => {},
                else => return ParseError.UnexpectedToken,
            }
            try set_values.append(self.allocator, next_val.lexeme);
        }

        var where_expr: ?*const Expr = null;
        if (self.peek().type == .kw_where) {
            _ = self.advance(); // consume WHERE
            where_expr = try self.parseWhereExpr();
        }
        _ = try self.expect(.semicolon);

        return Statement{ .update = .{
            .table_name = name_tok.lexeme,
            .set_columns = try set_columns.toOwnedSlice(self.allocator),
            .set_values = try set_values.toOwnedSlice(self.allocator),
            .where = null,
            .where_expr = where_expr,
        } };
    }

    fn parseDropTable(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_drop);
        _ = try self.expect(.kw_table);
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.semicolon);

        return Statement{ .drop_table = .{
            .table_name = name_tok.lexeme,
        } };
    }

    fn parseAlterTable(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_alter);
        _ = try self.expect(.kw_table);
        const name_tok = try self.expect(.identifier);

        if (self.peek().type == .kw_add) {
            // ALTER TABLE t ADD COLUMN col TYPE;
            _ = self.advance();
            // COLUMN keyword is optional in SQLite3
            if (self.peek().type == .kw_column) _ = self.advance();
            const col_name = try self.expect(.identifier);
            const col_type_tok = self.advance();
            const col_type_str: []const u8 = switch (col_type_tok.type) {
                .kw_integer => "INTEGER",
                .kw_text => "TEXT",
                else => return ParseError.UnexpectedToken,
            };
            _ = try self.expect(.semicolon);
            return Statement{ .alter_table = .{ .add_column = .{
                .table_name = name_tok.lexeme,
                .column = .{
                    .name = col_name.lexeme,
                    .col_type = col_type_str,
                    .is_primary_key = false,
                },
            } } };
        } else if (self.peek().type == .kw_rename) {
            // ALTER TABLE t RENAME TO new_name;
            _ = self.advance();
            _ = try self.expect(.kw_to);
            const new_name_tok = try self.expect(.identifier);
            _ = try self.expect(.semicolon);
            return Statement{ .alter_table = .{ .rename_to = .{
                .table_name = name_tok.lexeme,
                .new_name = new_name_tok.lexeme,
            } } };
        }
        return ParseError.UnexpectedToken;
    }

    fn parseBegin(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_begin);
        // Optional TRANSACTION keyword
        if (self.peek().type == .kw_transaction) _ = self.advance();
        _ = try self.expect(.semicolon);
        return Statement{ .begin = {} };
    }

    fn parseCommit(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_commit);
        if (self.peek().type == .kw_transaction) _ = self.advance();
        _ = try self.expect(.semicolon);
        return Statement{ .commit = {} };
    }

    fn parseRollback(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_rollback);
        if (self.peek().type == .kw_transaction) _ = self.advance();
        _ = try self.expect(.semicolon);
        return Statement{ .rollback = {} };
    }

    fn isSetOpKeyword(tt: TokenType) bool {
        return tt == .kw_union or tt == .kw_intersect or tt == .kw_except;
    }

    fn parseSetOp(self: *Parser, first_select_sql: []const u8) ParseError!Statement {
        // Already parsed first SELECT: SELECT ... (stored as raw SQL)
        // Currently at: UNION [ALL] | INTERSECT | EXCEPT ...
        var selects: std.ArrayList([]const u8) = .{};
        defer selects.deinit(self.allocator);

        // Add first SELECT
        selects.append(self.allocator, first_select_sql) catch return ParseError.OutOfMemory;

        // Determine set operation type
        const set_op: SetOp = switch (self.peek().type) {
            .kw_union => blk: {
                _ = self.advance();
                if (self.peek().type == .kw_all) {
                    _ = self.advance();
                    break :blk .union_all;
                }
                break :blk .union_distinct;
            },
            .kw_intersect => blk: {
                _ = self.advance();
                break :blk .intersect;
            },
            .kw_except => blk: {
                _ = self.advance();
                break :blk .except;
            },
            else => return ParseError.UnexpectedToken,
        };

        // Parse first subsequent SELECT
        _ = try self.expect(.kw_select);
        var select_start = self.pos - 1;
        while (self.peek().type != .semicolon and
               self.peek().type != .eof and
               !isSetOpKeyword(self.peek().type) and
               self.peek().type != .kw_order and
               self.peek().type != .kw_limit and
               self.peek().type != .kw_offset) {
            _ = self.advance();
        }
        var select_tokens = self.tokens[select_start..self.pos];
        var select_sql = try self.reconstructSQL(select_tokens);
        selects.append(self.allocator, select_sql) catch return ParseError.OutOfMemory;

        // Parse additional set operations (same operator)
        while (isSetOpKeyword(self.peek().type)) {
            _ = self.advance(); // consume UNION/INTERSECT/EXCEPT

            // Skip ALL keyword if present (only meaningful for UNION)
            if (self.peek().type == .kw_all) {
                _ = self.advance();
            }

            // Expect SELECT keyword
            _ = try self.expect(.kw_select);

            // Extract from SELECT to next set op/ORDER/LIMIT/OFFSET/EOF/SEMICOLON
            select_start = self.pos - 1;
            while (self.peek().type != .semicolon and
                   self.peek().type != .eof and
                   !isSetOpKeyword(self.peek().type) and
                   self.peek().type != .kw_order and
                   self.peek().type != .kw_limit and
                   self.peek().type != .kw_offset) {
                _ = self.advance();
            }

            select_tokens = self.tokens[select_start..self.pos];
            select_sql = try self.reconstructSQL(select_tokens);
            selects.append(self.allocator, select_sql) catch return ParseError.OutOfMemory;
        }

        // Parse optional ORDER BY / LIMIT / OFFSET
        const order_by = if (self.peek().type == .kw_order) try self.parseOrderByClause() else null;

        var limit: ?i64 = null;
        if (self.peek().type == .kw_limit) {
            _ = self.advance();
            const limit_tok = self.advance();
            if (limit_tok.type != .integer_literal) return ParseError.UnexpectedToken;
            limit = std.fmt.parseInt(i64, limit_tok.lexeme, 10) catch return ParseError.UnexpectedToken;
        }

        var offset: ?i64 = null;
        if (self.peek().type == .kw_offset) {
            _ = self.advance();
            const offset_tok = self.advance();
            if (offset_tok.type != .integer_literal) return ParseError.UnexpectedToken;
            offset = std.fmt.parseInt(i64, offset_tok.lexeme, 10) catch return ParseError.UnexpectedToken;
        }

        _ = try self.expect(.semicolon);

        return Statement{
            .union_select = .{
                .selects = selects.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .set_op = set_op,
                .order_by = order_by,
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn reconstructSQL(self: *Parser, tokens: []const Token) ParseError![]const u8 {
        // Reconstruct SQL from tokens by concatenating lexemes with spaces, appending semicolon
        var result: std.ArrayList(u8) = .{};
        defer result.deinit(self.allocator);

        for (tokens, 0..) |token, i| {
            if (i > 0) {
                result.append(self.allocator, ' ') catch return ParseError.OutOfMemory;
            }
            result.appendSlice(self.allocator, token.lexeme) catch return ParseError.OutOfMemory;
        }

        // Append semicolon so execute() can parse this as a complete statement
        result.append(self.allocator, ';') catch return ParseError.OutOfMemory;

        return result.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory;
    }

};

test "parse CREATE TABLE" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .create_table => |ct| {
            defer allocator.free(ct.columns);
            try std.testing.expectEqualStrings("users", ct.table_name);
            try std.testing.expectEqual(@as(usize, 2), ct.columns.len);
            try std.testing.expectEqualStrings("id", ct.columns[0].name);
            try std.testing.expect(ct.columns[0].is_primary_key);
            try std.testing.expectEqualStrings("name", ct.columns[1].name);
            try std.testing.expect(!ct.columns[1].is_primary_key);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse INSERT INTO" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("INSERT INTO users VALUES (1, 'alice');");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .insert => |ins| {
            defer allocator.free(ins.values);
            try std.testing.expectEqualStrings("users", ins.table_name);
            try std.testing.expectEqual(@as(usize, 2), ins.values.len);
            try std.testing.expectEqualStrings("1", ins.values[0]);
            try std.testing.expectEqualStrings("'alice'", ins.values[1]);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse SELECT" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("SELECT * FROM users;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .select_stmt => |sel| {
            defer allocator.free(sel.columns);
            defer allocator.free(sel.result_exprs);
            try std.testing.expectEqualStrings("users", sel.table_name);
            try std.testing.expectEqual(@as(usize, 0), sel.columns.len);
            try std.testing.expectEqual(@as(?WhereClause, null), sel.where);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse SELECT with WHERE" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("SELECT * FROM users WHERE id = 1;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .select_stmt => |sel| {
            defer allocator.free(sel.columns);
            defer allocator.free(sel.result_exprs);
            // Phase 6c: WHERE is now parsed as where_expr
            try std.testing.expectEqualStrings("users", sel.table_name);
            try std.testing.expect(sel.where_expr != null);
            defer Parser.freeExpr(allocator, sel.where_expr.?);
            // where_expr should be binary_op(eq, column_ref("id"), integer_literal(1))
            try std.testing.expect(sel.where_expr.?.* == .binary_op);
            try std.testing.expectEqual(BinOp.eq, sel.where_expr.?.binary_op.op);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse DELETE" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("DELETE FROM users WHERE id = 1;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .delete => |del| {
            try std.testing.expectEqualStrings("users", del.table_name);
            try std.testing.expect(del.where_expr != null);
            defer Parser.freeExpr(allocator, del.where_expr.?);
            try std.testing.expect(del.where_expr.?.* == .binary_op);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse UPDATE" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("UPDATE users SET name = 'bob' WHERE id = 1;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .update => |upd| {
            defer allocator.free(upd.set_columns);
            defer allocator.free(upd.set_values);
            try std.testing.expectEqualStrings("users", upd.table_name);
            try std.testing.expect(upd.set_columns.len == 1);
            try std.testing.expectEqualStrings("name", upd.set_columns[0]);
            try std.testing.expect(upd.set_values.len == 1);
            try std.testing.expectEqualStrings("'bob'", upd.set_values[0]);
            try std.testing.expect(upd.where_expr != null);
            defer Parser.freeExpr(allocator, upd.where_expr.?);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse SELECT with ORDER BY" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("SELECT * FROM users ORDER BY name DESC;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .select_stmt => |sel| {
            defer allocator.free(sel.columns);
            defer allocator.free(sel.result_exprs);
            try std.testing.expectEqualStrings("users", sel.table_name);
            try std.testing.expect(sel.order_by != null);
            try std.testing.expectEqualStrings("name", sel.order_by.?.column);
            try std.testing.expectEqual(SortOrder.desc, sel.order_by.?.order);
            try std.testing.expectEqual(@as(?i64, null), sel.limit);
            try std.testing.expectEqual(@as(?i64, null), sel.offset);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse SELECT with LIMIT OFFSET" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("SELECT * FROM users LIMIT 10 OFFSET 5;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .select_stmt => |sel| {
            defer allocator.free(sel.columns);
            defer allocator.free(sel.result_exprs);
            try std.testing.expectEqualStrings("users", sel.table_name);
            try std.testing.expectEqual(@as(?i64, 10), sel.limit);
            try std.testing.expectEqual(@as(?i64, 5), sel.offset);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse DROP TABLE" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("DROP TABLE users;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .drop_table => |dt| {
            try std.testing.expectEqualStrings("users", dt.table_name);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse SELECT COUNT(*)" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("SELECT COUNT(*) FROM users;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .select_stmt => |sel| {
            defer allocator.free(sel.columns);
            defer allocator.free(sel.select_exprs);
            defer Parser.freeExprSlice(allocator, sel.result_exprs);
            try std.testing.expectEqual(@as(usize, 0), sel.columns.len);
            try std.testing.expectEqual(@as(usize, 1), sel.select_exprs.len);
            try std.testing.expectEqual(SelectExpr{ .aggregate = .{ .func = .count, .arg = "*" } }, sel.select_exprs[0]);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse SELECT with multiple aggregates" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("SELECT COUNT(*), SUM(price), AVG(price) FROM products;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();

    switch (stmt) {
        .select_stmt => |sel| {
            defer allocator.free(sel.columns);
            defer allocator.free(sel.select_exprs);
            defer Parser.freeExprSlice(allocator, sel.result_exprs);
            try std.testing.expectEqual(@as(usize, 3), sel.select_exprs.len);
            try std.testing.expectEqual(AggFunc.count, sel.select_exprs[0].aggregate.func);
            try std.testing.expectEqual(AggFunc.sum, sel.select_exprs[1].aggregate.func);
            try std.testing.expectEqualStrings("price", sel.select_exprs[1].aggregate.arg);
            try std.testing.expectEqual(AggFunc.avg, sel.select_exprs[2].aggregate.func);
        },
        else => return error.UnexpectedToken,
    }
}

test "parse BEGIN TRANSACTION" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("BEGIN TRANSACTION;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();
    try std.testing.expectEqual(Statement{ .begin = {} }, stmt);
}

test "parse BEGIN" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("BEGIN;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();
    try std.testing.expectEqual(Statement{ .begin = {} }, stmt);
}

test "parse COMMIT" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("COMMIT;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();
    try std.testing.expectEqual(Statement{ .commit = {} }, stmt);
}

test "parse ROLLBACK" {
    const allocator = std.testing.allocator;
    var tok = Tokenizer.init("ROLLBACK;");
    const tokens = try tok.tokenize(allocator);
    defer allocator.free(tokens);

    var p = Parser.init(allocator, tokens);
    const stmt = try p.parse();
    try std.testing.expectEqual(Statement{ .rollback = {} }, stmt);
}
