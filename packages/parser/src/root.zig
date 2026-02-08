const std = @import("std");
const tokenizer = @import("tokenizer");

pub const TokenType = tokenizer.TokenType;
pub const Token = tokenizer.Token;
pub const Tokenizer = tokenizer.Tokenizer;

pub const ColumnDef = struct {
    name: []const u8,
    col_type: []const u8,
    is_primary_key: bool,
    default_value: ?[]const u8 = null,
    not_null: bool = false,
    autoincrement: bool = false,
    check_expr_sql: ?[]const u8 = null,
};

pub const SortOrder = enum {
    asc,
    desc,
};

pub const OrderByItem = struct {
    column: []const u8, // column name (empty if expr-based)
    order: SortOrder,
    expr: ?*const Expr = null, // expression for ORDER BY (null if simple column)
};

pub const OrderByClause = struct {
    column: []const u8, // first column (backward compat)
    order: SortOrder, // first column order (backward compat)
    items: []const OrderByItem, // all columns (including first)
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
    group_concat,
    total,
};

pub const BinOp = enum {
    add, // +
    sub, // -
    mul, // *
    div, // /
    mod, // %
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
    bit_and, // &
    bit_or, // |
    left_shift, // <<
    right_shift, // >>
};

pub const UnaryOp = enum {
    is_null,
    is_not_null,
    not,
    bit_not, // ~
};

pub const ScalarFunc = enum {
    abs,
    length,
    upper,
    lower,
    trim,
    typeof_fn,
    coalesce,
    nullif,
    max_fn,
    min_fn,
    iif,
    substr,
    instr,
    replace_fn,
    hex,
    unicode_fn,
    char_fn,
    zeroblob,
    printf_fn,
    ltrim,
    rtrim,
    round,
    ifnull,
    random,
    sign,
    date_fn,
    time_fn,
    datetime_fn,
    strftime_fn,
};

pub const Expr = union(enum) {
    integer_literal: i64,
    float_literal: f64,
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
        separator: []const u8 = ",", // for GROUP_CONCAT
        distinct: bool = false, // for COUNT(DISTINCT col)
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
    in_values: struct {
        operand: *const Expr,
        values: []const *const Expr, // IN (val1, val2, ...)
    },
    scalar_subquery: struct {
        subquery_sql: []const u8, // SQL text for (SELECT ...)
    },
    scalar_func: struct {
        func: ScalarFunc,
        args: []const *const Expr,
    },
    cast: struct {
        operand: *const Expr,
        target_type: CastType,
    },
    qualified_ref: struct {
        table: []const u8, // table name or alias
        column: []const u8, // column name
    },
    exists: struct {
        subquery_sql: []const u8,
        negated: bool = false, // NOT EXISTS
    },
    window_func: struct {
        func: WindowFunc,
        partition_by: []const []const u8, // PARTITION BY columns (empty if none)
        order_by: []const OrderByItem, // ORDER BY items in OVER clause
    },
};

pub const WindowFunc = enum {
    row_number,
    rank,
    dense_rank,
};

pub const CastType = enum {
    integer,
    text,
};

pub const SelectExpr = union(enum) {
    column: []const u8,
    aggregate: struct {
        func: AggFunc,
        arg: []const u8, // column name or "*"
        separator: []const u8 = ",", // for GROUP_CONCAT
        distinct: bool = false, // for COUNT(DISTINCT col)
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
    right,
    cross,
    full,
};

pub const JoinClause = struct {
    join_type: JoinType,
    table_name: []const u8,
    table_alias: ?[]const u8 = null, // JOIN table alias
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
        if_not_exists: bool = false,
    };

    pub const Insert = struct {
        table_name: []const u8,
        values: []const []const u8, // first row values (or empty for INSERT SELECT)
        extra_rows: []const []const []const u8, // additional rows for multi-row INSERT
        select_sql: ?[]const u8 = null, // INSERT INTO ... SELECT (raw SQL)
        replace_mode: bool = false, // REPLACE INTO / INSERT OR REPLACE behavior
        ignore_mode: bool = false, // INSERT OR IGNORE behavior
        column_names: []const []const u8 = &.{}, // explicit column list (empty = all)
    };

    pub const Select = struct {
        table_name: []const u8,
        table_alias: ?[]const u8 = null, // FROM table alias
        columns: []const []const u8, // empty = * (plain column names for backward compat)
        select_exprs: []const SelectExpr, // full expression list (includes aggregates)
        result_exprs: []const *const Expr, // parsed expression ASTs for each SELECT item
        aliases: []const ?[]const u8, // column aliases (null if no alias)
        distinct: bool,
        joins: []const JoinClause = &.{},
        where: ?WhereClause,
        where_expr: ?*const Expr = null, // Expr-based WHERE (Phase 6c)
        group_by: ?[]const []const u8, // column names (multiple GROUP BY columns)
        having: ?HavingClause,
        having_expr: ?*const Expr = null,
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
        set_values: []const []const u8, // kept for backward compat (empty when set_exprs used)
        set_exprs: []const *const Expr = &.{}, // expression ASTs for SET values
        where: ?WhereClause,
        where_expr: ?*const Expr = null,
    };

    pub const DropTable = struct {
        table_name: []const u8,
        if_exists: bool = false,
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
            .kw_replace => self.parseReplace(),
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

    /// Accept identifier or keyword as an alias name (keywords can be used as aliases)
    fn expectAlias(self: *Parser) ParseError!Token {
        const tok = self.advance();
        if (tok.type == .identifier) return tok;
        // Allow keywords as aliases (e.g., AS total, AS value, etc.)
        if (tok.lexeme.len > 0 and tok.type != .eof and tok.type != .semicolon) return tok;
        return ParseError.UnexpectedToken;
    }

    fn parseCreate(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_create);
        if (self.peek().type == .kw_index) {
            return self.parseCreateIndex();
        }
        // Default: CREATE TABLE [IF NOT EXISTS]
        _ = try self.expect(.kw_table);
        var if_not_exists = false;
        if (self.peek().type == .kw_if) {
            _ = self.advance();
            _ = try self.expect(.kw_not);
            _ = try self.expect(.kw_exists);
            if_not_exists = true;
        }
        return self.parseCreateTableBody(if_not_exists);
    }

    fn parseCreateTableBody(self: *Parser, if_not_exists: bool) ParseError!Statement {
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
                .kw_real => "REAL",
                else => return ParseError.UnexpectedToken,
            };

            var is_pk = false;
            var default_val: ?[]const u8 = null;
            var not_null = false;
            var autoincrement = false;
            var check_sql: ?[]const u8 = null;
            // Parse column constraints (PRIMARY KEY, NOT NULL, DEFAULT, AUTOINCREMENT, CHECK)
            while (true) {
                if (self.peek().type == .kw_primary) {
                    _ = self.advance();
                    _ = try self.expect(.kw_key);
                    is_pk = true;
                } else if (self.peek().type == .kw_autoincrement) {
                    _ = self.advance();
                    autoincrement = true;
                } else if (self.peek().type == .kw_not) {
                    _ = self.advance();
                    _ = try self.expect(.kw_null);
                    not_null = true;
                } else if (self.peek().type == .kw_default) {
                    _ = self.advance();
                    const def_tok = self.advance();
                    default_val = def_tok.lexeme;
                } else if (self.peek().type == .kw_check) {
                    _ = self.advance();
                    _ = try self.expect(.lparen);
                    // Capture the SQL text of the CHECK expression
                    const start_pos = self.pos;
                    var paren_depth: usize = 1;
                    while (self.pos < self.tokens.len) {
                        if (self.tokens[self.pos].type == .lparen) {
                            paren_depth += 1;
                        } else if (self.tokens[self.pos].type == .rparen) {
                            paren_depth -= 1;
                            if (paren_depth == 0) break;
                        }
                        self.pos += 1;
                    }
                    // Extract SQL text from source between start token and closing paren
                    if (start_pos < self.tokens.len and self.pos < self.tokens.len) {
                        const start_lexeme = self.tokens[start_pos].lexeme;
                        const end_lexeme = self.tokens[self.pos].lexeme;
                        const src_start = @intFromPtr(start_lexeme.ptr);
                        const src_end = @intFromPtr(end_lexeme.ptr);
                        if (src_end > src_start) {
                            check_sql = start_lexeme.ptr[0 .. src_end - src_start];
                        }
                    }
                    _ = try self.expect(.rparen);
                } else break;
            }

            columns.append(self.allocator, .{
                .name = col_name.lexeme,
                .col_type = col_type_str,
                .is_primary_key = is_pk,
                .default_value = default_val,
                .not_null = not_null,
                .autoincrement = autoincrement,
                .check_expr_sql = check_sql,
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
                .if_not_exists = if_not_exists,
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

        // Parse optional OR IGNORE / OR REPLACE
        var replace_mode = false;
        var ignore_mode = false;
        if (self.peek().type == .kw_or) {
            _ = self.advance(); // consume OR
            if (self.peek().type == .kw_replace) {
                _ = self.advance();
                replace_mode = true;
            } else if (self.peek().type == .kw_ignore) {
                _ = self.advance();
                ignore_mode = true;
            } else {
                return ParseError.UnexpectedToken;
            }
        }

        _ = try self.expect(.kw_into);
        const name_tok = try self.expect(.identifier);

        // Parse optional column list: INSERT INTO t(col1, col2, ...)
        var col_names: std.ArrayList([]const u8) = .{};
        if (self.peek().type == .lparen and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .identifier) {
            _ = self.advance(); // consume '('
            while (true) {
                const col_tok = try self.expectAlias();
                col_names.append(self.allocator, col_tok.lexeme) catch return ParseError.OutOfMemory;
                if (self.peek().type == .comma) {
                    _ = self.advance();
                } else break;
            }
            _ = try self.expect(.rparen);
        }

        // INSERT INTO ... SELECT or INSERT INTO ... VALUES
        if (self.peek().type == .kw_select) {
            const select_sql = try self.extractInsertSelectSQL();
            return Statement{
                .insert = .{
                    .table_name = name_tok.lexeme,
                    .values = &.{},
                    .extra_rows = &.{},
                    .select_sql = select_sql,
                    .replace_mode = replace_mode,
                    .ignore_mode = ignore_mode,
                    .column_names = col_names.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
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
                .replace_mode = replace_mode,
                .ignore_mode = ignore_mode,
                .column_names = col_names.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            },
        };
    }

    fn parseReplace(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_replace);
        _ = try self.expect(.kw_into);
        const name_tok = try self.expect(.identifier);

        _ = try self.expect(.kw_values);
        const first_row = try self.parseValueTuple();

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
                .replace_mode = true,
            },
        };
    }

    fn parseValueTuple(self: *Parser) ParseError![]const []const u8 {
        _ = try self.expect(.lparen);
        var values: std.ArrayList([]const u8) = .{};

        while (true) {
            const val_tok = self.advance();
            switch (val_tok.type) {
                .integer_literal, .float_literal, .string_literal, .identifier => {
                    values.append(self.allocator, val_tok.lexeme) catch return ParseError.OutOfMemory;
                },
                .minus => {
                    // Handle negative numbers: - followed by integer/float
                    const num_tok = self.advance();
                    if (num_tok.type != .integer_literal and num_tok.type != .float_literal) return ParseError.UnexpectedToken;
                    // Use source slice spanning minus and integer tokens (no allocation)
                    const start = val_tok.lexeme.ptr;
                    const end = num_tok.lexeme.ptr + num_tok.lexeme.len;
                    values.append(self.allocator, start[0 .. @intFromPtr(end) - @intFromPtr(start)]) catch return ParseError.OutOfMemory;
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
            .kw_total => .total,
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
        var aliases: std.ArrayList(?[]const u8) = .{};

        if (self.peek().type == .star) {
            _ = self.advance();
        } else {
            while (true) {
                // Parse each SELECT item as a full expression
                const expr = try self.parseExpr();
                result_exprs.append(self.allocator, expr) catch return ParseError.OutOfMemory;

                // Parse optional AS alias
                var alias: ?[]const u8 = null;
                if (self.peek().type == .kw_as) {
                    _ = self.advance();
                    const alias_tok = try self.expectAlias();
                    alias = alias_tok.lexeme;
                }
                aliases.append(self.allocator, alias) catch return ParseError.OutOfMemory;

                // Also populate legacy columns/select_exprs for backward compat
                if (exprAsAggregate(expr)) |agg| {
                    select_exprs.append(self.allocator, .{ .aggregate = .{ .func = agg.func, .arg = agg.arg, .separator = agg.separator, .distinct = agg.distinct } }) catch return ParseError.OutOfMemory;
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

        // FROM is optional for table-less SELECT (e.g., SELECT ABS(-10);)
        var table_name: []const u8 = "";
        var table_alias: ?[]const u8 = null;
        if (self.peek().type == .kw_from) {
            _ = self.advance();
            const table_tok = try self.expect(.identifier);
            table_name = table_tok.lexeme;
            // Parse optional alias: AS alias or bare alias
            if (self.peek().type == .kw_as) {
                _ = self.advance();
                const alias_tok = try self.expectAlias();
                table_alias = alias_tok.lexeme;
            } else if (self.peek().type == .identifier) {
                // Bare alias (not a keyword)
                table_alias = self.advance().lexeme;
            }
        }

        // If no FROM clause, skip directly to possible semicolon/set-op
        if (table_name.len == 0) {
            // Check for set operations
            if (self.peek().type == .kw_union or self.peek().type == .kw_intersect or self.peek().type == .kw_except) {
                var select_start: usize = 0;
                for (self.tokens, 0..) |token, i| {
                    if (token.type == .kw_select) {
                        select_start = i;
                        break;
                    }
                }
                const first_select_sql = try self.reconstructSQL(self.tokens[select_start..self.pos]);
                return try self.parseSetOp(first_select_sql);
            }
            _ = try self.expect(.semicolon);
            return Statement{
                .select_stmt = .{
                    .table_name = "",
                    .columns = columns.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                    .select_exprs = select_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                    .result_exprs = result_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                    .aliases = aliases.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                    .distinct = distinct,
                    .joins = &.{},
                    .where = null,
                    .where_expr = null,
                    .group_by = null,
                    .having = null,
                    .order_by = null,
                    .limit = null,
                    .offset = null,
                },
            };
        }

        // Parse optional JOINs (including comma syntax: FROM t1, t2 = CROSS JOIN)
        var joins: std.ArrayList(JoinClause) = .{};
        while (self.peek().type == .kw_inner or self.peek().type == .kw_left or self.peek().type == .kw_right or self.peek().type == .kw_join or self.peek().type == .kw_cross or self.peek().type == .kw_full) {
            const jc = try self.parseJoinClause();
            joins.append(self.allocator, jc) catch return ParseError.OutOfMemory;
        }
        // Handle comma syntax: FROM t1, t2
        if (joins.items.len == 0 and self.peek().type == .comma) {
            _ = self.advance(); // consume comma
            const join_table_tok = try self.expect(.identifier);
            var comma_alias: ?[]const u8 = null;
            if (self.peek().type == .kw_as) {
                _ = self.advance();
                const alias_tok = try self.expectAlias();
                comma_alias = alias_tok.lexeme;
            } else if (self.peek().type == .identifier) {
                comma_alias = self.advance().lexeme;
            }
            joins.append(self.allocator, .{
                .join_type = .cross,
                .table_name = join_table_tok.lexeme,
                .table_alias = comma_alias,
                .left_table = "",
                .left_column = "",
                .right_table = "",
                .right_column = "",
            }) catch return ParseError.OutOfMemory;
        }
        const joins_slice = joins.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory;

        // Parse WHERE clause: always use Expr-based parsing
        const where: ?WhereClause = null;
        var where_expr: ?*const Expr = null;
        if (self.peek().type == .kw_where) {
            _ = self.advance(); // consume WHERE
            where_expr = try self.parseWhereExpr();
        }

        // Parse optional GROUP BY
        var group_by: ?[]const []const u8 = null;
        if (self.peek().type == .kw_group) {
            _ = self.advance();
            _ = try self.expect(.kw_by);
            var gb_cols: std.ArrayList([]const u8) = .{};
            while (true) {
                const gb_tok = try self.expect(.identifier);
                gb_cols.append(self.allocator, gb_tok.lexeme) catch return ParseError.OutOfMemory;
                if (self.peek().type == .comma) {
                    _ = self.advance();
                } else {
                    break;
                }
            }
            group_by = gb_cols.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory;
        }

        // Parse optional HAVING (expr-based)
        const having: ?HavingClause = null;
        var having_expr: ?*const Expr = null;
        if (self.peek().type == .kw_having) {
            _ = self.advance();
            having_expr = try self.parseWhereExpr();
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
                .table_name = table_name,
                .table_alias = table_alias,
                .columns = columns.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .select_exprs = select_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .result_exprs = result_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .aliases = aliases.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .distinct = distinct,
                .joins = joins_slice,
                .where = where,
                .where_expr = where_expr,
                .group_by = group_by,
                .having = having,
                .having_expr = having_expr,
                .order_by = order_by,
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseJoinClause(self: *Parser) ParseError!JoinClause {
        // Parse: [INNER|LEFT|CROSS] JOIN table [AS alias] [ON t1.col = t2.col]
        var join_type: JoinType = .inner;
        if (self.peek().type == .kw_inner) {
            _ = self.advance();
            join_type = .inner;
        } else if (self.peek().type == .kw_left) {
            _ = self.advance();
            if (self.peek().type == .kw_outer) _ = self.advance(); // optional OUTER
            join_type = .left;
        } else if (self.peek().type == .kw_right) {
            _ = self.advance();
            if (self.peek().type == .kw_outer) _ = self.advance(); // optional OUTER
            join_type = .right;
        } else if (self.peek().type == .kw_cross) {
            _ = self.advance();
            join_type = .cross;
        } else if (self.peek().type == .kw_full) {
            _ = self.advance();
            if (self.peek().type == .kw_outer) _ = self.advance(); // optional OUTER
            join_type = .full;
        }
        _ = try self.expect(.kw_join);
        const join_table = try self.expect(.identifier);

        // Parse optional alias for join table
        var join_alias: ?[]const u8 = null;
        if (self.peek().type == .kw_as) {
            _ = self.advance();
            const alias_tok = try self.expectAlias();
            join_alias = alias_tok.lexeme;
        } else if (self.peek().type == .identifier and self.peek().type != .kw_on) {
            join_alias = self.advance().lexeme;
        }

        // CROSS JOIN has no ON clause
        if (join_type == .cross) {
            return .{
                .join_type = .cross,
                .table_name = join_table.lexeme,
                .table_alias = join_alias,
                .left_table = "",
                .left_column = "",
                .right_table = "",
                .right_column = "",
            };
        }

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
            .table_alias = join_alias,
            .left_table = left_table.lexeme,
            .left_column = left_col.lexeme,
            .right_table = right_table.lexeme,
            .right_column = right_col.lexeme,
        };
    }

    fn parseOrderByClause(self: *Parser) ParseError!OrderByClause {
        _ = try self.expect(.kw_order);
        _ = try self.expect(.kw_by);

        var items: std.ArrayList(OrderByItem) = .{};

        while (true) {
            const expr = try self.parseExpr();
            // Check if it's a simple column reference
            const col_name: []const u8 = if (expr.* == .column_ref) expr.column_ref else "";
            var order: SortOrder = .asc;
            if (self.peek().type == .kw_asc) {
                _ = self.advance();
                order = .asc;
            } else if (self.peek().type == .kw_desc) {
                _ = self.advance();
                order = .desc;
            }
            const order_expr: ?*const Expr = if (col_name.len == 0) expr else blk: {
                // Free the Expr allocation for simple column refs since we keep just the name
                self.allocator.destroy(@constCast(expr));
                break :blk null;
            };
            items.append(self.allocator, .{ .column = col_name, .order = order, .expr = order_expr }) catch return ParseError.OutOfMemory;

            if (self.peek().type == .comma) {
                _ = self.advance();
            } else {
                break;
            }
        }

        const first = items.items[0];
        return .{
            .column = first.column,
            .order = first.order,
            .items = items.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
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
            // NOT EXISTS (...) → exists with negated=true
            if (self.peek().type == .kw_exists) {
                _ = self.advance();
                _ = try self.expect(.lparen);
                const sql = try self.extractSubquerySQL();
                return self.allocExpr(.{ .exists = .{ .subquery_sql = sql, .negated = true } });
            }
            const operand = try self.parseNot(); // right-associative
            return self.allocExpr(.{ .unary_op = .{ .op = .not, .operand = operand } });
        }
        return self.parseComparison();
    }

    /// Parse comparison operators: =, !=, <, <=, >, >=, LIKE, IS NULL, IS NOT NULL, IN
    fn parseComparison(self: *Parser) ParseError!*const Expr {
        var left = try self.parseBitwise();

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
                            var in_expr: *const Expr = undefined;
                            if (self.peek().type == .kw_select) {
                                const sql = try self.extractSubquerySQL();
                                in_expr = try self.allocExpr(.{ .in_list = .{ .operand = left, .subquery_sql = sql } });
                            } else {
                                var vals: std.ArrayList(*const Expr) = .{};
                                while (true) {
                                    const val_expr = try self.parseExpr();
                                    vals.append(self.allocator, val_expr) catch return ParseError.OutOfMemory;
                                    if (self.peek().type == .comma) {
                                        _ = self.advance();
                                    } else {
                                        break;
                                    }
                                }
                                _ = try self.expect(.rparen);
                                in_expr = try self.allocExpr(.{ .in_values = .{
                                    .operand = left,
                                    .values = vals.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                                } });
                            }
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

            // IN (SELECT ...) or IN (val1, val2, ...)
            if (self.peek().type == .kw_in) {
                _ = self.advance();
                _ = try self.expect(.lparen);
                if (self.peek().type == .kw_select) {
                    const sql = try self.extractSubquerySQL();
                    left = try self.allocExpr(.{ .in_list = .{ .operand = left, .subquery_sql = sql } });
                } else {
                    // Parse value list
                    var vals: std.ArrayList(*const Expr) = .{};
                    while (true) {
                        const val_expr = try self.parseExpr();
                        vals.append(self.allocator, val_expr) catch return ParseError.OutOfMemory;
                        if (self.peek().type == .comma) {
                            _ = self.advance();
                        } else {
                            break;
                        }
                    }
                    _ = try self.expect(.rparen);
                    left = try self.allocExpr(.{ .in_values = .{
                        .operand = left,
                        .values = vals.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                    } });
                }
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

    /// Parse &, |, <<, >> (between addition and comparison)
    fn parseBitwise(self: *Parser) ParseError!*const Expr {
        var left = try self.parseAddSub();

        while (true) {
            const op: BinOp = switch (self.peek().type) {
                .ampersand => .bit_and,
                .pipe => .bit_or,
                .left_shift => .left_shift,
                .right_shift => .right_shift,
                else => break,
            };
            _ = self.advance();
            const right = try self.parseAddSub();
            left = try self.allocExpr(.{ .binary_op = .{ .op = op, .left = left, .right = right } });
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

    /// Parse *, /, % (higher precedence)
    fn parseMulDiv(self: *Parser) ParseError!*const Expr {
        var left = try self.parsePrimary();

        while (true) {
            const op: BinOp = switch (self.peek().type) {
                .star => .mul,
                .divide => .div,
                .modulo => .mod,
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

        // Bitwise NOT (~)
        if (tok.type == .tilde) {
            _ = self.advance();
            const operand = try self.parsePrimary();
            return try self.allocExpr(.{ .unary_op = .{ .op = .bit_not, .operand = operand } });
        }

        // Unary minus
        if (tok.type == .minus) {
            _ = self.advance();
            // Check for integer literal directly for efficiency
            if (self.peek().type == .integer_literal) {
                const num_tok = self.advance();
                const num = std.fmt.parseInt(i64, num_tok.lexeme, 10) catch return ParseError.UnexpectedToken;
                return self.allocExpr(.{ .integer_literal = -num });
            }
            // Check for float literal
            if (self.peek().type == .float_literal) {
                const num_tok = self.advance();
                const num = std.fmt.parseFloat(f64, num_tok.lexeme) catch return ParseError.UnexpectedToken;
                return self.allocExpr(.{ .float_literal = -num });
            }
            const operand = try self.parsePrimary();
            // Desugar -x to 0 - x
            const zero = try self.allocExpr(.{ .integer_literal = 0 });
            return self.allocExpr(.{ .binary_op = .{ .op = .sub, .left = zero, .right = operand } });
        }

        // EXISTS (SELECT ...) / NOT EXISTS (SELECT ...)
        if (tok.type == .kw_exists) {
            _ = self.advance(); // consume EXISTS
            _ = try self.expect(.lparen);
            const sql = try self.extractSubquerySQL();
            return self.allocExpr(.{ .exists = .{ .subquery_sql = sql, .negated = false } });
        }

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

        // Aggregate function (or MAX/MIN scalar with multiple args)
        if (isAggFunc(tok.type)) |func| {
            _ = self.advance();
            _ = try self.expect(.lparen);
            // Check for DISTINCT keyword
            const is_distinct = self.peek().type == .kw_distinct;
            if (is_distinct) _ = self.advance();
            const arg_tok = self.peek();
            const arg_expr = if (arg_tok.type == .star) blk: {
                _ = self.advance();
                break :blk try self.allocExpr(.{ .star = {} });
            } else try self.parseExpr();
            // MAX/MIN with multiple args → scalar function
            if (self.peek().type == .comma and (func == .max or func == .min)) {
                var args: std.ArrayList(*const Expr) = .{};
                args.append(self.allocator, arg_expr) catch return ParseError.OutOfMemory;
                while (self.peek().type == .comma) {
                    _ = self.advance();
                    const next_arg = try self.parseExpr();
                    args.append(self.allocator, next_arg) catch return ParseError.OutOfMemory;
                }
                _ = try self.expect(.rparen);
                const scalar_func: ScalarFunc = if (func == .max) .max_fn else .min_fn;
                return self.allocExpr(.{ .scalar_func = .{
                    .func = scalar_func,
                    .args = args.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                } });
            }
            _ = try self.expect(.rparen);
            return self.allocExpr(.{ .aggregate = .{ .func = func, .arg = arg_expr, .distinct = is_distinct } });
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

        // Float literal
        if (tok.type == .float_literal) {
            _ = self.advance();
            const num = std.fmt.parseFloat(f64, tok.lexeme) catch return ParseError.UnexpectedToken;
            return self.allocExpr(.{ .float_literal = num });
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

        // REPLACE() scalar function (kw_replace followed by lparen)
        if (tok.type == .kw_replace and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .lparen) {
            _ = self.advance(); // consume REPLACE
            _ = self.advance(); // consume '('
            var args: std.ArrayList(*const Expr) = .{};
            if (self.peek().type != .rparen) {
                while (true) {
                    const arg = try self.parseExpr();
                    args.append(self.allocator, arg) catch return ParseError.OutOfMemory;
                    if (self.peek().type == .comma) {
                        _ = self.advance();
                    } else {
                        break;
                    }
                }
            }
            _ = try self.expect(.rparen);
            return self.allocExpr(.{ .scalar_func = .{
                .func = .replace_fn,
                .args = args.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            } });
        }

        // GROUP_CONCAT(expr) or GROUP_CONCAT(expr, separator)
        if (tok.type == .identifier and std.ascii.eqlIgnoreCase(tok.lexeme, "GROUP_CONCAT") and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .lparen) {
            _ = self.advance(); // consume GROUP_CONCAT
            _ = self.advance(); // consume '('
            const gc_distinct = self.peek().type == .kw_distinct;
            if (gc_distinct) _ = self.advance();
            const arg_expr = try self.parseExpr();
            var separator: []const u8 = ",";
            if (self.peek().type == .comma) {
                _ = self.advance();
                const sep_tok = self.advance();
                if (sep_tok.type == .string_literal) {
                    separator = sep_tok.lexeme[1 .. sep_tok.lexeme.len - 1]; // strip quotes
                }
            }
            _ = try self.expect(.rparen);
            return self.allocExpr(.{ .aggregate = .{ .func = .group_concat, .arg = arg_expr, .separator = separator, .distinct = gc_distinct } });
        }

        // CAST(expr AS type) expression
        if (tok.type == .identifier and std.ascii.eqlIgnoreCase(tok.lexeme, "CAST") and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .lparen) {
            _ = self.advance(); // consume CAST
            _ = self.advance(); // consume '('
            const operand = try self.parseExpr();
            _ = try self.expect(.kw_as);
            const type_tok = self.advance();
            const target_type: CastType = if (type_tok.type == .kw_integer)
                .integer
            else if (type_tok.type == .kw_text)
                .text
            else if (type_tok.type == .identifier and std.ascii.eqlIgnoreCase(type_tok.lexeme, "REAL"))
                .integer // treat REAL as integer for now
            else
                return ParseError.UnexpectedToken;
            _ = try self.expect(.rparen);
            return self.allocExpr(.{ .cast = .{ .operand = operand, .target_type = target_type } });
        }

        // Window function: ROW_NUMBER() OVER (...), RANK() OVER (...), DENSE_RANK() OVER (...)
        if (tok.type == .identifier and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .lparen) {
            if (classifyWindowFunc(tok.lexeme)) |wfunc| {
                _ = self.advance(); // consume function name
                _ = self.advance(); // consume '('
                _ = try self.expect(.rparen); // window funcs take no args
                _ = try self.expect(.kw_over);
                _ = try self.expect(.lparen);
                // Parse optional PARTITION BY
                var partition_by: std.ArrayList([]const u8) = .{};
                if (self.peek().type == .kw_partition) {
                    _ = self.advance(); // consume PARTITION
                    _ = try self.expect(.kw_by);
                    while (true) {
                        const col_tok = try self.expect(.identifier);
                        partition_by.append(self.allocator, col_tok.lexeme) catch return ParseError.OutOfMemory;
                        if (self.peek().type == .comma) {
                            _ = self.advance();
                        } else break;
                    }
                }
                // Parse optional ORDER BY
                var order_items: std.ArrayList(OrderByItem) = .{};
                if (self.peek().type == .kw_order) {
                    _ = self.advance();
                    _ = try self.expect(.kw_by);
                    while (true) {
                        const ob_expr = try self.parseExpr();
                        const ob_col: []const u8 = if (ob_expr.* == .column_ref) ob_expr.column_ref else "";
                        var ob_order: SortOrder = .asc;
                        if (self.peek().type == .kw_asc) {
                            _ = self.advance();
                        } else if (self.peek().type == .kw_desc) {
                            _ = self.advance();
                            ob_order = .desc;
                        }
                        const ob_expr_val: ?*const Expr = if (ob_col.len == 0) ob_expr else blk: {
                            self.allocator.destroy(@constCast(ob_expr));
                            break :blk null;
                        };
                        order_items.append(self.allocator, .{ .column = ob_col, .order = ob_order, .expr = ob_expr_val }) catch return ParseError.OutOfMemory;
                        if (self.peek().type == .comma) {
                            _ = self.advance();
                        } else break;
                    }
                }
                _ = try self.expect(.rparen);
                return self.allocExpr(.{ .window_func = .{
                    .func = wfunc,
                    .partition_by = partition_by.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                    .order_by = order_items.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                } });
            }
        }

        // Scalar function call: identifier followed by '('
        if (tok.type == .identifier and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].type == .lparen) {
            if (classifyScalarFunc(tok.lexeme)) |func| {
                _ = self.advance(); // consume function name
                _ = self.advance(); // consume '('
                var args: std.ArrayList(*const Expr) = .{};
                if (self.peek().type != .rparen) {
                    while (true) {
                        const arg = try self.parseExpr();
                        args.append(self.allocator, arg) catch return ParseError.OutOfMemory;
                        if (self.peek().type == .comma) {
                            _ = self.advance();
                        } else {
                            break;
                        }
                    }
                }
                _ = try self.expect(.rparen);
                return self.allocExpr(.{ .scalar_func = .{
                    .func = func,
                    .args = args.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                } });
            }
        }

        // Identifier (column reference), possibly table-qualified (t1.col)
        if (tok.type == .identifier) {
            _ = self.advance();
            // Check for table-qualified reference: ident.ident
            if (self.peek().type == .dot) {
                _ = self.advance(); // consume dot
                const col_tok = try self.expect(.identifier);
                return self.allocExpr(.{ .qualified_ref = .{ .table = tok.lexeme, .column = col_tok.lexeme } });
            }
            return self.allocExpr(.{ .column_ref = tok.lexeme });
        }

        return ParseError.UnexpectedToken;
    }

    /// Helper: check if Expr is a simple column reference and return its name
    pub fn exprAsColumnName(expr: *const Expr) ?[]const u8 {
        return switch (expr.*) {
            .column_ref => |name| name,
            .qualified_ref => |qr| qr.column,
            else => null,
        };
    }

    /// Helper: check if Expr is a simple aggregate and extract func/arg
    pub fn exprAsAggregate(expr: *const Expr) ?struct { func: AggFunc, arg: []const u8, separator: []const u8, distinct: bool } {
        return switch (expr.*) {
            .aggregate => |agg| {
                const arg_name: []const u8 = switch (agg.arg.*) {
                    .column_ref => |name| name,
                    .star => "*",
                    else => return null,
                };
                return .{ .func = agg.func, .arg = arg_name, .separator = agg.separator, .distinct = agg.distinct };
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
            .in_values => |iv| {
                freeExpr(allocator, iv.operand);
                for (iv.values) |v| freeExpr(allocator, v);
                allocator.free(iv.values);
            },
            .scalar_func => |sf| {
                for (sf.args) |arg| {
                    freeExpr(allocator, arg);
                }
                allocator.free(sf.args);
            },
            .cast => |c| {
                freeExpr(allocator, c.operand);
            },
            .window_func => |wf| {
                allocator.free(wf.partition_by);
                for (wf.order_by) |item| {
                    if (item.expr) |e| freeExpr(allocator, e);
                }
                allocator.free(wf.order_by);
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

    fn classifyScalarFunc(name: []const u8) ?ScalarFunc {
        const funcs = .{
            .{ "ABS", ScalarFunc.abs },
            .{ "LENGTH", ScalarFunc.length },
            .{ "UPPER", ScalarFunc.upper },
            .{ "LOWER", ScalarFunc.lower },
            .{ "TRIM", ScalarFunc.trim },
            .{ "TYPEOF", ScalarFunc.typeof_fn },
            .{ "COALESCE", ScalarFunc.coalesce },
            .{ "NULLIF", ScalarFunc.nullif },
            .{ "MAX", ScalarFunc.max_fn },
            .{ "MIN", ScalarFunc.min_fn },
            .{ "IIF", ScalarFunc.iif },
            .{ "SUBSTR", ScalarFunc.substr },
            .{ "SUBSTRING", ScalarFunc.substr },
            .{ "INSTR", ScalarFunc.instr },
            .{ "REPLACE", ScalarFunc.replace_fn },
            .{ "HEX", ScalarFunc.hex },
            .{ "UNICODE", ScalarFunc.unicode_fn },
            .{ "CHAR", ScalarFunc.char_fn },
            .{ "ZEROBLOB", ScalarFunc.zeroblob },
            .{ "PRINTF", ScalarFunc.printf_fn },
            .{ "LTRIM", ScalarFunc.ltrim },
            .{ "RTRIM", ScalarFunc.rtrim },
            .{ "ROUND", ScalarFunc.round },
            .{ "IFNULL", ScalarFunc.ifnull },
            .{ "RANDOM", ScalarFunc.random },
            .{ "SIGN", ScalarFunc.sign },
            .{ "DATE", ScalarFunc.date_fn },
            .{ "TIME", ScalarFunc.time_fn },
            .{ "DATETIME", ScalarFunc.datetime_fn },
            .{ "STRFTIME", ScalarFunc.strftime_fn },
        };
        inline for (funcs) |entry| {
            if (std.ascii.eqlIgnoreCase(name, entry[0])) {
                return entry[1];
            }
        }
        return null;
    }

    fn classifyWindowFunc(name: []const u8) ?WindowFunc {
        const funcs = .{
            .{ "ROW_NUMBER", WindowFunc.row_number },
            .{ "RANK", WindowFunc.rank },
            .{ "DENSE_RANK", WindowFunc.dense_rank },
        };
        inline for (funcs) |entry| {
            if (std.ascii.eqlIgnoreCase(name, entry[0])) {
                return entry[1];
            }
        }
        return null;
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
        var set_exprs: std.ArrayList(*const Expr) = .{};

        while (true) {
            const col_tok = try self.expect(.identifier);
            try set_columns.append(self.allocator, col_tok.lexeme);
            _ = try self.expect(.equals);
            const expr = try self.parseExpr();
            set_exprs.append(self.allocator, expr) catch return ParseError.OutOfMemory;

            if (self.peek().type == .comma) {
                _ = self.advance();
            } else {
                break;
            }
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
            .set_values = &.{},
            .set_exprs = set_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            .where = null,
            .where_expr = where_expr,
        } };
    }

    fn parseDropTable(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_drop);
        _ = try self.expect(.kw_table);
        var if_exists = false;
        if (self.peek().type == .kw_if) {
            _ = self.advance();
            _ = try self.expect(.kw_exists);
            if_exists = true;
        }
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.semicolon);

        return Statement{ .drop_table = .{
            .table_name = name_tok.lexeme,
            .if_exists = if_exists,
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
                .kw_real => "REAL",
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
            defer allocator.free(sel.aliases);
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
            defer allocator.free(sel.aliases);
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
            defer {
                for (upd.set_exprs) |e| Parser.freeExpr(allocator, e);
                allocator.free(upd.set_exprs);
            }
            try std.testing.expectEqualStrings("users", upd.table_name);
            try std.testing.expect(upd.set_columns.len == 1);
            try std.testing.expectEqualStrings("name", upd.set_columns[0]);
            try std.testing.expect(upd.set_exprs.len == 1);
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
            defer allocator.free(sel.aliases);
            defer if (sel.order_by) |ob| allocator.free(ob.items);
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
            defer allocator.free(sel.aliases);
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
            defer allocator.free(sel.aliases);
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
            defer allocator.free(sel.aliases);
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
