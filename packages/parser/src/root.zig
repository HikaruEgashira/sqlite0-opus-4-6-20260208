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
};

pub const WhereClause = struct {
    column: []const u8,
    op: CompOp,
    value: []const u8,
};

pub const AggFunc = enum {
    count,
    sum,
    avg,
    min,
    max,
};

pub const SelectExpr = union(enum) {
    column: []const u8,
    aggregate: struct {
        func: AggFunc,
        arg: []const u8, // column name or "*"
    },
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

pub const Statement = union(enum) {
    create_table: CreateTable,
    insert: Insert,
    select_stmt: Select,
    delete: Delete,
    update: Update,
    drop_table: DropTable,

    pub const CreateTable = struct {
        table_name: []const u8,
        columns: []const ColumnDef,
    };

    pub const Insert = struct {
        table_name: []const u8,
        values: []const []const u8,
    };

    pub const Select = struct {
        table_name: []const u8,
        columns: []const []const u8, // empty = * (plain column names for backward compat)
        select_exprs: []const SelectExpr, // full expression list (includes aggregates)
        join: ?JoinClause,
        where: ?WhereClause,
        group_by: ?[]const u8, // column name
        order_by: ?OrderByClause,
        limit: ?i64,
        offset: ?i64,
    };

    pub const Delete = struct {
        table_name: []const u8,
        where: ?WhereClause,
    };

    pub const Update = struct {
        table_name: []const u8,
        set_column: []const u8,
        set_value: []const u8,
        where: ?WhereClause,
    };

    pub const DropTable = struct {
        table_name: []const u8,
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
            .kw_create => self.parseCreateTable(),
            .kw_insert => self.parseInsert(),
            .kw_select => self.parseSelect(),
            .kw_delete => self.parseDelete(),
            .kw_update => self.parseUpdate(),
            .kw_drop => self.parseDropTable(),
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

    fn parseCreateTable(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_create);
        _ = try self.expect(.kw_table);
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

    fn parseInsert(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_insert);
        _ = try self.expect(.kw_into);
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.kw_values);
        _ = try self.expect(.lparen);

        var values: std.ArrayList([]const u8) = .{};

        while (true) {
            const val_tok = self.advance();
            switch (val_tok.type) {
                .integer_literal, .string_literal, .identifier => {
                    values.append(self.allocator, val_tok.lexeme) catch return ParseError.OutOfMemory;
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
        _ = try self.expect(.semicolon);

        return Statement{
            .insert = .{
                .table_name = name_tok.lexeme,
                .values = values.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
            },
        };
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

        var columns: std.ArrayList([]const u8) = .{};
        var select_exprs: std.ArrayList(SelectExpr) = .{};

        if (self.peek().type == .star) {
            _ = self.advance();
        } else {
            while (true) {
                // Check for aggregate function
                if (isAggFunc(self.peek().type)) |func| {
                    _ = self.advance(); // consume func keyword
                    _ = try self.expect(.lparen);
                    const arg_tok = self.advance();
                    const arg: []const u8 = switch (arg_tok.type) {
                        .star => "*",
                        .identifier => arg_tok.lexeme,
                        else => return ParseError.UnexpectedToken,
                    };
                    _ = try self.expect(.rparen);
                    select_exprs.append(self.allocator, .{ .aggregate = .{ .func = func, .arg = arg } }) catch return ParseError.OutOfMemory;
                } else {
                    const col_tok = try self.expect(.identifier);
                    columns.append(self.allocator, col_tok.lexeme) catch return ParseError.OutOfMemory;
                    select_exprs.append(self.allocator, .{ .column = col_tok.lexeme }) catch return ParseError.OutOfMemory;
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

        const where = if (self.peek().type == .kw_where) try self.parseWhereClause() else null;

        // Parse optional GROUP BY
        var group_by: ?[]const u8 = null;
        if (self.peek().type == .kw_group) {
            _ = self.advance();
            _ = try self.expect(.kw_by);
            const gb_tok = try self.expect(.identifier);
            group_by = gb_tok.lexeme;
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

        _ = try self.expect(.semicolon);

        return Statement{
            .select_stmt = .{
                .table_name = table_tok.lexeme,
                .columns = columns.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .select_exprs = select_exprs.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .join = join,
                .where = where,
                .group_by = group_by,
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

    fn parseWhereClause(self: *Parser) ParseError!WhereClause {
        _ = try self.expect(.kw_where);
        const col_tok = try self.expect(.identifier);
        const op_tok = self.advance();
        const op: CompOp = switch (op_tok.type) {
            .equals => .eq,
            .not_equals => .ne,
            .less_than => .lt,
            .less_equal => .le,
            .greater_than => .gt,
            .greater_equal => .ge,
            else => return ParseError.UnexpectedToken,
        };
        const val_tok = self.advance();
        switch (val_tok.type) {
            .integer_literal, .string_literal, .identifier => {},
            else => return ParseError.UnexpectedToken,
        }
        return .{
            .column = col_tok.lexeme,
            .op = op,
            .value = val_tok.lexeme,
        };
    }

    fn parseDelete(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_delete);
        _ = try self.expect(.kw_from);
        const name_tok = try self.expect(.identifier);

        const where = if (self.peek().type == .kw_where) try self.parseWhereClause() else null;
        _ = try self.expect(.semicolon);

        return Statement{ .delete = .{
            .table_name = name_tok.lexeme,
            .where = where,
        } };
    }

    fn parseUpdate(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_update);
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.kw_set);
        const col_tok = try self.expect(.identifier);
        _ = try self.expect(.equals);
        const val_tok = self.advance();
        switch (val_tok.type) {
            .integer_literal, .string_literal, .identifier => {},
            else => return ParseError.UnexpectedToken,
        }

        const where = if (self.peek().type == .kw_where) try self.parseWhereClause() else null;
        _ = try self.expect(.semicolon);

        return Statement{ .update = .{
            .table_name = name_tok.lexeme,
            .set_column = col_tok.lexeme,
            .set_value = val_tok.lexeme,
            .where = where,
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
            try std.testing.expectEqualStrings("users", sel.table_name);
            try std.testing.expect(sel.where != null);
            try std.testing.expectEqualStrings("id", sel.where.?.column);
            try std.testing.expectEqual(CompOp.eq, sel.where.?.op);
            try std.testing.expectEqualStrings("1", sel.where.?.value);
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
            try std.testing.expect(del.where != null);
            try std.testing.expectEqualStrings("id", del.where.?.column);
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
            try std.testing.expectEqualStrings("users", upd.table_name);
            try std.testing.expectEqualStrings("name", upd.set_column);
            try std.testing.expectEqualStrings("'bob'", upd.set_value);
            try std.testing.expect(upd.where != null);
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
            try std.testing.expectEqual(@as(usize, 3), sel.select_exprs.len);
            try std.testing.expectEqual(AggFunc.count, sel.select_exprs[0].aggregate.func);
            try std.testing.expectEqual(AggFunc.sum, sel.select_exprs[1].aggregate.func);
            try std.testing.expectEqualStrings("price", sel.select_exprs[1].aggregate.arg);
            try std.testing.expectEqual(AggFunc.avg, sel.select_exprs[2].aggregate.func);
        },
        else => return error.UnexpectedToken,
    }
}
