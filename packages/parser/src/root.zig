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
        columns: []const []const u8, // empty = *
        where: ?WhereClause,
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

    fn parseSelect(self: *Parser) ParseError!Statement {
        _ = try self.expect(.kw_select);

        var columns: std.ArrayList([]const u8) = .{};

        if (self.peek().type == .star) {
            _ = self.advance();
        } else {
            while (true) {
                const col_tok = try self.expect(.identifier);
                columns.append(self.allocator, col_tok.lexeme) catch return ParseError.OutOfMemory;
                if (self.peek().type == .comma) {
                    _ = self.advance();
                } else {
                    break;
                }
            }
        }

        _ = try self.expect(.kw_from);
        const table_tok = try self.expect(.identifier);

        const where = if (self.peek().type == .kw_where) try self.parseWhereClause() else null;
        _ = try self.expect(.semicolon);

        return Statement{
            .select_stmt = .{
                .table_name = table_tok.lexeme,
                .columns = columns.toOwnedSlice(self.allocator) catch return ParseError.OutOfMemory,
                .where = where,
            },
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
