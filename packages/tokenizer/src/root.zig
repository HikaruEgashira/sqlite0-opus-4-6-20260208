const std = @import("std");

pub const TokenType = enum {
    // Keywords
    kw_select,
    kw_from,
    kw_where,
    kw_insert,
    kw_into,
    kw_values,
    kw_create,
    kw_table,
    kw_integer,
    kw_text,
    kw_primary,
    kw_key,
    kw_delete,
    kw_update,
    kw_set,
    kw_drop,
    kw_and,
    kw_or,
    kw_not,
    kw_order,
    kw_by,
    kw_asc,
    kw_desc,
    kw_limit,
    kw_offset,
    kw_count,
    kw_sum,
    kw_avg,
    kw_min,
    kw_max,
    kw_total,
    kw_group,
    kw_join,
    kw_inner,
    kw_left,
    kw_right,
    kw_on,
    kw_distinct,
    kw_having,
    kw_null,
    kw_is,
    kw_alter,
    kw_add,
    kw_column,
    kw_rename,
    kw_to,
    kw_index,
    kw_in,
    kw_begin,
    kw_commit,
    kw_rollback,
    kw_transaction,
    kw_case,
    kw_when,
    kw_then,
    kw_else,
    kw_end,
    kw_like,
    kw_union,
    kw_all,
    kw_intersect,
    kw_except,
    kw_glob,
    kw_between,
    kw_if,
    kw_exists,
    kw_as,
    kw_replace,
    kw_ignore,
    kw_cross,
    kw_outer,
    kw_default,
    kw_autoincrement,
    kw_check,
    kw_real,
    kw_full,

    // Literals
    integer_literal,
    float_literal,
    string_literal,
    identifier,

    // Symbols
    semicolon,
    comma,
    lparen,
    rparen,
    star,
    equals,
    not_equals,
    less_than,
    less_equal,
    greater_than,
    greater_equal,
    dot,

    // Arithmetic operators
    plus,
    minus,
    divide,
    concat,  // ||

    modulo, // %

    // Bitwise operators
    ampersand, // &
    pipe, // |
    tilde, // ~
    left_shift, // <<
    right_shift, // >>

    // Special
    eof,
    invalid,
};

pub const Token = struct {
    type: TokenType,
    lexeme: []const u8,
};

pub const Tokenizer = struct {
    source: []const u8,
    pos: usize,

    pub fn init(source: []const u8) Tokenizer {
        return .{ .source = source, .pos = 0 };
    }

    pub fn next(self: *Tokenizer) Token {
        self.skipWhitespace();

        if (self.pos >= self.source.len) {
            return .{ .type = .eof, .lexeme = "" };
        }

        const c = self.source[self.pos];

        // Multi-character operators
        if (c == '!' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '=') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .not_equals, .lexeme = lexeme };
        }
        if (c == '<' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '>') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .not_equals, .lexeme = lexeme };
        }
        if (c == '<' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '=') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .less_equal, .lexeme = lexeme };
        }
        if (c == '>' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '=') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .greater_equal, .lexeme = lexeme };
        }
        if (c == '|' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '|') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .concat, .lexeme = lexeme };
        }
        if (c == '|') {
            const lexeme = self.source[self.pos .. self.pos + 1];
            self.pos += 1;
            return .{ .type = .pipe, .lexeme = lexeme };
        }
        if (c == '<' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '<') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .left_shift, .lexeme = lexeme };
        }
        if (c == '>' and self.pos + 1 < self.source.len and self.source[self.pos + 1] == '>') {
            const lexeme = self.source[self.pos .. self.pos + 2];
            self.pos += 2;
            return .{ .type = .right_shift, .lexeme = lexeme };
        }

        // Single-character tokens
        const single_token: ?TokenType = switch (c) {
            ';' => .semicolon,
            ',' => .comma,
            '(' => .lparen,
            ')' => .rparen,
            '*' => .star,
            '=' => .equals,
            '<' => .less_than,
            '>' => .greater_than,
            '.' => .dot,
            '+' => .plus,
            '-' => .minus,
            '/' => .divide,
            '%' => .modulo,
            '&' => .ampersand,
            '~' => .tilde,
            else => null,
        };

        if (single_token) |tt| {
            const lexeme = self.source[self.pos .. self.pos + 1];
            self.pos += 1;
            return .{ .type = tt, .lexeme = lexeme };
        }

        // String literal
        if (c == '\'') {
            return self.readString();
        }

        // Integer literal
        if (std.ascii.isDigit(c)) {
            return self.readInteger();
        }

        // Identifier or keyword
        if (std.ascii.isAlphabetic(c) or c == '_') {
            return self.readIdentifierOrKeyword();
        }

        self.pos += 1;
        return .{ .type = .invalid, .lexeme = self.source[self.pos - 1 .. self.pos] };
    }

    fn skipWhitespace(self: *Tokenizer) void {
        while (self.pos < self.source.len) {
            if (std.ascii.isWhitespace(self.source[self.pos])) {
                self.pos += 1;
            } else if (self.pos + 1 < self.source.len and self.source[self.pos] == '-' and self.source[self.pos + 1] == '-') {
                // Skip -- line comment
                self.pos += 2;
                while (self.pos < self.source.len and self.source[self.pos] != '\n') {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn readString(self: *Tokenizer) Token {
        const start = self.pos;
        self.pos += 1; // skip opening quote
        while (self.pos < self.source.len and self.source[self.pos] != '\'') {
            self.pos += 1;
        }
        if (self.pos < self.source.len) {
            self.pos += 1; // skip closing quote
        }
        return .{ .type = .string_literal, .lexeme = self.source[start..self.pos] };
    }

    fn readInteger(self: *Tokenizer) Token {
        const start = self.pos;
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.pos += 1;
        }
        // Check for decimal point (float literal)
        if (self.pos < self.source.len and self.source[self.pos] == '.' and
            self.pos + 1 < self.source.len and std.ascii.isDigit(self.source[self.pos + 1]))
        {
            self.pos += 1; // skip '.'
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
                self.pos += 1;
            }
            return .{ .type = .float_literal, .lexeme = self.source[start..self.pos] };
        }
        return .{ .type = .integer_literal, .lexeme = self.source[start..self.pos] };
    }

    fn readIdentifierOrKeyword(self: *Tokenizer) Token {
        const start = self.pos;
        while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) {
            self.pos += 1;
        }
        const lexeme = self.source[start..self.pos];
        const tt = classifyKeyword(lexeme);
        return .{ .type = tt, .lexeme = lexeme };
    }

    fn classifyKeyword(lexeme: []const u8) TokenType {
        const keywords = .{
            .{ "SELECT", TokenType.kw_select },
            .{ "FROM", TokenType.kw_from },
            .{ "WHERE", TokenType.kw_where },
            .{ "INSERT", TokenType.kw_insert },
            .{ "INTO", TokenType.kw_into },
            .{ "VALUES", TokenType.kw_values },
            .{ "CREATE", TokenType.kw_create },
            .{ "TABLE", TokenType.kw_table },
            .{ "INTEGER", TokenType.kw_integer },
            .{ "TEXT", TokenType.kw_text },
            .{ "PRIMARY", TokenType.kw_primary },
            .{ "KEY", TokenType.kw_key },
            .{ "DELETE", TokenType.kw_delete },
            .{ "UPDATE", TokenType.kw_update },
            .{ "SET", TokenType.kw_set },
            .{ "DROP", TokenType.kw_drop },
            .{ "AND", TokenType.kw_and },
            .{ "OR", TokenType.kw_or },
            .{ "NOT", TokenType.kw_not },
            .{ "ORDER", TokenType.kw_order },
            .{ "BY", TokenType.kw_by },
            .{ "ASC", TokenType.kw_asc },
            .{ "DESC", TokenType.kw_desc },
            .{ "LIMIT", TokenType.kw_limit },
            .{ "OFFSET", TokenType.kw_offset },
            .{ "COUNT", TokenType.kw_count },
            .{ "SUM", TokenType.kw_sum },
            .{ "AVG", TokenType.kw_avg },
            .{ "MIN", TokenType.kw_min },
            .{ "MAX", TokenType.kw_max },
            .{ "TOTAL", TokenType.kw_total },
            .{ "GROUP", TokenType.kw_group },
            .{ "JOIN", TokenType.kw_join },
            .{ "INNER", TokenType.kw_inner },
            .{ "LEFT", TokenType.kw_left },
            .{ "RIGHT", TokenType.kw_right },
            .{ "ON", TokenType.kw_on },
            .{ "DISTINCT", TokenType.kw_distinct },
            .{ "HAVING", TokenType.kw_having },
            .{ "NULL", TokenType.kw_null },
            .{ "IS", TokenType.kw_is },
            .{ "ALTER", TokenType.kw_alter },
            .{ "ADD", TokenType.kw_add },
            .{ "COLUMN", TokenType.kw_column },
            .{ "RENAME", TokenType.kw_rename },
            .{ "TO", TokenType.kw_to },
            .{ "INDEX", TokenType.kw_index },
            .{ "IN", TokenType.kw_in },
            .{ "BEGIN", TokenType.kw_begin },
            .{ "COMMIT", TokenType.kw_commit },
            .{ "ROLLBACK", TokenType.kw_rollback },
            .{ "TRANSACTION", TokenType.kw_transaction },
            .{ "CASE", TokenType.kw_case },
            .{ "WHEN", TokenType.kw_when },
            .{ "THEN", TokenType.kw_then },
            .{ "ELSE", TokenType.kw_else },
            .{ "END", TokenType.kw_end },
            .{ "LIKE", TokenType.kw_like },
            .{ "UNION", TokenType.kw_union },
            .{ "ALL", TokenType.kw_all },
            .{ "INTERSECT", TokenType.kw_intersect },
            .{ "EXCEPT", TokenType.kw_except },
            .{ "GLOB", TokenType.kw_glob },
            .{ "BETWEEN", TokenType.kw_between },
            .{ "IF", TokenType.kw_if },
            .{ "EXISTS", TokenType.kw_exists },
            .{ "AS", TokenType.kw_as },
            .{ "REPLACE", TokenType.kw_replace },
            .{ "IGNORE", TokenType.kw_ignore },
            .{ "CROSS", TokenType.kw_cross },
            .{ "OUTER", TokenType.kw_outer },
            .{ "DEFAULT", TokenType.kw_default },
            .{ "AUTOINCREMENT", TokenType.kw_autoincrement },
            .{ "CHECK", TokenType.kw_check },
            .{ "REAL", TokenType.kw_real },
            .{ "FULL", TokenType.kw_full },
        };

        inline for (keywords) |entry| {
            if (std.ascii.eqlIgnoreCase(lexeme, entry[0])) {
                return entry[1];
            }
        }

        return .identifier;
    }

    pub fn tokenize(self: *Tokenizer, allocator: std.mem.Allocator) ![]Token {
        var tokens: std.ArrayList(Token) = .{};
        defer tokens.deinit(allocator);

        while (true) {
            const tok = self.next();
            try tokens.append(allocator, tok);
            if (tok.type == .eof) break;
        }

        return try tokens.toOwnedSlice(allocator);
    }
};

test "tokenize SELECT * FROM table" {
    var t = Tokenizer.init("SELECT * FROM users;");
    try std.testing.expectEqual(TokenType.kw_select, t.next().type);
    try std.testing.expectEqual(TokenType.star, t.next().type);
    try std.testing.expectEqual(TokenType.kw_from, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.semicolon, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "tokenize CREATE TABLE" {
    var t = Tokenizer.init("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
    try std.testing.expectEqual(TokenType.kw_create, t.next().type);
    try std.testing.expectEqual(TokenType.kw_table, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.lparen, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.kw_integer, t.next().type);
    try std.testing.expectEqual(TokenType.kw_primary, t.next().type);
    try std.testing.expectEqual(TokenType.kw_key, t.next().type);
    try std.testing.expectEqual(TokenType.comma, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.kw_text, t.next().type);
    try std.testing.expectEqual(TokenType.rparen, t.next().type);
    try std.testing.expectEqual(TokenType.semicolon, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "tokenize INSERT INTO" {
    var t = Tokenizer.init("INSERT INTO users VALUES (1, 'alice');");
    try std.testing.expectEqual(TokenType.kw_insert, t.next().type);
    try std.testing.expectEqual(TokenType.kw_into, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.kw_values, t.next().type);
    try std.testing.expectEqual(TokenType.lparen, t.next().type);
    try std.testing.expectEqual(TokenType.integer_literal, t.next().type);
    try std.testing.expectEqual(TokenType.comma, t.next().type);
    try std.testing.expectEqual(TokenType.string_literal, t.next().type);
    try std.testing.expectEqual(TokenType.rparen, t.next().type);
    try std.testing.expectEqual(TokenType.semicolon, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "case insensitive keywords" {
    var t = Tokenizer.init("select FROM Select");
    try std.testing.expectEqual(TokenType.kw_select, t.next().type);
    try std.testing.expectEqual(TokenType.kw_from, t.next().type);
    try std.testing.expectEqual(TokenType.kw_select, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "tokenize comparison operators" {
    var t = Tokenizer.init("= != < <= > >=");
    try std.testing.expectEqual(TokenType.equals, t.next().type);
    try std.testing.expectEqual(TokenType.not_equals, t.next().type);
    try std.testing.expectEqual(TokenType.less_than, t.next().type);
    try std.testing.expectEqual(TokenType.less_equal, t.next().type);
    try std.testing.expectEqual(TokenType.greater_than, t.next().type);
    try std.testing.expectEqual(TokenType.greater_equal, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "tokenize ORDER BY LIMIT OFFSET" {
    var t = Tokenizer.init("ORDER BY name ASC LIMIT 10 OFFSET 5 DESC");
    try std.testing.expectEqual(TokenType.kw_order, t.next().type);
    try std.testing.expectEqual(TokenType.kw_by, t.next().type);
    try std.testing.expectEqual(TokenType.identifier, t.next().type);
    try std.testing.expectEqual(TokenType.kw_asc, t.next().type);
    try std.testing.expectEqual(TokenType.kw_limit, t.next().type);
    try std.testing.expectEqual(TokenType.integer_literal, t.next().type);
    try std.testing.expectEqual(TokenType.kw_offset, t.next().type);
    try std.testing.expectEqual(TokenType.integer_literal, t.next().type);
    try std.testing.expectEqual(TokenType.kw_desc, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}

test "tokenize DELETE UPDATE DROP" {
    var t = Tokenizer.init("DELETE FROM UPDATE SET DROP TABLE");
    try std.testing.expectEqual(TokenType.kw_delete, t.next().type);
    try std.testing.expectEqual(TokenType.kw_from, t.next().type);
    try std.testing.expectEqual(TokenType.kw_update, t.next().type);
    try std.testing.expectEqual(TokenType.kw_set, t.next().type);
    try std.testing.expectEqual(TokenType.kw_drop, t.next().type);
    try std.testing.expectEqual(TokenType.kw_table, t.next().type);
    try std.testing.expectEqual(TokenType.eof, t.next().type);
}
