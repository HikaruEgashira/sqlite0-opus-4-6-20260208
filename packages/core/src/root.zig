const std = @import("std");
const tokenizer = @import("tokenizer");
const parser = @import("parser");

pub const Tokenizer = tokenizer.Tokenizer;
pub const Parser = parser.Parser;
pub const Statement = parser.Statement;

pub const Value = union(enum) {
    integer: i64,
    text: []const u8,
    null_val: void,
};

pub const Row = struct {
    values: []Value,
};

pub const Column = struct {
    name: []const u8,
    col_type: []const u8,
    is_primary_key: bool,
};

fn dupeStr(allocator: std.mem.Allocator, s: []const u8) ![]const u8 {
    const copy = try allocator.alloc(u8, s.len);
    @memcpy(copy, s);
    return copy;
}

pub const Table = struct {
    name: []const u8,
    columns: []const Column,
    rows: std.ArrayList(Row),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, columns: []const Column) Table {
        return .{
            .name = name,
            .columns = columns,
            .rows = .{},
            .allocator = allocator,
        };
    }

    pub fn insertRow(self: *Table, raw_values: []const []const u8) !void {
        var values = try self.allocator.alloc(Value, raw_values.len);
        for (raw_values, 0..) |raw, i| {
            if (raw.len >= 2 and raw[0] == '\'') {
                values[i] = .{ .text = try dupeStr(self.allocator, raw[1 .. raw.len - 1]) };
            } else {
                const num = std.fmt.parseInt(i64, raw, 10) catch {
                    values[i] = .{ .text = try dupeStr(self.allocator, raw) };
                    continue;
                };
                values[i] = .{ .integer = num };
            }
        }
        try self.rows.append(self.allocator, .{ .values = values });
    }

    pub fn deinit(self: *Table) void {
        for (self.rows.items) |row| {
            for (row.values) |val| {
                switch (val) {
                    .text => |t| self.allocator.free(t),
                    else => {},
                }
            }
            self.allocator.free(row.values);
        }
        self.rows.deinit(self.allocator);
        for (self.columns) |col| {
            self.allocator.free(col.name);
        }
        self.allocator.free(self.columns);
        self.allocator.free(self.name);
    }
};

pub const Database = struct {
    tables: std.StringHashMap(Table),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{
            .tables = std.StringHashMap(Table).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Database) void {
        var it = self.tables.valueIterator();
        while (it.next()) |table| {
            table.deinit();
        }
        self.tables.deinit();
    }

    pub fn execute(self: *Database, sql: []const u8) !ExecuteResult {
        var tok = Tokenizer.init(sql);
        const tokens = try tok.tokenize(self.allocator);
        defer self.allocator.free(tokens);

        var p = Parser.init(self.allocator, tokens);
        const stmt = try p.parse();

        switch (stmt) {
            .create_table => |ct| {
                defer self.allocator.free(ct.columns);
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
            .insert => |ins| {
                defer self.allocator.free(ins.values);
                if (self.tables.getPtr(ins.table_name)) |table| {
                    try table.insertRow(ins.values);
                    return .ok;
                }
                return .{ .err = "table not found" };
            },
            .select_stmt => |sel| {
                defer self.allocator.free(sel.columns);
                if (self.tables.get(sel.table_name)) |table| {
                    return .{ .rows = table.rows.items };
                }
                return .{ .err = "table not found" };
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
