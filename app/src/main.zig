const std = @import("std");
const core = @import("core");

fn writeAll(file: std.fs.File, bytes: []const u8) !void {
    var index: usize = 0;
    while (index < bytes.len) {
        index += file.write(bytes[index..]) catch |err| return err;
    }
}

fn printStr(file: std.fs.File, comptime fmt: []const u8, args: anytype) !void {
    var buf: [4096]u8 = undefined;
    const slice = std.fmt.bufPrint(&buf, fmt, args) catch return error.NoSpaceLeft;
    try writeAll(file, slice);
}

fn readLine(stdin: std.fs.File, line_buf: *[4096]u8) !?[]const u8 {
    var line_len: usize = 0;
    while (line_len < line_buf.len) {
        var byte: [1]u8 = undefined;
        const n = stdin.read(&byte) catch break;
        if (n == 0) {
            if (line_len == 0) return null;
            break;
        }
        if (byte[0] == '\n') break;
        line_buf[line_len] = byte[0];
        line_len += 1;
    }
    return line_buf[0..line_len];
}

fn endsWithSemicolon(s: []const u8) bool {
    // Strip trailing inline comment (-- ...) before checking
    var effective_end: usize = s.len;
    // Find last "--" that isn't inside quotes
    var in_single_quote = false;
    var in_double_quote = false;
    var j: usize = 0;
    while (j < s.len) : (j += 1) {
        if (s[j] == '\'' and !in_double_quote) {
            in_single_quote = !in_single_quote;
        } else if (s[j] == '"' and !in_single_quote) {
            in_double_quote = !in_double_quote;
        } else if (!in_single_quote and !in_double_quote and j + 1 < s.len and s[j] == '-' and s[j + 1] == '-') {
            effective_end = j;
            break;
        }
    }
    // Check if the effective content ends with ';'
    var i: usize = effective_end;
    while (i > 0) {
        i -= 1;
        if (!std.ascii.isWhitespace(s[i])) {
            return s[i] == ';';
        }
    }
    return false;
}

pub fn main() !void {
    const stdout = std.fs.File.stdout();
    const stdin = std.fs.File.stdin();

    try writeAll(stdout, "sqlite0 v0.1.0\n");
    try writeAll(stdout, "Enter SQL statements (Ctrl+D to exit)\n");

    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = core.Database.init(allocator);
    defer db.deinit();

    var line_buf: [4096]u8 = undefined;
    var sql_buf: std.ArrayList(u8) = .{};
    defer sql_buf.deinit(allocator);

    while (true) {
        const is_continuation = sql_buf.items.len > 0;
        if (is_continuation) {
            try writeAll(stdout, "   ...> ");
        } else {
            try writeAll(stdout, "sqlite0> ");
        }

        const line = try readLine(stdin, &line_buf) orelse {
            if (is_continuation) {
                sql_buf.clearRetainingCapacity();
            }
            try writeAll(stdout, "\nBye!\n");
            return;
        };

        if (line.len == 0 and !is_continuation) continue;

        // Handle dot-commands (only on first line)
        if (!is_continuation and line.len > 0 and line[0] == '.') {
            if (std.mem.eql(u8, line, ".tables")) {
                var it = db.tables.keyIterator();
                while (it.next()) |key| {
                    try writeAll(stdout, key.*);
                    try writeAll(stdout, "\n");
                }
                continue;
            } else if (std.mem.eql(u8, line, ".schema")) {
                var it = db.tables.iterator();
                while (it.next()) |entry| {
                    try writeAll(stdout, "CREATE TABLE ");
                    try writeAll(stdout, entry.key_ptr.*);
                    try writeAll(stdout, " (");
                    for (entry.value_ptr.columns, 0..) |col, ci| {
                        if (ci > 0) try writeAll(stdout, ", ");
                        try writeAll(stdout, col.name);
                        try writeAll(stdout, " ");
                        try writeAll(stdout, col.col_type);
                        if (col.is_primary_key) try writeAll(stdout, " PRIMARY KEY");
                        if (col.not_null) try writeAll(stdout, " NOT NULL");
                        if (col.is_unique) try writeAll(stdout, " UNIQUE");
                    }
                    try writeAll(stdout, ");\n");
                }
                continue;
            } else if (std.mem.eql(u8, line, ".quit") or std.mem.eql(u8, line, ".exit")) {
                try writeAll(stdout, "Bye!\n");
                return;
            } else {
                try writeAll(stdout, "Unknown command: ");
                try writeAll(stdout, line);
                try writeAll(stdout, "\n");
                continue;
            }
        }

        // Append line to SQL buffer
        if (sql_buf.items.len > 0) {
            try sql_buf.append(allocator, ' ');
        }
        try sql_buf.appendSlice(allocator, line);

        // Check if statement is complete (ends with semicolon)
        if (!endsWithSemicolon(sql_buf.items)) {
            // Skip comment-only lines that won't end with semicolon
            const trimmed = std.mem.trim(u8, sql_buf.items, " \t\r\n");
            if (trimmed.len >= 2 and trimmed[0] == '-' and trimmed[1] == '-') {
                sql_buf.clearRetainingCapacity();
                continue;
            }
            continue;
        }

        // Execute the complete SQL statement
        const result = db.execute(sql_buf.items) catch |err| {
            try printStr(stdout, "Error: {}\n", .{err});
            sql_buf.clearRetainingCapacity();
            continue;
        };

        sql_buf.clearRetainingCapacity();

        switch (result) {
            .ok => try writeAll(stdout, "OK\n"),
            .rows => |rows| {
                for (rows) |row| {
                    for (row.values, 0..) |val, i| {
                        if (i > 0) try writeAll(stdout, "|");
                        switch (val) {
                            .integer => |n| try printStr(stdout, "{d}", .{n}),
                            .text => |t| try writeAll(stdout, t),
                            .null_val => {},
                        }
                    }
                    try writeAll(stdout, "\n");
                }
            },
            .err => |msg| {
                try writeAll(stdout, "Error: ");
                try writeAll(stdout, msg);
                try writeAll(stdout, "\n");
            },
        }
    }
}
