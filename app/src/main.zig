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

    var buf: [4096]u8 = undefined;
    while (true) {
        try writeAll(stdout, "sqlite0> ");

        var line_len: usize = 0;
        while (line_len < buf.len) {
            var byte: [1]u8 = undefined;
            const n = stdin.read(&byte) catch break;
            if (n == 0) {
                if (line_len == 0) {
                    try writeAll(stdout, "\nBye!\n");
                    return;
                }
                break;
            }
            if (byte[0] == '\n') break;
            buf[line_len] = byte[0];
            line_len += 1;
        }
        const line = buf[0..line_len];

        if (line.len == 0) continue;

        const result = db.execute(line) catch |err| {
            try printStr(stdout, "Error: {}\n", .{err});
            continue;
        };

        switch (result) {
            .ok => try writeAll(stdout, "OK\n"),
            .rows => |rows| {
                for (rows) |row| {
                    for (row.values, 0..) |val, i| {
                        if (i > 0) try writeAll(stdout, "|");
                        switch (val) {
                            .integer => |n| try printStr(stdout, "{d}", .{n}),
                            .text => |t| try writeAll(stdout, t),
                            .null_val => try writeAll(stdout, "NULL"),
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
