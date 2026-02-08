const std = @import("std");
const value_mod = @import("value.zig");
const parser = @import("parser");

pub const Value = value_mod.Value;
pub const Row = value_mod.Row;
pub const Column = value_mod.Column;
const dupeStr = value_mod.dupeStr;

const Expr = parser.Expr;

pub fn valueToBool(val: Value) bool {
    return switch (val) {
        .integer => |n| n != 0,
        .text => true,
        .null_val => false,
    };
}

pub fn valueToText(allocator: std.mem.Allocator, val: Value) []const u8 {
    switch (val) {
        .text => |t| return dupeStr(allocator, t) catch "",
        .integer => |n| {
            var buf: [32]u8 = undefined;
            const slice = std.fmt.bufPrint(&buf, "{d}", .{n}) catch return "";
            return dupeStr(allocator, slice) catch "";
        },
        .null_val => return dupeStr(allocator, "") catch "",
    }
}

pub fn valueToI64(val: Value) ?i64 {
    switch (val) {
        .integer => |n| return n,
        .text => |t| return std.fmt.parseInt(i64, t, 10) catch null,
        .null_val => return null,
    }
}

pub fn valueToF64(val: Value) ?f64 {
    switch (val) {
        .integer => |n| return @floatFromInt(n),
        .text => |t| return std.fmt.parseFloat(f64, t) catch null,
        .null_val => return null,
    }
}

pub fn isFloatValue(val: Value) bool {
    switch (val) {
        .text => |t| {
            for (t) |c| {
                if (c == '.') return true;
            }
            return false;
        },
        else => return false,
    }
}

pub fn formatFloat(allocator: std.mem.Allocator, f_in: f64) Value {
    // Normalize negative zero to positive zero (SQLite behavior)
    const f = if (f_in == 0.0) @as(f64, 0.0) else f_in;
    var buf: [64]u8 = undefined;

    const full = std.fmt.bufPrint(&buf, "{d}", .{f}) catch return .null_val;

    // Count significant digits
    var sig_count: usize = 0;
    var started = false;
    var has_dot = false;
    for (full) |c| {
        if (c == '-') continue;
        if (c == '.') {
            has_dot = true;
            continue;
        }
        if (c == 'e' or c == 'E') break;
        if (c != '0') started = true;
        if (started) sig_count += 1;
    }

    if (sig_count <= 15) {
        if (!has_dot) {
            const txt = std.fmt.allocPrint(allocator, "{s}.0", .{full}) catch return .null_val;
            return .{ .text = txt };
        }
        const txt = allocator.alloc(u8, full.len) catch return .null_val;
        @memcpy(txt, full);
        return .{ .text = txt };
    }

    // Need to truncate: rebuild with 15 significant digits via rounding
    var result_buf: [64]u8 = undefined;
    var ri: usize = 0;
    var sig: usize = 0;
    var s = false;
    for (full, 0..) |c, i| {
        if (c == '-' or c == '.') {
            result_buf[ri] = c;
            ri += 1;
            continue;
        }
        if (c == 'e' or c == 'E') {
            const rest = full[i..];
            @memcpy(result_buf[ri .. ri + rest.len], rest);
            ri += rest.len;
            break;
        }
        if (c != '0') s = true;
        if (s) sig += 1;
        if (sig <= 15) {
            result_buf[ri] = c;
            ri += 1;
        } else if (sig == 16) {
            if (c >= '5') {
                var j = ri;
                while (j > 0) {
                    j -= 1;
                    if (result_buf[j] == '.') continue;
                    if (result_buf[j] == '-') break;
                    if (result_buf[j] < '9') {
                        result_buf[j] += 1;
                        break;
                    } else {
                        result_buf[j] = '0';
                    }
                }
            }
            break;
        }
    }

    // Remove trailing zeros after decimal point
    var end = ri;
    if (std.mem.indexOfScalar(u8, result_buf[0..ri], '.')) |dot_pos| {
        while (end > dot_pos + 2 and result_buf[end - 1] == '0') {
            end -= 1;
        }
    }
    const txt = allocator.alloc(u8, end) catch return .null_val;
    @memcpy(txt, result_buf[0..end]);
    return .{ .text = txt };
}

pub fn freeExprDeep(allocator: std.mem.Allocator, expr: *const Expr) void {
    switch (expr.*) {
        .binary_op => |bin| {
            freeExprDeep(allocator, bin.left);
            freeExprDeep(allocator, bin.right);
        },
        .aggregate => |agg| {
            freeExprDeep(allocator, agg.arg);
        },
        .case_when => |cw| {
            for (cw.conditions) |cond| freeExprDeep(allocator, cond);
            allocator.free(cw.conditions);
            for (cw.results) |res| freeExprDeep(allocator, res);
            allocator.free(cw.results);
            if (cw.else_result) |er| freeExprDeep(allocator, er);
        },
        .unary_op => |u| {
            freeExprDeep(allocator, u.operand);
        },
        .in_list => |il| {
            freeExprDeep(allocator, il.operand);
            allocator.free(@constCast(il.subquery_sql));
        },
        .in_values => |iv| {
            freeExprDeep(allocator, iv.operand);
            for (iv.values) |v| freeExprDeep(allocator, v);
            allocator.free(iv.values);
        },
        .scalar_subquery => |sq| {
            allocator.free(@constCast(sq.subquery_sql));
        },
        .exists => |ex| {
            allocator.free(@constCast(ex.subquery_sql));
        },
        .scalar_func => |sf| {
            for (sf.args) |arg| freeExprDeep(allocator, arg);
            allocator.free(sf.args);
        },
        .cast => |c_val| {
            freeExprDeep(allocator, c_val.operand);
        },
        .window_func => |wf| {
            if (wf.arg) |arg| freeExprDeep(allocator, arg);
            if (wf.default_val) |dv| freeExprDeep(allocator, dv);
            for (wf.order_by) |ob| {
                if (ob.expr) |e| freeExprDeep(allocator, e);
            }
            if (wf.order_by.len > 0) allocator.free(wf.order_by);
            if (wf.partition_by.len > 0) allocator.free(wf.partition_by);
        },
        else => {},
    }
    allocator.destroy(@constCast(expr));
}
