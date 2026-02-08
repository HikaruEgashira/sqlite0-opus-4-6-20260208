const std = @import("std");

pub const Value = union(enum) {
    integer: i64,
    text: []const u8,
    null_val: void,
};

pub const Row = struct {
    rowid: i64 = 0,
    values: []Value,
};

pub const Column = struct {
    name: []const u8,
    col_type: []const u8,
    is_primary_key: bool,
    default_value: ?[]const u8 = null,
    not_null: bool = false,
    autoincrement: bool = false,
    is_unique: bool = false,
    check_expr_sql: ?[]const u8 = null,
};

pub const TableSnapshot = struct {
    name: []const u8,
    columns: []const Column,
    rows: []const Row,
    next_rowid: i64 = 1,
};

pub fn dupeStr(allocator: std.mem.Allocator, s: []const u8) ![]const u8 {
    const copy = try allocator.alloc(u8, s.len);
    @memcpy(copy, s);
    return copy;
}

pub fn compareValuesOrder(a: Value, b: Value) std.math.Order {
    // Integer vs Integer
    if (a == .integer and b == .integer) {
        return std.math.order(a.integer, b.integer);
    }
    // Text vs Text
    if (a == .text and b == .text) {
        // Try numeric comparison if both parse as numbers
        if (parseTextAsNumber(a.text)) |a_num| {
            if (parseTextAsNumber(b.text)) |b_num| {
                return floatOrder(a_num, b_num);
            }
        }
        return std.mem.order(u8, a.text, b.text);
    }
    // Integer vs Text: try numeric comparison (SQLite3 type affinity)
    if (a == .integer and b == .text) {
        if (parseTextAsNumber(b.text)) |b_num| {
            return floatOrder(@floatFromInt(a.integer), b_num);
        }
        return .lt;
    }
    if (a == .text and b == .integer) {
        if (parseTextAsNumber(a.text)) |a_num| {
            return floatOrder(a_num, @floatFromInt(b.integer));
        }
        return .gt;
    }
    // null_val sorts before everything
    if (a == .null_val and b == .null_val) return .eq;
    if (a == .null_val) return .lt;
    if (b == .null_val) return .gt;
    return .eq;
}

fn parseTextAsNumber(text: []const u8) ?f64 {
    return std.fmt.parseFloat(f64, text) catch null;
}

fn floatOrder(a: f64, b: f64) std.math.Order {
    if (a < b) return .lt;
    if (a > b) return .gt;
    return .eq;
}

/// LIKE pattern matching (SQL compatible)
/// % matches any sequence of characters (0 or more)
/// _ matches any single character
pub fn likeMatch(text: []const u8, pattern: []const u8) !bool {
    var text_idx: usize = 0;
    var pattern_idx: usize = 0;

    while (pattern_idx < pattern.len) {
        if (pattern[pattern_idx] == '%') {
            // Match remaining pattern starting from each position in text
            pattern_idx += 1;
            if (pattern_idx >= pattern.len) return true; // % at end matches anything

            while (text_idx <= text.len) {
                if (try likeMatch(text[text_idx..], pattern[pattern_idx..])) {
                    return true;
                }
                text_idx += 1;
            }
            return false;
        } else if (pattern[pattern_idx] == '_') {
            // Match any single character
            if (text_idx >= text.len) return false;
            text_idx += 1;
            pattern_idx += 1;
        } else {
            // Match literal character (case-insensitive)
            if (text_idx >= text.len) return false;
            const text_char = std.ascii.toLower(text[text_idx]);
            const pattern_char = std.ascii.toLower(pattern[pattern_idx]);
            if (text_char != pattern_char) return false;
            text_idx += 1;
            pattern_idx += 1;
        }
    }

    return text_idx == text.len;
}

/// GLOB pattern matching (SQL compatible, case-sensitive)
/// * matches any sequence of characters (0 or more)
/// ? matches any single character
pub fn globMatch(text: []const u8, pattern: []const u8) !bool {
    var text_idx: usize = 0;
    var pattern_idx: usize = 0;

    while (pattern_idx < pattern.len) {
        if (pattern[pattern_idx] == '*') {
            pattern_idx += 1;
            if (pattern_idx >= pattern.len) return true;

            while (text_idx <= text.len) {
                if (try globMatch(text[text_idx..], pattern[pattern_idx..])) {
                    return true;
                }
                text_idx += 1;
            }
            return false;
        } else if (pattern[pattern_idx] == '?') {
            if (text_idx >= text.len) return false;
            text_idx += 1;
            pattern_idx += 1;
        } else {
            // Match literal character (case-sensitive)
            if (text_idx >= text.len) return false;
            if (text[text_idx] != pattern[pattern_idx]) return false;
            text_idx += 1;
            pattern_idx += 1;
        }
    }

    return text_idx == text.len;
}

pub fn rowsEqual(a: Row, b: Row) bool {
    if (a.values.len != b.values.len) return false;
    for (a.values, b.values) |va, vb| {
        if (compareValuesOrder(va, vb) != .eq) return false;
    }
    return true;
}
