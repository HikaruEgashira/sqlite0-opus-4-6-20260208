const std = @import("std");
const value_mod = @import("value.zig");
const table_mod = @import("table.zig");

const Value = value_mod.Value;
const Row = value_mod.Row;
const Column = value_mod.Column;
const TableSnapshot = value_mod.TableSnapshot;
const Table = table_mod.Table;
const dupeStr = value_mod.dupeStr;

const root = @import("root.zig");
const Database = root.Database;

pub const ProjectedState = struct {
    rows: ?[]Row,
    values: ?[][]Value,
    texts: ?[][]const u8,
    col_names: ?[][]const u8,
};

pub fn freeProjected(self: *Database) void {
    if (self.projected_texts) |pt| {
        for (pt) |t| {
            self.allocator.free(t);
        }
        self.allocator.free(pt);
        self.projected_texts = null;
    }
    if (self.projected_values) |pv| {
        for (pv) |v| {
            self.allocator.free(v);
        }
        self.allocator.free(pv);
        self.projected_values = null;
    }
    if (self.projected_column_names) |pcn| {
        for (pcn) |name| {
            self.allocator.free(name);
        }
        self.allocator.free(pcn);
        self.projected_column_names = null;
    }
    if (self.projected_rows) |pr| {
        self.allocator.free(pr);
        self.projected_rows = null;
    }
    // Clean up CTE temporary tables
    if (self.temp_cte_names) |names| {
        for (names) |name| {
            // Must remove from hash map BEFORE deiniting table,
            // because table.name is the same allocation as the hash map key
            if (self.tables.fetchRemove(name)) |kv| {
                var tbl = kv.value;
                tbl.deinit();
            }
            self.allocator.free(name);
        }
        self.allocator.free(names);
        self.temp_cte_names = null;
    }
}

pub fn saveProjectedState(self: *Database) ProjectedState {
    const state = ProjectedState{
        .rows = self.projected_rows,
        .values = self.projected_values,
        .texts = self.projected_texts,
        .col_names = self.projected_column_names,
    };
    self.projected_rows = null;
    self.projected_values = null;
    self.projected_texts = null;
    self.projected_column_names = null;
    return state;
}

pub fn restoreProjectedState(self: *Database, state: ProjectedState) void {
    self.projected_rows = state.rows;
    self.projected_values = state.values;
    self.projected_texts = state.texts;
    self.projected_column_names = state.col_names;
}

pub fn freeSnapshot(self: *Database) void {
    if (self.transaction_snapshot) |snapshots| {
        for (snapshots) |snap| {
            for (snap.rows) |row| {
                for (row.values) |val| {
                    switch (val) {
                        .text => |t| self.allocator.free(t),
                        else => {},
                    }
                }
                self.allocator.free(row.values);
            }
            self.allocator.free(snap.rows);
            for (snap.columns) |col| {
                self.allocator.free(col.name);
            }
            self.allocator.free(snap.columns);
            self.allocator.free(snap.name);
        }
        self.allocator.free(snapshots);
        self.transaction_snapshot = null;
    }
}

pub fn takeSnapshot(self: *Database) !void {
    var snapshots: std.ArrayList(TableSnapshot) = .{};
    var it = self.tables.iterator();
    while (it.next()) |entry| {
        const table = entry.value_ptr;
        // Deep copy columns
        var cols = try self.allocator.alloc(Column, table.columns.len);
        for (table.columns, 0..) |col, i| {
            cols[i] = .{
                .name = try dupeStr(self.allocator, col.name),
                .col_type = col.col_type,
                .is_primary_key = col.is_primary_key,
                .is_unique = col.is_unique,
            };
        }
        // Deep copy rows
        const src_rows = table.storage().scan();
        var rows = try self.allocator.alloc(Row, src_rows.len);
        for (src_rows, 0..) |row, ri| {
            var values = try self.allocator.alloc(Value, row.values.len);
            for (row.values, 0..) |val, vi| {
                values[vi] = switch (val) {
                    .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                    .integer => |n| .{ .integer = n },
                    .null_val => .null_val,
                };
            }
            rows[ri] = .{ .values = values };
        }
        try snapshots.append(self.allocator, .{
            .name = try dupeStr(self.allocator, table.name),
            .columns = cols,
            .rows = rows,
        });
    }
    self.transaction_snapshot = try snapshots.toOwnedSlice(self.allocator);
}

pub fn restoreSnapshot(self: *Database) !void {
    if (self.transaction_snapshot == null) return;
    const snapshots = self.transaction_snapshot.?;

    // Drop all current tables
    var it = self.tables.valueIterator();
    while (it.next()) |table| {
        table.deinit();
    }
    self.tables.clearRetainingCapacity();

    // Restore from snapshot
    for (snapshots) |snap| {
        const table_name = try dupeStr(self.allocator, snap.name);
        var cols = try self.allocator.alloc(Column, snap.columns.len);
        for (snap.columns, 0..) |col, i| {
            cols[i] = .{
                .name = try dupeStr(self.allocator, col.name),
                .col_type = col.col_type,
                .is_primary_key = col.is_primary_key,
                .is_unique = col.is_unique,
            };
        }
        var table = Table.init(self.allocator, table_name, cols);
        for (snap.rows) |row| {
            var values = try self.allocator.alloc(Value, row.values.len);
            for (row.values, 0..) |val, vi| {
                values[vi] = switch (val) {
                    .text => |t| .{ .text = try dupeStr(self.allocator, t) },
                    .integer => |n| .{ .integer = n },
                    .null_val => .null_val,
                };
            }
            try table.storage().append(self.allocator, .{ .values = values });
        }
        try self.tables.put(table_name, table);
    }

    // Free the snapshot
    freeSnapshot(self);
}
