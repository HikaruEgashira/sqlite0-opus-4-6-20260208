const std = @import("std");
const value_mod = @import("value.zig");
const row_storage = @import("row_storage.zig");

const Row = value_mod.Row;
const RowStorage = row_storage.RowStorage;

/// B+Tree order: each node holds up to MAX_KEYS keys.
/// Internal nodes have up to MAX_KEYS+1 children.
const ORDER = 4;
const MAX_KEYS = 2 * ORDER - 1; // 7
const MIN_KEYS = ORDER - 1; // 3

const BTreeNode = struct {
    is_leaf: bool,
    num_keys: usize,
    keys: [MAX_KEYS]i64,
    // Leaf: rows stored here; Internal: unused
    rows: [MAX_KEYS]Row,
    // Internal: child pointers; Leaf: unused
    children: [MAX_KEYS + 1]?*BTreeNode,
    // Leaf linked list
    next_leaf: ?*BTreeNode,
    prev_leaf: ?*BTreeNode,
    parent: ?*BTreeNode,

    fn init(is_leaf: bool) BTreeNode {
        return .{
            .is_leaf = is_leaf,
            .num_keys = 0,
            .keys = undefined,
            .rows = undefined,
            .children = .{null} ** (MAX_KEYS + 1),
            .next_leaf = null,
            .prev_leaf = null,
            .parent = null,
        };
    }

    /// Binary search for key position in this node.
    /// Returns the index where key should be (or is).
    fn findKeyPos(self: *const BTreeNode, key: i64) usize {
        var lo: usize = 0;
        var hi: usize = self.num_keys;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.keys[mid] < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /// Insert a key-row pair at position, shifting existing entries right.
    fn insertAt(self: *BTreeNode, pos: usize, key: i64, row: Row) void {
        // Shift keys and rows right
        var i: usize = self.num_keys;
        while (i > pos) : (i -= 1) {
            self.keys[i] = self.keys[i - 1];
            self.rows[i] = self.rows[i - 1];
        }
        self.keys[pos] = key;
        self.rows[pos] = row;
        self.num_keys += 1;
    }

    /// Remove key-row at position, shifting remaining entries left.
    fn removeAt(self: *BTreeNode, pos: usize) Row {
        const row = self.rows[pos];
        var i: usize = pos;
        while (i + 1 < self.num_keys) : (i += 1) {
            self.keys[i] = self.keys[i + 1];
            self.rows[i] = self.rows[i + 1];
        }
        self.num_keys -= 1;
        return row;
    }

    /// Insert a child pointer at position in an internal node, shifting right.
    fn insertChildAt(self: *BTreeNode, pos: usize, key: i64, child: *BTreeNode) void {
        // Shift keys right
        var i: usize = self.num_keys;
        while (i > pos) : (i -= 1) {
            self.keys[i] = self.keys[i - 1];
        }
        // Shift children right
        i = self.num_keys + 1;
        while (i > pos + 1) : (i -= 1) {
            self.children[i] = self.children[i - 1];
        }
        self.keys[pos] = key;
        self.children[pos + 1] = child;
        child.parent = self;
        self.num_keys += 1;
    }

    /// Remove key and right child at position in an internal node.
    fn removeChildAt(self: *BTreeNode, pos: usize) void {
        var i: usize = pos;
        while (i + 1 < self.num_keys) : (i += 1) {
            self.keys[i] = self.keys[i + 1];
        }
        i = pos + 1;
        while (i < self.num_keys) : (i += 1) {
            self.children[i] = self.children[i + 1];
        }
        self.children[self.num_keys] = null;
        self.num_keys -= 1;
    }
};

/// B+Tree-backed row storage.
/// Rows are stored in leaf nodes keyed by rowid.
/// A materialized array cache provides scan()/scanMut() compatibility.
pub const BTreeStorage = struct {
    root: *BTreeNode,
    allocator: std.mem.Allocator,
    count: usize,
    // Materialized cache (invalidated on mutation)
    cache: ?[]Row,
    write_dirty: bool,

    const vtable = RowStorage.VTable{
        .scan = scan,
        .scanMut = scanMut,
        .len = getLen,
        .append = appendRow,
        .orderedRemove = remove,
        .clearRetainingCapacity = clear,
        .deinit = deinitStorage,
    };

    pub fn init(allocator: std.mem.Allocator) BTreeStorage {
        const root = allocator.create(BTreeNode) catch @panic("OOM");
        root.* = BTreeNode.init(true);
        return .{
            .root = root,
            .allocator = allocator,
            .count = 0,
            .cache = null,
            .write_dirty = false,
        };
    }

    pub fn storage(self: *const BTreeStorage) RowStorage {
        return .{
            .ptr = @ptrCast(@constCast(self)),
            .vtable = &vtable,
        };
    }

    // --- Vtable implementations ---

    fn scan(ptr: *anyopaque) []const Row {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        self.syncIfDirty();
        self.materializeIfNeeded();
        return if (self.cache) |c| c else &.{};
    }

    fn scanMut(ptr: *anyopaque) []Row {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        self.syncIfDirty();
        self.materializeIfNeeded();
        self.write_dirty = true;
        return if (self.cache) |c| c else &.{};
    }

    fn getLen(ptr: *anyopaque) usize {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        return self.count;
    }

    fn appendRow(ptr: *anyopaque, _: std.mem.Allocator, row: Row) error{OutOfMemory}!void {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        self.syncIfDirty();
        self.invalidateCache();
        try self.insertInternal(row.rowid, row);
        self.count += 1;
    }

    fn remove(ptr: *anyopaque, index: usize) Row {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        self.syncIfDirty();
        self.materializeIfNeeded();
        // Get the rowid from the cached row at this index
        const rowid = if (self.cache) |c| c[index].rowid else return Row{ .values = &.{} };
        self.invalidateCache();
        return self.deleteInternal(rowid) orelse Row{ .values = &.{} };
    }

    fn clear(ptr: *anyopaque) void {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        self.invalidateCache();
        self.write_dirty = false;
        self.freeAllNodes(self.root);
        const new_root = self.allocator.create(BTreeNode) catch return;
        new_root.* = BTreeNode.init(true);
        self.root = new_root;
        self.count = 0;
    }

    fn deinitStorage(ptr: *anyopaque, _: std.mem.Allocator) void {
        const self: *BTreeStorage = @ptrCast(@alignCast(ptr));
        self.invalidateCache();
        self.freeAllNodes(self.root);
    }

    // --- B+Tree core operations ---

    /// Find the leaf node where a key belongs.
    /// In B+Tree routing: children[i] has keys < keys[i], children[i+1] has keys >= keys[i].
    /// So we follow children[pos] where pos is the first index with keys[pos] > key.
    fn findLeaf(self: *BTreeStorage, key: i64) *BTreeNode {
        var node = self.root;
        while (!node.is_leaf) {
            var pos: usize = 0;
            while (pos < node.num_keys and key >= node.keys[pos]) {
                pos += 1;
            }
            node = node.children[pos] orelse break;
        }
        return node;
    }

    /// Insert a key-row pair into the B+Tree.
    fn insertInternal(self: *BTreeStorage, key: i64, row: Row) error{OutOfMemory}!void {
        var leaf = self.findLeaf(key);
        const pos = leaf.findKeyPos(key);

        if (leaf.num_keys < MAX_KEYS) {
            // Room in leaf — just insert
            leaf.insertAt(pos, key, row);
        } else {
            // Leaf is full — split
            try self.splitLeafAndInsert(leaf, pos, key, row);
        }
    }

    /// Split a full leaf node and insert the new key-row.
    fn splitLeafAndInsert(self: *BTreeStorage, leaf: *BTreeNode, pos: usize, key: i64, row: Row) error{OutOfMemory}!void {
        // Create temporary overflow arrays
        var tmp_keys: [MAX_KEYS + 1]i64 = undefined;
        var tmp_rows: [MAX_KEYS + 1]Row = undefined;

        // Copy existing entries, inserting the new one at pos
        var j: usize = 0;
        for (0..leaf.num_keys) |i| {
            if (j == pos) {
                tmp_keys[j] = key;
                tmp_rows[j] = row;
                j += 1;
            }
            tmp_keys[j] = leaf.keys[i];
            tmp_rows[j] = leaf.rows[i];
            j += 1;
        }
        if (j == pos) {
            tmp_keys[j] = key;
            tmp_rows[j] = row;
            j += 1;
        }
        const total = j;

        // Split point: left gets ceil(total/2)
        const split = (total + 1) / 2;

        // Update current leaf with left half
        leaf.num_keys = split;
        for (0..split) |i| {
            leaf.keys[i] = tmp_keys[i];
            leaf.rows[i] = tmp_rows[i];
        }

        // Create new right leaf
        const new_leaf = try self.allocator.create(BTreeNode);
        new_leaf.* = BTreeNode.init(true);
        new_leaf.num_keys = total - split;
        for (0..new_leaf.num_keys) |i| {
            new_leaf.keys[i] = tmp_keys[split + i];
            new_leaf.rows[i] = tmp_rows[split + i];
        }

        // Update leaf linked list
        new_leaf.next_leaf = leaf.next_leaf;
        new_leaf.prev_leaf = leaf;
        if (leaf.next_leaf) |next| {
            next.prev_leaf = new_leaf;
        }
        leaf.next_leaf = new_leaf;

        // Promote the first key of the new leaf to the parent
        const promote_key = new_leaf.keys[0];
        try self.insertIntoParent(leaf, promote_key, new_leaf);
    }

    /// Insert a new key and child pointer into a parent node (after a split).
    fn insertIntoParent(self: *BTreeStorage, left: *BTreeNode, key: i64, right: *BTreeNode) error{OutOfMemory}!void {
        if (left.parent == null) {
            // Create new root
            const new_root = try self.allocator.create(BTreeNode);
            new_root.* = BTreeNode.init(false);
            new_root.keys[0] = key;
            new_root.num_keys = 1;
            new_root.children[0] = left;
            new_root.children[1] = right;
            left.parent = new_root;
            right.parent = new_root;
            self.root = new_root;
            return;
        }

        var parent = left.parent.?;
        if (parent.num_keys < MAX_KEYS) {
            // Room in parent
            const pos = parent.findKeyPos(key);
            parent.insertChildAt(pos, key, right);
        } else {
            // Split parent
            try self.splitInternalAndInsert(parent, key, right);
        }
    }

    /// Split a full internal node and insert a new key-child.
    fn splitInternalAndInsert(self: *BTreeStorage, node: *BTreeNode, key: i64, right_child: *BTreeNode) error{OutOfMemory}!void {
        var tmp_keys: [MAX_KEYS + 1]i64 = undefined;
        var tmp_children: [MAX_KEYS + 2]?*BTreeNode = .{null} ** (MAX_KEYS + 2);

        const pos = node.findKeyPos(key);

        // Build temporary arrays with the new key-child inserted
        var j: usize = 0;
        tmp_children[0] = node.children[0];
        for (0..node.num_keys) |i| {
            if (j == pos) {
                tmp_keys[j] = key;
                tmp_children[j + 1] = right_child;
                j += 1;
            }
            tmp_keys[j] = node.keys[i];
            tmp_children[j + 1] = node.children[i + 1];
            j += 1;
        }
        if (j == pos) {
            tmp_keys[j] = key;
            tmp_children[j + 1] = right_child;
            j += 1;
        }
        const total = j;

        // Split: left gets first split keys, right gets rest, middle key promoted
        const split = total / 2;
        const promote_key = tmp_keys[split];

        // Update current node with left half
        node.num_keys = split;
        for (0..split) |i| {
            node.keys[i] = tmp_keys[i];
            node.children[i] = tmp_children[i];
        }
        node.children[split] = tmp_children[split];
        // Clear remaining children
        for (split + 1..MAX_KEYS + 1) |i| {
            node.children[i] = null;
        }

        // Create new right internal node
        const new_node = try self.allocator.create(BTreeNode);
        new_node.* = BTreeNode.init(false);
        new_node.num_keys = total - split - 1;
        for (0..new_node.num_keys) |i| {
            new_node.keys[i] = tmp_keys[split + 1 + i];
            new_node.children[i] = tmp_children[split + 1 + i];
            if (new_node.children[i]) |c| c.parent = new_node;
        }
        new_node.children[new_node.num_keys] = tmp_children[total];
        if (new_node.children[new_node.num_keys]) |c| c.parent = new_node;

        // Update children parents for left node
        for (0..node.num_keys + 1) |i| {
            if (node.children[i]) |c| c.parent = node;
        }

        // Promote to parent
        try self.insertIntoParent(node, promote_key, new_node);
    }

    /// Delete a key from the B+Tree. Returns the removed Row if found.
    fn deleteInternal(self: *BTreeStorage, key: i64) ?Row {
        var leaf = self.findLeaf(key);
        const pos = leaf.findKeyPos(key);

        // Check if key actually exists at this position
        if (pos >= leaf.num_keys or leaf.keys[pos] != key) return null;

        const row = leaf.removeAt(pos);
        self.count -= 1;

        // If root is empty leaf, that's fine (empty tree)
        if (leaf == self.root) return row;

        // Check for underflow
        if (leaf.num_keys < MIN_KEYS) {
            self.handleLeafUnderflow(leaf);
        } else {
            // Update parent key if we removed the first key
            if (pos == 0 and leaf.num_keys > 0) {
                self.updateParentKey(leaf, key, leaf.keys[0]);
            }
        }

        return row;
    }

    /// Handle underflow in a leaf node after deletion.
    fn handleLeafUnderflow(self: *BTreeStorage, leaf: *BTreeNode) void {
        // Try to borrow from left sibling
        if (leaf.prev_leaf) |left_sib| {
            if (left_sib.parent == leaf.parent and left_sib.num_keys > MIN_KEYS) {
                // Borrow last entry from left sibling
                const borrow_key = left_sib.keys[left_sib.num_keys - 1];
                const borrow_row = left_sib.rows[left_sib.num_keys - 1];
                left_sib.num_keys -= 1;
                leaf.insertAt(0, borrow_key, borrow_row);
                // Update parent key
                self.updateParentKey(leaf, borrow_key, leaf.keys[0]);
                return;
            }
        }

        // Try to borrow from right sibling
        if (leaf.next_leaf) |right_sib| {
            if (right_sib.parent == leaf.parent and right_sib.num_keys > MIN_KEYS) {
                const old_first = right_sib.keys[0];
                const borrow_row = right_sib.removeAt(0);
                leaf.insertAt(leaf.num_keys, old_first, borrow_row);
                // Update parent key for right sibling
                self.updateParentKey(right_sib, old_first, right_sib.keys[0]);
                return;
            }
        }

        // Merge with a sibling
        if (leaf.prev_leaf) |left_sib| {
            if (left_sib.parent == leaf.parent) {
                self.mergeLeaves(left_sib, leaf);
                return;
            }
        }
        if (leaf.next_leaf) |right_sib| {
            if (right_sib.parent == leaf.parent) {
                self.mergeLeaves(leaf, right_sib);
                return;
            }
        }
    }

    /// Merge right leaf into left leaf, then remove right from parent.
    fn mergeLeaves(self: *BTreeStorage, left: *BTreeNode, right: *BTreeNode) void {
        // Copy all entries from right to left
        for (0..right.num_keys) |i| {
            if (left.num_keys < MAX_KEYS) {
                left.keys[left.num_keys] = right.keys[i];
                left.rows[left.num_keys] = right.rows[i];
                left.num_keys += 1;
            }
        }

        // Update linked list
        left.next_leaf = right.next_leaf;
        if (right.next_leaf) |next| {
            next.prev_leaf = left;
        }

        // Remove right's key from parent
        if (right.parent) |parent| {
            // Find the key in parent that points to right
            const right_key = right.keys[0]; // First key was the separator
            _ = right_key;
            // Find position of right child in parent
            for (0..parent.num_keys + 1) |i| {
                if (parent.children[i] == right) {
                    // Remove key at i-1 and child at i
                    if (i > 0) {
                        parent.removeChildAt(i - 1);
                    } else {
                        parent.removeChildAt(0);
                    }
                    break;
                }
            }

            // Free the right node
            self.allocator.destroy(right);

            // Check if parent underflows
            if (parent == self.root) {
                if (parent.num_keys == 0) {
                    // Root has no keys — promote only child
                    if (parent.children[0]) |child| {
                        self.root = child;
                        child.parent = null;
                        self.allocator.destroy(parent);
                    }
                }
            } else if (parent.num_keys < MIN_KEYS) {
                self.handleInternalUnderflow(parent);
            }
        }
    }

    /// Handle underflow in an internal node.
    fn handleInternalUnderflow(self: *BTreeStorage, node: *BTreeNode) void {
        const parent = node.parent orelse return;

        // Find node's position in parent
        var node_pos: usize = 0;
        for (0..parent.num_keys + 1) |i| {
            if (parent.children[i] == node) {
                node_pos = i;
                break;
            }
        }

        // Try borrowing from left sibling
        if (node_pos > 0) {
            if (parent.children[node_pos - 1]) |left_sib| {
                if (left_sib.num_keys > MIN_KEYS) {
                    // Rotate right through parent
                    // Shift node's entries right
                    var i: usize = node.num_keys;
                    while (i > 0) : (i -= 1) {
                        node.keys[i] = node.keys[i - 1];
                        node.children[i + 1] = node.children[i];
                    }
                    node.children[1] = node.children[0];
                    // Pull parent key down
                    node.keys[0] = parent.keys[node_pos - 1];
                    // Pull left sibling's last child
                    node.children[0] = left_sib.children[left_sib.num_keys];
                    if (node.children[0]) |c| c.parent = node;
                    // Push left sibling's last key up to parent
                    parent.keys[node_pos - 1] = left_sib.keys[left_sib.num_keys - 1];
                    left_sib.children[left_sib.num_keys] = null;
                    left_sib.num_keys -= 1;
                    node.num_keys += 1;
                    return;
                }
            }
        }

        // Try borrowing from right sibling
        if (node_pos < parent.num_keys) {
            if (parent.children[node_pos + 1]) |right_sib| {
                if (right_sib.num_keys > MIN_KEYS) {
                    // Rotate left through parent
                    node.keys[node.num_keys] = parent.keys[node_pos];
                    node.children[node.num_keys + 1] = right_sib.children[0];
                    if (node.children[node.num_keys + 1]) |c| c.parent = node;
                    node.num_keys += 1;
                    parent.keys[node_pos] = right_sib.keys[0];
                    // Shift right sibling left
                    var i: usize = 0;
                    while (i + 1 < right_sib.num_keys) : (i += 1) {
                        right_sib.keys[i] = right_sib.keys[i + 1];
                        right_sib.children[i] = right_sib.children[i + 1];
                    }
                    right_sib.children[right_sib.num_keys - 1] = right_sib.children[right_sib.num_keys];
                    right_sib.children[right_sib.num_keys] = null;
                    right_sib.num_keys -= 1;
                    return;
                }
            }
        }

        // Merge with a sibling
        if (node_pos > 0) {
            if (parent.children[node_pos - 1]) |left_sib| {
                self.mergeInternal(left_sib, node, node_pos - 1);
                return;
            }
        }
        if (node_pos < parent.num_keys) {
            if (parent.children[node_pos + 1]) |right_sib| {
                self.mergeInternal(node, right_sib, node_pos);
                return;
            }
        }
    }

    /// Merge right internal node into left, pulling separator from parent.
    fn mergeInternal(self: *BTreeStorage, left: *BTreeNode, right: *BTreeNode, parent_key_pos: usize) void {
        const parent = left.parent orelse return;

        // Pull separator key from parent
        left.keys[left.num_keys] = parent.keys[parent_key_pos];
        left.num_keys += 1;

        // Copy right's entries into left
        for (0..right.num_keys) |i| {
            left.keys[left.num_keys] = right.keys[i];
            left.children[left.num_keys] = right.children[i];
            if (right.children[i]) |c| c.parent = left;
            left.num_keys += 1;
        }
        left.children[left.num_keys] = right.children[right.num_keys];
        if (right.children[right.num_keys]) |c| c.parent = left;

        // Remove separator and right child from parent
        parent.removeChildAt(parent_key_pos);
        self.allocator.destroy(right);

        if (parent == self.root and parent.num_keys == 0) {
            self.root = left;
            left.parent = null;
            self.allocator.destroy(parent);
        } else if (parent != self.root and parent.num_keys < MIN_KEYS) {
            self.handleInternalUnderflow(parent);
        }
    }

    /// Search for a row by rowid. Returns the Row if found.
    pub fn search(self: *BTreeStorage, key: i64) ?Row {
        const leaf = self.findLeaf(key);
        const pos = leaf.findKeyPos(key);
        if (pos < leaf.num_keys and leaf.keys[pos] == key) {
            return leaf.rows[pos];
        }
        return null;
    }

    /// Update a parent key when a leaf's first key changes.
    fn updateParentKey(self: *BTreeStorage, node: *BTreeNode, old_key: i64, new_key: i64) void {
        _ = self;
        var current = node.parent;
        while (current) |p| {
            for (0..p.num_keys) |i| {
                if (p.keys[i] == old_key) {
                    p.keys[i] = new_key;
                    return;
                }
            }
            current = p.parent;
        }
    }

    // --- Cache management ---

    fn materializeIfNeeded(self: *BTreeStorage) void {
        if (self.cache != null) return;
        if (self.count == 0) return;

        var arr = self.allocator.alloc(Row, self.count) catch return;
        var idx: usize = 0;

        // Find leftmost leaf
        var node = self.root;
        while (!node.is_leaf) {
            node = node.children[0] orelse break;
        }

        // Traverse leaf linked list
        while (true) {
            for (0..node.num_keys) |i| {
                if (idx < arr.len) {
                    arr[idx] = node.rows[i];
                    idx += 1;
                }
            }
            if (node.next_leaf) |next| {
                node = next;
            } else break;
        }

        self.cache = arr;
    }

    fn invalidateCache(self: *BTreeStorage) void {
        if (self.cache) |c| {
            self.allocator.free(c);
            self.cache = null;
        }
    }

    /// Sync modifications from the materialized cache back to leaf nodes.
    /// This handles ALTER TABLE ADD COLUMN which replaces row.values pointers.
    fn syncIfDirty(self: *BTreeStorage) void {
        if (!self.write_dirty) return;
        self.write_dirty = false;

        const cached = self.cache orelse return;
        if (cached.len != self.count) return;

        // Walk leaf nodes and update rows from cache
        var node = self.root;
        while (!node.is_leaf) {
            node = node.children[0] orelse return;
        }

        var idx: usize = 0;
        while (true) {
            for (0..node.num_keys) |i| {
                if (idx < cached.len) {
                    node.rows[i] = cached[idx];
                    idx += 1;
                }
            }
            if (node.next_leaf) |next| {
                node = next;
            } else break;
        }
    }

    /// Free all nodes in the tree recursively.
    fn freeAllNodes(self: *BTreeStorage, node: *BTreeNode) void {
        if (!node.is_leaf) {
            for (0..node.num_keys + 1) |i| {
                if (node.children[i]) |child| {
                    self.freeAllNodes(child);
                }
            }
        }
        self.allocator.destroy(node);
    }

    /// Get the first (leftmost) leaf node.
    fn firstLeaf(self: *BTreeStorage) *BTreeNode {
        var node = self.root;
        while (!node.is_leaf) {
            node = node.children[0] orelse break;
        }
        return node;
    }

    /// Range search: find all rows with min_rowid <= rowid <= max_rowid.
    pub fn rangeSearch(self: *BTreeStorage, min_key: i64, max_key: i64) ?[]const Row {
        if (self.count == 0) return null;

        // Find the leaf containing min_key
        var leaf = self.findLeaf(min_key);
        var results: std.ArrayList(Row) = .{};

        outer: while (true) {
            for (0..leaf.num_keys) |i| {
                if (leaf.keys[i] > max_key) break :outer;
                if (leaf.keys[i] >= min_key) {
                    results.append(self.allocator, leaf.rows[i]) catch return null;
                }
            }
            if (leaf.next_leaf) |next| {
                leaf = next;
            } else break;
        }

        if (results.items.len == 0) {
            results.deinit(self.allocator);
            return null;
        }
        return results.toOwnedSlice(self.allocator) catch null;
    }
};

// --- Unit Tests ---

test "BTreeStorage: init and basic operations" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer bts.storage().deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), bts.storage().len());
}

test "BTreeStorage: insert and scan" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer {
        // Free row values before deinit
        const rows = bts.storage().scan();
        for (rows) |row| {
            allocator.free(row.values);
        }
        bts.storage().deinit(allocator);
    }

    // Insert 3 rows
    for (1..4) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i * 10) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    try std.testing.expectEqual(@as(usize, 3), bts.storage().len());

    const rows = bts.storage().scan();
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    // Rows should be in rowid order
    try std.testing.expectEqual(@as(i64, 10), rows[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 20), rows[1].values[0].integer);
    try std.testing.expectEqual(@as(i64, 30), rows[2].values[0].integer);
}

test "BTreeStorage: orderedRemove" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer {
        const rows = bts.storage().scan();
        for (rows) |row| {
            allocator.free(row.values);
        }
        bts.storage().deinit(allocator);
    }

    // Insert 3 rows
    for (1..4) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    // Remove middle row (index 1, rowid 2)
    const removed = bts.storage().orderedRemove(1);
    allocator.free(removed.values);

    try std.testing.expectEqual(@as(usize, 2), bts.storage().len());
    const rows = bts.storage().scan();
    try std.testing.expectEqual(@as(i64, 1), rows[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 3), rows[1].values[0].integer);
}

test "BTreeStorage: search by rowid" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer {
        const rows = bts.storage().scan();
        for (rows) |row| {
            allocator.free(row.values);
        }
        bts.storage().deinit(allocator);
    }

    for (1..6) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i * 100) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    // Search for existing key
    const found = bts.search(3);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(i64, 300), found.?.values[0].integer);

    // Search for non-existing key
    const not_found = bts.search(99);
    try std.testing.expect(not_found == null);
}

test "BTreeStorage: many inserts trigger splits" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer {
        const rows = bts.storage().scan();
        for (rows) |row| {
            allocator.free(row.values);
        }
        bts.storage().deinit(allocator);
    }

    // Insert enough rows to trigger at least one split (MAX_KEYS = 7)
    const N = 20;
    for (1..N + 1) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    try std.testing.expectEqual(@as(usize, N), bts.storage().len());

    // Verify all rows are in order
    const rows = bts.storage().scan();
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.values[0].integer);
        try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.rowid);
    }

    // Verify search works for all
    for (1..N + 1) |i| {
        const found = bts.search(@intCast(i));
        try std.testing.expect(found != null);
        try std.testing.expectEqual(@as(i64, @intCast(i)), found.?.values[0].integer);
    }
}

test "BTreeStorage: delete causes merge" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer {
        const rows = bts.storage().scan();
        for (rows) |row| {
            allocator.free(row.values);
        }
        bts.storage().deinit(allocator);
    }

    // Insert enough rows
    const N = 15;
    for (1..N + 1) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    // Delete several rows to trigger merges
    for (0..8) |_| {
        const removed = bts.storage().orderedRemove(0);
        allocator.free(removed.values);
    }

    try std.testing.expectEqual(@as(usize, N - 8), bts.storage().len());

    // Verify remaining rows are correct
    const rows = bts.storage().scan();
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i + 9)), row.values[0].integer);
    }
}

test "BTreeStorage: clearRetainingCapacity" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer bts.storage().deinit(allocator);

    for (1..4) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    // Free row values before clear (simulating table.freeRow behavior)
    const rows = bts.storage().scan();
    for (rows) |row| {
        allocator.free(row.values);
    }

    bts.storage().clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), bts.storage().len());
}

test "BTreeStorage: scanMut allows mutation" {
    const allocator = std.testing.allocator;
    var bts = BTreeStorage.init(allocator);
    defer {
        const rows = bts.storage().scan();
        for (rows) |row| {
            allocator.free(row.values);
        }
        bts.storage().deinit(allocator);
    }

    for (1..4) |i| {
        var values = try allocator.alloc(value_mod.Value, 1);
        values[0] = .{ .integer = @intCast(i) };
        try bts.storage().append(allocator, .{ .rowid = @intCast(i), .values = values });
    }

    // Mutate via scanMut (like UPDATE)
    for (bts.storage().scanMut()) |*row| {
        row.values[0] = .{ .integer = row.values[0].integer * 10 };
    }

    // Verify mutations persisted (after sync)
    const rows = bts.storage().scan();
    try std.testing.expectEqual(@as(i64, 10), rows[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 20), rows[1].values[0].integer);
    try std.testing.expectEqual(@as(i64, 30), rows[2].values[0].integer);
}
