const std = @import("std");
const mem = std.mem;
const c = @import("constants.zig");

// A Node is just an array of bytes, this file provides the API to access the
// data organized in the slice. This slice backs each Page in the Pager.
pub const Node = [c.TAB_PAGE_SIZE]u8;

const NodeType = enum { Leaf, Internal };

// COMMON NODE HEADER LAYOUT
const NODE_TYPE_SIZE = @sizeOf(NodeType);
const NODE_TYPE_OFFSET = 0;
const IS_ROOT_SIZE = @sizeOf(u8);
const IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE = @sizeOf(*anyopaque);
const PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// LEAF NODE HEADER LAYOUT
const LEAF_NODE_NUM_CELLS_SIZE = @sizeOf(u64);
const LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// LEAF NODE BODY LAYOUT
const LEAF_NODE_KEY_SIZE = @sizeOf(u32);
const LEAF_NODE_KEY_OFFSET = 0;
const LEAF_NODE_VALUE_SIZE = c.ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS = c.TAB_PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS = @divFloor(LEAF_NODE_SPACE_FOR_CELLS, LEAF_NODE_CELL_SIZE);

// LEAF NODE ACCESSES
// These are literally going to be raw ptr reads / writes, not serializing /
// deserializing anything. Lots of alignment / casting will be required. Not
// sure why the article does it this way, rather than creating a real struct.
// Minimizes copies i guess? I can see that persisting a node is now super
// easy, it's just writing 4096 buffer to disk, but man this sucks to work
// with. Not exactly sure why they do it like this. If 4kb of contiguous memory
// per node is important, perhaps a better impl would be packed structs for
// each property? Though I tried using a packed struct for a row, which is 293
// bytes, and it wasn't allowed. Maybe packing rows with different alignment
// can help.
pub fn leafNodeNumCells(node: *Node) *u64 {
    std.debug.print("CELLS {*}\n", .{node});
    const nodeData = node.*;

    std.debug.print("CELLS {d}\n", .{LEAF_NODE_NUM_CELLS_OFFSET});
    std.debug.print("CELLS {d}\n", .{LEAF_NODE_NUM_CELLS_OFFSET + @sizeOf(u64)});

    var bytes align(@alignOf(u64)) = nodeData[LEAF_NODE_NUM_CELLS_OFFSET .. LEAF_NODE_NUM_CELLS_OFFSET + @sizeOf(u64)];
    const bytesPtr = &bytes;

    std.debug.print("CELLS {d}\n", .{bytes});
    std.debug.print("CELLS {d}\n", .{bytesPtr});
    std.debug.print("CELLS {d}\n", .{bytesPtr.*});
    const u64_ptr: *u64 = @ptrCast(&bytes);
    
    std.debug.print("CELLS {*}\n", .{u64_ptr});
    return u64_ptr;
}

pub fn leafNodeCell(node: *Node, cellNum: u64) [*]u8 {
    const leaf_node_ptr: [*]u8 = node;
    const leaf_node_cell_ptr = leaf_node_ptr[LEAF_NODE_HEADER_SIZE + cellNum * LEAF_NODE_CELL_SIZE..];
    return leaf_node_cell_ptr;
}

pub fn leafNodeKey(node: *Node, cellNum: u64) *u32 {
    const leaf_node_ptr = leafNodeCell(node, cellNum);

    // the key is the first part of the cell
    var bytes align(@alignOf(u32)) = leaf_node_ptr[0 .. @sizeOf(u32)];
    const u32_ptr: *u32 = @ptrCast(&bytes);
    return u32_ptr;
}

pub fn leafNodeValue(node: *Node, cellNum: u64) [*]u8 {
    const leaf_node_ptr: [*]u8 = leafNodeCell(node, cellNum);
    return leaf_node_ptr + LEAF_NODE_KEY_SIZE;
}

pub fn initializeLeafeNode(node: *Node) void {
    std.debug.print("HI {*}\n", .{node});
    const numCellsPtr = leafNodeNumCells(node);
    std.debug.print("HI {d}\n", .{numCellsPtr});
    std.debug.print("HI {d}\n", .{numCellsPtr.*});
    numCellsPtr.* = 0;
    std.debug.print("HI {d}\n", .{numCellsPtr.*});
}

const OFFSET = 13;
fn getU64Ptr(data: *[1024]u8) *u64 {
    var bytes align(@alignOf(u64)) = data[OFFSET .. OFFSET + @sizeOf(u64)];
    const u64_ptr: *u64 = @ptrCast(&bytes);

    return u64_ptr;
}
fn initU64Value(data: *[1024]u8) void {
    const ptr = getU64Ptr(data);
    std.debug.print("INTERNAL PTR {d}\n", .{ptr});
    ptr.* = 0;
    std.debug.print("INTERNAL VAL {d}\n", .{ptr.*});
}

fn hi() !i32 {
    var a: i32 = 1;
    const a_ptr = &a;
    var prng = std.Random.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const num = prng.random().int(i32);

    std.debug.print("NUM: {d}\n", .{num});
    if (num > 0) {
        a_ptr.* = 3;
    }
    std.debug.print("A: {d}\n", .{a});
    return a;
}

test "example" {
    _ = try hi();
}
