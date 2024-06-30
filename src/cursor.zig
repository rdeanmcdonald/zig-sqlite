const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const c = @import("constants.zig");
const e = @import("errors.zig");
const Table = @import("table.zig");
const Row = @import("row.zig");
const assert = @import("utils.zig").assert;

const Self = @This();

table: *Table,
rowNum: u64,
endOfTable: bool,

pub fn initAtStart(table: *Table) Self {
    return Self{
        .table = table,
        .rowNum = 0,
        // is end if the table is empty
        .endOfTable = table.numRows == 0,
    };
}

pub fn initAtEnd(table: *Table) Self {
    return Self{
        .table = table,
        .rowNum = table.numRows,
        .endOfTable = true,
    };
}

pub fn advance(self: *Self) void {
    self.*.rowNum += 1;
    if (self.rowNum >= self.table.numRows) {
        self.*.endOfTable = true;
    }
}

/// Gets the slice to the row which the cursor is on
pub fn getRowSlot(self: *Self) ![]u8 {
    const rowIdxOnPage, const pageIdx = getIdxsFromRowIdx(self.rowNum);
    const page = try self.table.pager.getOrCreatePage(pageIdx);
    const rowOffset = rowIdxOnPage * c.ROW_SIZE;
    return page.data[rowOffset .. rowOffset + c.ROW_SIZE];
}

/// Read the row pointed to by the cursor
pub fn readRow(self: *Self, row: *Row) !void {
    const rowSlot = try self.getRowSlot();
    try row.deserializeFromSlice(rowSlot);
}

fn getIdxsFromRowIdx(rowIdx: u64) [2]u64 {
    const rowIdxInPage = @mod(rowIdx, c.TAB_ROWS_PER_PAGE);
    const pageIdx = @divFloor(rowIdx, c.TAB_ROWS_PER_PAGE);
    return .{ rowIdxInPage, pageIdx };
}
