const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const c = @import("constants.zig");
const e = @import("errors.zig");
const Pager = @import("pager.zig");
const Page = Pager.Page;
const Row = @import("row.zig");
const assert = @import("utils.zig").assert;

const Self = @This();

pager: *Pager,
numRows: u64,

pub fn init(pager: *Pager) !Self {
    const fullPages = @divFloor(pager.fileLength, c.TAB_PAGE_SIZE);
    var numRows = fullPages * c.TAB_ROWS_PER_PAGE;
    if (@mod(pager.fileLength, c.TAB_PAGE_SIZE) > 0) {
        // The last page is partial, get the number of rows on it
        const bytesOnLastPage = pager.fileLength - (fullPages * c.TAB_PAGE_SIZE);
        try assert(@mod(bytesOnLastPage, c.ROW_SIZE) == 0, e.DbError.CorruptedData);
        numRows += @divFloor(bytesOnLastPage, c.ROW_SIZE);
    }
    return Self{
        .pager = pager,
        .numRows = numRows,
    };
}

pub fn insertRow(self: *Self, row: *Row) !void {
    if (self.numRows >= c.TAB_MAX_ROWS) {
        return error.TableFull;
    }

    const rowIdxOnPage, const pageIdx = self.getIdxsFromRowIdx(self.numRows);
    const page = try self.pager.getOrCreatePage(pageIdx);
    try self.writeRowData(rowIdxOnPage, row, page);
}

fn getIdxsFromRowIdx(_: *Self, rowIdx: u64) [2]u64 {
    const rowIdxInPage = @mod(rowIdx, c.TAB_ROWS_PER_PAGE);
    const pageIdx = @divFloor(rowIdx, c.TAB_ROWS_PER_PAGE);
    return .{ rowIdxInPage, pageIdx };
}

fn getRowSliceFromIdx(_: *Self, idx: u64, page: *Page) []u8 {
    const rowOffset = idx * c.ROW_SIZE;
    return page.data[rowOffset .. rowOffset + c.ROW_SIZE];
}

fn writeRowData(self: *Self, rowIdxOnPage: u64, row: *Row, page: *Page) !void {
    const rowSlice = self.getRowSliceFromIdx(rowIdxOnPage, page);

    try row.serializeToSlice(rowSlice);
    self.numRows += 1;
}

pub fn readRow(self: *Self, idx: u64, row: *Row) !bool {
    const rowIdxOnPage, const pageIdx = self.getIdxsFromRowIdx(idx);
    if (try self.pager.getPage(pageIdx)) |page| {
        const rowSlice = self.getRowSliceFromIdx(rowIdxOnPage, page);
        try row.deserializeFromSlice(rowSlice);
        return true;
    } else {
        return false;
    }
}

/// Write every page to the file, except the last page which might not be
/// full
pub fn close(self: *Self) !void {
    if (self.numRows == 0) {
        return;
    }
    var offset: usize = 0;
    var pageIdx: usize = 0;
    const lastPageIdx = @divFloor(self.numRows - 1, c.TAB_ROWS_PER_PAGE);
    const lastPageIsPartial = @mod(self.numRows, c.TAB_ROWS_PER_PAGE) != 0;
    var bytesWritten: usize = 0;
    while (self.pager.pages[pageIdx]) |page| : ({
        offset += page.data.len;
        pageIdx += 1;
    }) {
        if (pageIdx == lastPageIdx and lastPageIsPartial) {
            // last page is partial page, will break out of while loop next
            const rowIdxOnPage, _ = self.getIdxsFromRowIdx(self.numRows);
            const lastRowLastByte = rowIdxOnPage * c.ROW_SIZE;
            try self.pager.file.pwriteAll(page.data[0..lastRowLastByte], offset);
            bytesWritten += lastRowLastByte;
        } else {
            try self.pager.file.pwriteAll(page.data[0..], offset);
            bytesWritten += page.data.len;
        }
    }
}
