const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const c = @import("constants.zig");
const e = @import("errors.zig");
const Pager = @import("pager.zig");
const Page = Pager.Page;
const Row = @import("row.zig");
const Cursor = @import("cursor.zig");
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

pub fn insertRow(self: *Self, row: *Row, cursor: *Cursor) !void {
    if (self.numRows >= c.TAB_MAX_ROWS) {
        return error.TableFull;
    }

    const rowSlot = try cursor.getRowSlot();
    try row.serializeToSlice(rowSlot);
    self.numRows += 1;
}

/// Write every page to the file. The last page might not be full, so write
/// only the rows that exist there.
pub fn close(self: *Self) !void {
    if (self.numRows == 0) {
        return;
    }
    var offset: usize = 0;
    var pageIdx: usize = 0;
    const lastPageIdx = @divFloor(self.numRows - 1, c.TAB_ROWS_PER_PAGE);
    const numAdditionalRows = @mod(self.numRows, c.TAB_ROWS_PER_PAGE);
    var bytesWritten: usize = 0;
    while (self.pager.pages[pageIdx]) |page| : ({
        offset += page.data.len;
        pageIdx += 1;
    }) {
        if (pageIdx == lastPageIdx and numAdditionalRows > 0) {
            // last page is partial page, will break out of while loop next
            const rowBytes = numAdditionalRows * c.ROW_SIZE;
            try self.pager.file.pwriteAll(page.data[0..rowBytes], offset);
            bytesWritten += rowBytes;
        } else {
            try self.pager.file.pwriteAll(page.data[0..], offset);
            bytesWritten += page.data.len;
        }
    }
}
