const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const c = @import("constants.zig");
const e = @import("errors.zig");
const assert = @import("utils.zig").assert;

pub const Page = struct {
    data: [c.TAB_PAGE_SIZE]u8,
};

const Self = @This();

pages: [c.TAB_MAX_PAGES]?*Page,
allocator: *Allocator,
file: std.fs.File,
fileLength: u64,

pub fn init(allocator: *Allocator, filename: []const u8) !Self {
    const file = std.fs.File{ .handle = try std.posix.open(filename, std.posix.O{ .ACCMODE = .RDWR, .CREAT = true }, 0o666) };
    const fileLength = try file.getEndPos();
    var pager = Self{
        .pages = undefined,
        .allocator = allocator,
        .file = file,
        .fileLength = fileLength,
    };

    if (fileLength == 0) {
        return pager;
    }

    const lastPageIdx = @divFloor(fileLength - 1, c.TAB_PAGE_SIZE);
    var pageIdx: usize = 0;
    var offset: usize = 0;
    while (pageIdx <= lastPageIdx) : ({
        pageIdx += 1;
        offset += c.TAB_PAGE_SIZE;
    }) {
        var page = try pager.createPage(pageIdx);
        // pread all will only read data.len or less if there's no more
        // data, so even the last page with potentially partial data is
        // safe to do this.
        _ = try file.preadAll(page.data[0..], offset);
    }

    return pager;
}

pub fn deinit(self: Self) void {
    for (self.pages) |maybePage| {
        if (maybePage) |page| {
            self.allocator.destroy(page);
        }
    }
}

pub fn createPage(self: *Self, idx: u64) !*Page {
    const page = try self.allocator.create(Page);
    self.pages[idx] = page;
    return page;
}

pub fn getOrCreatePage(self: *Self, idx: u64) !*Page {
    if (self.pages[idx]) |page| {
        return page;
    } else {
        return self.createPage(idx);
    }
}

pub fn getPage(self: *Self, idx: u64) !?*Page {
    try assert(idx < c.TAB_MAX_PAGES, e.DbError.General);
    return self.pages[idx];
}
