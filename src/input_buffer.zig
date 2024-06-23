const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");

buf: []u8,
inlen: usize,
allocator: *Allocator,
cli: *Cli,

const Self = @This();

pub fn init(allocator: *Allocator, cli: *Cli) !Self {
    const buflen = 4096;
    const buf = try allocator.alloc(u8, buflen);
    return Self{
        .buf = buf,
        .inlen = 0,
        .allocator = allocator,
        .cli = cli,
    };
}

pub fn deinit(self: Self) void {
    self.allocator.free(self.buf);
}

pub fn read(self: *Self) !void {
    var fbs = std.io.fixedBufferStream(self.buf);
    try self.cli.reader.reader().streamUntilDelimiter(fbs.writer(), '\n', null);
    self.inlen = fbs.getWritten().len;
}

pub fn getInput(self: *Self) []u8 {
    return self.buf[0..self.inlen];
}
