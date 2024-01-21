const std = @import("std");
const os = std.os;
const mem = std.mem;
const Allocator = mem.Allocator;
const stdin = std.io.getStdIn().reader();
const stdout = std.io.getStdOut().writer();

const InputBuf = struct {
    buf: []u8,
    inlen: usize,
    allocator: *Allocator,

    const Self = @This();

    pub fn init(allocator: *Allocator) !Self {
        const buflen = 4096;
        const buf = try allocator.alloc(u8, buflen);
        return Self {
            .buf = buf,
            .inlen = 0,
            .allocator = allocator,
        };
    }

    pub fn read(self: *Self) !void {
        if (try stdin.readUntilDelimiterOrEof(self.buf, '\n')) |in| {
            self.inlen = in.len;
        } else {
            std.debug.print("NO INPUT", .{});
        }
    }

    pub fn input(self: *Self) []u8 {
        return self.buf[0..self.inlen];
    }
};

fn print_prompt() void {
    std.debug.print("db > ", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();
    var inputbuf = try InputBuf.init(&allocator);
    while (true) {
        print_prompt();
        try inputbuf.read();
        if (std.mem.eql(u8, inputbuf.input(), ".exit")) {
            std.debug.print("Exiting...\n", .{});
            std.os.exit(0);
        } else {
            std.debug.print("Unrecognized command: {s}\n", .{inputbuf.input()});
        }
    }
}
