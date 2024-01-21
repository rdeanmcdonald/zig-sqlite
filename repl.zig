const std = @import("std");
const os = std.os;
const mem = std.mem;
const Allocator = mem.Allocator;
const stdin = std.io.getStdIn().reader();
const stdout = std.io.getStdOut().writer();

const someerr = error{Err};
const MetaCmdRes = enum { META_CMD_SUCCESS, META_CMD_UNRECOGNIZED };
const StatementType = enum { STATEMENT_INSERT, STATEMENT_SELECT };
const Statement = struct {
    type: StatementType,

    const Self = @This();

    pub fn init(inbuf: *InputBuf) !Self {
        if (inbuf.startsWith("insert")) {
            return Self{
                .type = StatementType.STATEMENT_INSERT,
            };
        }

        if (inbuf.startsWith("select")) {
            return Self{
                .type = StatementType.STATEMENT_SELECT,
            };
        }

        return someerr.Err;
    }

    pub fn exec(self: *Self) !void {
        switch (self.type) {
            StatementType.STATEMENT_INSERT => {
                std.debug.print("Executing insert...\n", .{});
            },
            StatementType.STATEMENT_SELECT => {
                std.debug.print("Executing select...\n", .{});
            },
        }
    }
};

fn doMetaCmd(inbuf: *InputBuf) !void {
    if (inbuf.startsWith(".exit")) {
        std.os.exit(0);
    } else {
        return someerr.Err;
    }
}

const InputBuf = struct {
    buf: []u8,
    inlen: usize,
    allocator: *Allocator,

    const Self = @This();

    pub fn init(allocator: *Allocator) !Self {
        const buflen = 4096;
        const buf = try allocator.alloc(u8, buflen);
        return Self{
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
    pub fn getInput(self: *Self) []u8 {
        return self.buf[0..self.inlen];
    }

    pub fn startsWith(self: *Self, tst: []const u8) bool {
        if (std.mem.startsWith(u8, self.getInput(), tst)) {
            return true;
        } else {
            return false;
        }
    }
};

fn printPrompt() void {
    std.debug.print("db > ", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();
    var inbuf = try InputBuf.init(&allocator);
    while (true) {
        printPrompt();
        try inbuf.read();
        if (inbuf.startsWith(".")) {
            if (doMetaCmd(&inbuf)) |_| {} else |_| {
                std.debug.print("Unrecognized meta command: {s}\n", .{inbuf.getInput()});
            }
            continue;
        }
        if (Statement.init(&inbuf)) |s| {
            var statement = s;
            try statement.exec();
        } else |_| {
            std.debug.print("Unrecognized statement: {s}\n", .{inbuf.getInput()});
        }
    }
}
