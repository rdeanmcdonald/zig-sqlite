const std = @import("std");
const os = std.os;
const mem = std.mem;
const Allocator = mem.Allocator;
const stdin = std.io.getStdIn().reader();
const stdout = std.io.getStdOut().writer();
const assert = std.debug.assert;

const StatementError = error{ General, InvalidInput };
const MetaCmdRes = enum { META_CMD_SUCCESS, META_CMD_UNRECOGNIZED };
const StatementType = enum { STATEMENT_INSERT, STATEMENT_SELECT };

const ID_SIZE = @sizeOf(u32);
const USERNAME_SIZE = 32;
const EMAIL_SIZE = 255;
const ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const ID_OFF = 0;
const USERNAME_OFF = ID_OFF + ID_SIZE;
const EMAIL_OFF = USERNAME_OFF + USERNAME_SIZE;

const Row = struct {
    const Self = @This();

    id: u32,
    username: [USERNAME_SIZE]u8,
    email: [EMAIL_SIZE]u8,

    pub fn initFromString(idStr: []const u8, usernameStr: []const u8, emailStr: []const u8, allocator: *Allocator) !*Self {
        const id = try std.fmt.parseInt(u32, idStr, 10); 

        const idBytes = try allocator.alloc(u8, ID_SIZE);
        mem.writePackedIntNative(u32, idBytes, 0, id);

        return try initFromBytes(idBytes, usernameStr, emailStr, allocator);
    }

    pub fn initFromBytes(idBytes: []const u8, usernameBytes: []const u8, emailBytes: []const u8, allocator: *Allocator) !*Self {
        assert(idBytes.len == ID_SIZE);
        assert(usernameBytes.len <= USERNAME_SIZE);
        assert(emailBytes.len <= EMAIL_SIZE);

        var row = try allocator.create(Row);

        row.id = mem.readPackedIntNative(u32, idBytes, 0);
        mem.copyForwards(u8, row.username[0..], usernameBytes);
        mem.copyForwards(u8, row.email[0..], emailBytes);

        return row;
    }

    /// Write Row bytes to dest slice
    pub fn serializeToSlice(self: Self, dest: []u8) void {
        assert(dest.len >= ROW_SIZE);
        mem.writePackedIntNative(@TypeOf(self.id), dest[ID_OFF..ID_OFF + ID_SIZE], 0, self.id);
        mem.copyForwards(u8, dest[USERNAME_OFF..USERNAME_OFF + USERNAME_SIZE], self.username[0..]);
        mem.copyForwards(u8, dest[EMAIL_OFF..EMAIL_OFF + EMAIL_SIZE], self.email[0..]);
    }

    /// Initialize a Row from a previously serialized Row
    pub fn initFromSerializedSlice(src: []u8, allocator: *Allocator) !*Self {
        assert(src.len >= ROW_SIZE);
        const idSlice = src[ID_OFF..ID_OFF + ID_SIZE];
        const usernameSlice = src[USERNAME_OFF..USERNAME_OFF + USERNAME_SIZE];
        const emailSlice = src[EMAIL_OFF..EMAIL_OFF + EMAIL_SIZE];

        return Row.initFromBytes(idSlice, usernameSlice, emailSlice, allocator);
    }
};

const Insert = struct {
    const Self = @This();

    row: *Row,

    pub fn exec(self: Self) !void {
        std.debug.print("INSERTING {d}, {s}, {s}\n", .{ self.row.id, self.row.username, self.row.email });
    }

    pub fn init(idBytes: []const u8, usernameBytes: []const u8, emailBytes: []const u8, allocator: *Allocator) !Self {
        const row = try Row.initFromString(idBytes, usernameBytes, emailBytes, allocator);
        // var dest: [ROW_SIZE]u8 = undefined;
        // row.serializeToSlice(dest[0..]);
        // const newRow = try Row.initFromSerializedSlice(dest[0..], allocator);
        // std.debug.print("newRow {any}\n", .{newRow});

        return Self{ .row = row };
    }
};

const Select = struct {
    const Self = @This();

    pub fn exec(self: Self) !void {
        _ = self;
        std.debug.print("SELECTING\n", .{});
    }

    pub fn init() Self {
        return Self{};
    }
};

// Basic "interface" using tagged enum approach
// See: https://zig.news/kristoff/easy-interfaces-with-zig-0100-2hc5
const Statement = union(enum) {
    const Self = @This();

    insert: Insert,
    select: Select,

    pub fn init(inbuf: *InputBuf, allocator: *Allocator) !Self {
        var tokens = mem.splitScalar(u8, inbuf.getInput(), ' ');

        const cmd = tokens.next() orelse return StatementError.InvalidInput;
        if (mem.eql(u8, cmd, "insert")) {
            const id = tokens.next() orelse return StatementError.InvalidInput;
            const username = tokens.next() orelse return StatementError.InvalidInput;
            const email = tokens.next() orelse return StatementError.InvalidInput;
            const impl = try Insert.init(id, username, email, allocator);
            return Self{
                .insert = impl,
            };
        }

        if (mem.eql(u8, cmd, "select")) {
            const impl = Select.init();
            return Self{
                .select = impl,
            };
        }

        return StatementError.General;
    }

    pub fn exec(self: Self) !void {
        switch (self) {
            inline else => |case| try case.exec(),
        }
    }
};

fn doMetaCmd(inbuf: *InputBuf) !void {
    if (inbuf.startsWith(".exit")) {
        std.os.exit(0);
    } else {
        return StatementError.General;
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
            std.debug.print("INPUT ERROR", .{});
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
        if (Statement.init(&inbuf, &allocator)) |s| {
            var statement = s;
            try statement.exec();
        } else |err| {
            std.debug.print("ERROR ENCOUNTERED: {any} \nFOR INPUT {s}\n", .{err, inbuf.getInput()});
        }
    }
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
