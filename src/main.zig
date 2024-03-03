const std = @import("std");
const os = std.os;
const mem = std.mem;
const Allocator = mem.Allocator;
const stdin = std.io.getStdIn().reader();
const stdout = std.io.getStdOut().writer();
const assert = std.debug.assert;

const DbError = error{ General, InvalidInput, TableFull };
const MetaCmdRes = enum { META_CMD_SUCCESS, META_CMD_UNRECOGNIZED };
const StatementType = enum { STATEMENT_INSERT, STATEMENT_SELECT };

const TAB_PAGE_SIZE = 4096;
const TAB_MAX_PAGES = 100;
const TAB_ROWS_PER_PAGE = TAB_PAGE_SIZE / ROW_SIZE;
const TAB_MAX_ROWS = TAB_ROWS_PER_PAGE * TAB_MAX_PAGES;

const Table = struct {
    const Self = @This();
    const Page = struct {
        data: [TAB_PAGE_SIZE]u8,
    };

    nextAvailableRowIdx: u32,
    pages: [TAB_MAX_PAGES]?*Page,
    allocator: *Allocator,

    pub fn init(allocator: *Allocator) Self {
        return Self{
            .nextAvailableRowIdx = 0,
            .pages = undefined,
            .allocator = allocator,
        };
    }

    pub fn insertRow(self: *Self, row: *Row) !void {
        if (self.nextAvailableRowIdx >= TAB_MAX_ROWS) {
            return error.TableFull;
        }

        const pageIdx = @divFloor(self.nextAvailableRowIdx, TAB_ROWS_PER_PAGE);
        if (self.pages[pageIdx] == null) {
            self.pages[pageIdx] = try self.allocator.create(Page);
        }

        var pageData = self.pages[pageIdx].?.data;
        const rowSlice = pageData[self.nextAvailableRowIdx .. self.nextAvailableRowIdx + ROW_SIZE];

        row.serializeToSlice(rowSlice);
        self.nextAvailableRowIdx += 1;

        std.debug.print("PAGE DATA {any}\n", .{pageData});
    }
};

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
        defer allocator.free(idBytes);

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
        mem.writePackedIntNative(@TypeOf(self.id), dest[ID_OFF .. ID_OFF + ID_SIZE], 0, self.id);
        mem.copyForwards(u8, dest[USERNAME_OFF .. USERNAME_OFF + USERNAME_SIZE], self.username[0..]);
        mem.copyForwards(u8, dest[EMAIL_OFF .. EMAIL_OFF + EMAIL_SIZE], self.email[0..]);
    }

    /// Initialize a Row from a previously serialized Row
    pub fn initFromSerializedSlice(src: []u8, allocator: *Allocator) !*Self {
        assert(src.len >= ROW_SIZE);
        const idSlice = src[ID_OFF .. ID_OFF + ID_SIZE];
        const usernameSlice = src[USERNAME_OFF .. USERNAME_OFF + USERNAME_SIZE];
        const emailSlice = src[EMAIL_OFF .. EMAIL_OFF + EMAIL_SIZE];

        return Row.initFromBytes(idSlice, usernameSlice, emailSlice, allocator);
    }
};

const Insert = struct {
    const Self = @This();

    row: *Row,

    pub fn exec(self: Self, table: *Table) !void {
        std.debug.print("INSERTING {d}, {s}, {s}\n", .{ self.row.id, self.row.username, self.row.email });
        try table.insertRow(self.row);
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

    pub fn exec(self: Self, table: *Table) !void {
        _ = self;
        _ = table;
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

        const cmd = tokens.next() orelse return DbError.InvalidInput;
        if (mem.eql(u8, cmd, "insert")) {
            const id = tokens.next() orelse return DbError.InvalidInput;
            const username = tokens.next() orelse return DbError.InvalidInput;
            const email = tokens.next() orelse return DbError.InvalidInput;
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

        return DbError.General;
    }

    pub fn exec(self: Self, table: *Table) !void {
        switch (self) {
            inline else => |case| try case.exec(table),
        }
    }
};

fn doMetaCmd(inbuf: *InputBuf) !void {
    if (inbuf.startsWith(".exit")) {
        std.os.exit(0);
    } else {
        return DbError.General;
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
    var table = Table.init(&allocator);
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
            try statement.exec(&table);
        } else |err| {
            std.debug.print("ERROR ENCOUNTERED: {any} \nFOR INPUT {s}\n", .{ err, inbuf.getInput() });
        }
    }
}

test "simple test" {
    const idx: u32 = 14;
    const pages: [TAB_MAX_PAGES]?usize = undefined;
    const pageIdx = @divFloor(idx, TAB_ROWS_PER_PAGE);

    std.debug.print("TEST {any}\n", .{pages[pageIdx]});
}
