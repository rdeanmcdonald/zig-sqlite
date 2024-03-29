const std = @import("std");
const os = std.os;
const mem = std.mem;
const Allocator = mem.Allocator;
const stdin = std.io.getStdIn().reader();
const stdout = std.io.getStdOut().writer();

const DbError = error{ General, InvalidInput, TableFull };
const MetaCmdRes = enum { META_CMD_SUCCESS, META_CMD_UNRECOGNIZED };
const StatementType = enum { STATEMENT_INSERT, STATEMENT_SELECT };

fn assert(ok: bool, err: DbError) !void {
    if (!ok) {
        return err;
    }
}

const TAB_PAGE_SIZE = 4096;
const TAB_MAX_PAGES = 100;
const TAB_ROWS_PER_PAGE = TAB_PAGE_SIZE / ROW_SIZE;
const TAB_MAX_ROWS = TAB_ROWS_PER_PAGE * TAB_MAX_PAGES;
const ID_SIZE = @sizeOf(u32);
const USERNAME_SIZE = 32;
const EMAIL_SIZE = 255;
const ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ID_OFF = 0;
const USERNAME_OFF = ID_OFF + ID_SIZE;
const EMAIL_OFF = USERNAME_OFF + USERNAME_SIZE;

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

        const rowIdxOnPage, const pageIdx = self.getIdxsFromRowIdx(self.nextAvailableRowIdx);
        if (self.pages[pageIdx]) |page| {
            try self.writeRowData(rowIdxOnPage, row, page);
        } else {
            const page = try self.allocator.create(Page);
            self.pages[pageIdx] = page;
            try self.writeRowData(rowIdxOnPage, row, page);
        }
    }

    /// .{rowIdxInPage, pageIdx}
    fn getIdxsFromRowIdx(_: *Self, rowIdx: u32) [2]u32 {
        const mod = @mod(rowIdx, TAB_ROWS_PER_PAGE);
        return .{ mod, @divFloor(rowIdx, TAB_ROWS_PER_PAGE) };
    }

    fn getRowSliceFromIdx(_: *Self, idx: u32, page: *Page) []u8 {
        const rowOffset = idx * ROW_SIZE;
        return page.data[rowOffset .. rowOffset + ROW_SIZE];
    }

    fn writeRowData(self: *Self, rowIdxOnPage: u32, row: *Row, page: *Page) !void {
        const rowSlice = self.getRowSliceFromIdx(rowIdxOnPage, page);

        try row.serializeToSlice(rowSlice);
        self.nextAvailableRowIdx += 1;
        // std.debug.print("PAGE DATA {any}\n", .{page.data});
    }

    pub fn readRow(self: *Self, idx: u32, row: *Row) !bool {
        const rowIdxOnPage, const pageIdx = self.getIdxsFromRowIdx(idx);
        if (self.pages[pageIdx]) |page| {
            const rowSlice = self.getRowSliceFromIdx(rowIdxOnPage, page);
            try row.deserializeFromSlice(rowSlice);
            return true;
        } else {
            return false;
        }
    }
};

const Row = struct {
    const Self = @This();

    id: u32,
    username: [USERNAME_SIZE]u8,
    email: [EMAIL_SIZE]u8,

    pub fn init(allocator: *Allocator) !*Self {
        return try allocator.create(Row);
    }

    pub fn initFromUtf8(idUtf8: []const u8, usernameUtf8: []const u8, emailUtf8: []const u8, allocator: *Allocator) !*Self {
        const id = try std.fmt.parseInt(u32, idUtf8, 10);

        var idBytes: [ID_SIZE]u8 = .{ 0, 0, 0, 0 };
        mem.writePackedIntNative(u32, idBytes[0..], 0, id);

        const row = try Row.init(allocator);
        try row.fillFromBytes(idBytes[0..], usernameUtf8, emailUtf8);

        return row;
    }

    /// Write Row bytes to dest slice
    pub fn serializeToSlice(self: *Self, dest: []u8) !void {
        try assert(dest.len >= ROW_SIZE, DbError.General);
        mem.writePackedIntNative(@TypeOf(self.id), dest[ID_OFF .. ID_OFF + ID_SIZE], 0, self.id);
        mem.copyForwards(u8, dest[USERNAME_OFF .. USERNAME_OFF + USERNAME_SIZE], self.username[0..]);
        mem.copyForwards(u8, dest[EMAIL_OFF .. EMAIL_OFF + EMAIL_SIZE], self.email[0..]);
    }

    /// Read in a Row from a previously serialized Row
    pub fn deserializeFromSlice(self: *Self, src: []u8) !void {
        try assert(src.len >= ROW_SIZE, DbError.General);
        const idSlice = src[ID_OFF .. ID_OFF + ID_SIZE];
        const usernameSlice = src[USERNAME_OFF .. USERNAME_OFF + USERNAME_SIZE];
        const emailSlice = src[EMAIL_OFF .. EMAIL_OFF + EMAIL_SIZE];

        return try self.fillFromBytes(idSlice, usernameSlice, emailSlice);
    }

    fn fillFromBytes(self: *Self, idBytes: []const u8, usernameBytes: []const u8, emailBytes: []const u8) !void {
        try assert(idBytes.len == ID_SIZE, DbError.InvalidInput);
        try assert(usernameBytes.len <= USERNAME_SIZE, DbError.InvalidInput);
        try assert(emailBytes.len <= EMAIL_SIZE, DbError.InvalidInput);

        self.id = mem.readPackedIntNative(u32, idBytes, 0);
        mem.copyForwards(u8, self.username[0..], usernameBytes);
        mem.copyForwards(u8, self.email[0..], emailBytes);
    }
};

const Insert = struct {
    const Self = @This();

    row: *Row,

    pub fn exec(self: Self, table: *Table) !void {
        try table.insertRow(self.row);
        std.debug.print("INSERTED {d}, {s}, {s}\n", .{ self.row.id, self.row.username, self.row.email });
    }

    pub fn init(idUtf8: []const u8, usernameUtf8: []const u8, emailUtf8: []const u8, arena: *Allocator) !Self {
        return Self{ .row = try Row.initFromUtf8(idUtf8, usernameUtf8, emailUtf8, arena) };
    }
};

const Select = struct {
    const Self = @This();

    arena: *Allocator,

    pub fn exec(self: Self, table: *Table) !void {
        const row = try Row.init(self.arena);
        for (0..table.nextAvailableRowIdx) |i| {
            const validRead = try table.readRow(@intCast(i), row);
            if (validRead) {
                std.debug.print("ROW IDX: {d}, ID: {d}, USERNAME: {s}, EMAIL: {s}\n", .{ i, row.id, row.username, row.email });
            }
        }
    }

    pub fn init(arena: *Allocator) Self {
        return Self{ .arena = arena };
    }
};

// Basic "interface" using tagged enum approach
// See: https://zig.news/kristoff/easy-interfaces-with-zig-0100-2hc5
const Statement = union(enum) {
    const Self = @This();

    insert: Insert,
    select: Select,

    pub fn init(inbuf: *InputBuf, arena: *Allocator) !Self {
        var tokens = mem.splitScalar(u8, inbuf.getInput(), ' ');

        const cmd = tokens.next() orelse return DbError.InvalidInput;
        if (mem.eql(u8, cmd, "insert")) {
            const id = tokens.next() orelse return DbError.InvalidInput;
            const username = tokens.next() orelse return DbError.InvalidInput;
            const email = tokens.next() orelse return DbError.InvalidInput;
            const insert = try Insert.init(id, username, email, arena);
            return Self{
                .insert = insert,
            };
        }

        if (mem.eql(u8, cmd, "select")) {
            const select = Select.init(arena);
            return Self{
                .select = select,
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
        // statement allocations are alloc/freed once per exec, all statement
        // execs can alloc without freeing
        var statementAllocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        var arena = statementAllocator.allocator();
        defer statementAllocator.deinit();
        printPrompt();
        try inbuf.read();
        if (inbuf.startsWith(".")) {
            if (doMetaCmd(&inbuf)) |_| {} else |_| {
                std.debug.print("Unrecognized meta command: <<<{s}>>>\n", .{inbuf.getInput()});
            }
            continue;
        }
        if (Statement.init(&inbuf, &arena)) |s| {
            var statement = s;
            try statement.exec(&table);
        } else |err| {
            std.debug.print("ERROR <<<{any}>>> FOR INPUT <<<{s}>>>\n", .{ err, inbuf.getInput() });
        }
    }
}

test "inserts and selects rows" {
    const idx: u32 = 14;
    const pages: [TAB_MAX_PAGES]?usize = undefined;
    const pageIdx = @divFloor(idx, TAB_ROWS_PER_PAGE);

    std.debug.print("TEST {any}\n", .{pages[pageIdx]});
}
