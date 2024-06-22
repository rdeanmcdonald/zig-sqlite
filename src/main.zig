const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const DbError = error{ General, InvalidInput, TableFull };
const MetaCmdRes = enum { META_CMD_SUCCESS, META_CMD_UNRECOGNIZED };
const StatementType = enum { STATEMENT_INSERT, STATEMENT_SELECT };

fn assert(ok: bool, err: DbError) !void {
    if (!ok) {
        return err;
    }
}

const TAB_PAGE_SIZE = 4096;
const TAB_MAX_PAGES = 10000;
const TAB_ROWS_PER_PAGE = TAB_PAGE_SIZE / ROW_SIZE;
const TAB_MAX_ROWS = TAB_ROWS_PER_PAGE * TAB_MAX_PAGES;

const ID_SIZE = @sizeOf(u32);
const USERNAME_SIZE = 32;
const USERNAME_LEN_SIZE = 1;
const EMAIL_SIZE = 255;
const EMAIL_LEN_SIZE = 1;
const ROW_SIZE = ID_SIZE + USERNAME_LEN_SIZE + USERNAME_SIZE + EMAIL_LEN_SIZE + EMAIL_SIZE;

const ID_OFF = 0;
const USERNAME_LEN_OFF = ID_OFF + ID_SIZE;
const USERNAME_OFF = USERNAME_LEN_OFF + USERNAME_LEN_SIZE;
const EMAIL_LEN_OFF = USERNAME_OFF + USERNAME_SIZE;
const EMAIL_OFF = EMAIL_LEN_OFF + EMAIL_LEN_SIZE;

const Reader = std.io.StreamSource.Reader;
const Writer = std.io.StreamSource.Writer;

const Cli = struct {
    const Self = @This();

    reader: std.io.BufferedReader(4096, Reader),
    writer: std.io.BufferedWriter(4096, Writer),

    pub fn init(r: Reader, w: Writer) Self {
        return .{
            .reader = std.io.bufferedReader(r),
            .writer = std.io.bufferedWriter(w),
        };
    }
};

const Page = struct {
    data: [TAB_PAGE_SIZE]u8,
};

const Pager = struct {
    const Self = @This();

    pages: [TAB_MAX_PAGES]?*Page,
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

        const lastPageIdx = @divFloor(fileLength, TAB_PAGE_SIZE);
        var pageIdx: usize = 0;
        var offset: usize = 0;
        while (pageIdx <= lastPageIdx) : ({
            pageIdx += 1;
            offset += TAB_PAGE_SIZE;
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
        try assert(idx < TAB_MAX_PAGES, DbError.General);
        return self.pages[idx];
    }
};

const Table = struct {
    const Self = @This();

    numRows: u64,
    pager: *Pager,

    pub fn init(pager: *Pager) Self {
        const numRows = @divFloor(pager.fileLength, ROW_SIZE);
        return Self{
            .numRows = @divFloor(pager.fileLength, ROW_SIZE),
            .pager = pager,
        };
    }

    pub fn insertRow(self: *Self, row: *Row) !void {
        if (self.numRows >= TAB_MAX_ROWS) {
            return error.TableFull;
        }

        const rowIdxOnPage, const pageIdx = self.getIdxsFromRowIdx(self.numRows);
        const page = try self.pager.getOrCreatePage(pageIdx);
        try self.writeRowData(rowIdxOnPage, row, page);
    }

    fn getIdxsFromRowIdx(_: *Self, rowIdx: u64) [2]u64 {
        const rowIdxInPage = @mod(rowIdx, TAB_ROWS_PER_PAGE);
        const pageIdx = @divFloor(rowIdx, TAB_ROWS_PER_PAGE);
        return .{ rowIdxInPage, pageIdx };
    }

    fn getRowSliceFromIdx(_: *Self, idx: u64, page: *Page) []u8 {
        const rowOffset = idx * ROW_SIZE;
        return page.data[rowOffset .. rowOffset + ROW_SIZE];
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
        var offset: usize = 0;
        var pageNum: usize = 0;
// LAST FULL PAGE: 0
// ROWS PER PAGE: 13
// PAGE NUM: 0
// OFFSET: 0
// NOT PARTIAL PAGE
// PAGE NUM: 1
// OFFSET: 4096
// POTENTIAL PARTIAL PAGE
// PAGE NUM: 2
// OFFSET: 8192
// POTENTIAL PARTIAL PAGE
        const lastFullPage = @divFloor(self.numRows, TAB_ROWS_PER_PAGE) - 1;
        while (self.pager.pages[pageNum]) |page| : ({
            offset += page.data.len;
            pageNum += 1;
        }) {
            if (pageNum <= lastFullPage) {
                try self.pager.file.pwriteAll(page.data[0..], offset);
            } else {
                // last page, will break out of while loop next
                const rowIdxOnPage, _ = self.getIdxsFromRowIdx(self.numRows);
                const lastRowLastByte = rowIdxOnPage * ROW_SIZE;
                try self.pager.file.pwriteAll(page.data[0 .. lastRowLastByte], offset);
            }
        }
    }
};

const Row = struct {
    const Self = @This();

    id: u32,
    username: [USERNAME_SIZE]u8,
    username_len: u8, // username can't be > 32 bits
    email: [EMAIL_SIZE]u8,
    email_len: u8, // email can't be > 255 bits

    pub const rowFormat = "({d}, {s}, {s})\n";

    pub fn init(allocator: *Allocator) !*Self {
        return try allocator.create(Row);
    }

    pub fn initFromUtf8(idUtf8: []const u8, usernameUtf8: []const u8, emailUtf8: []const u8, allocator: *Allocator) !*Self {
        const id = try std.fmt.parseInt(u32, idUtf8, 10);

        var idBytes: [ID_SIZE]u8 = .{ 0, 0, 0, 0 };
        mem.writePackedIntNative(u32, idBytes[0..], 0, id);

        var usernameLenBytes: [USERNAME_LEN_SIZE]u8 = .{0};
        mem.writePackedIntNative(u8, usernameLenBytes[0..], 0, @intCast(usernameUtf8.len));

        var emailLenBytes: [EMAIL_LEN_SIZE]u8 = .{0};
        mem.writePackedIntNative(u8, emailLenBytes[0..], 0, @intCast(emailUtf8.len));

        const row = try Row.init(allocator);
        try row.fillFromBytes(idBytes[0..], usernameLenBytes[0..], usernameUtf8, emailLenBytes[0..], emailUtf8);

        return row;
    }

    /// Write Row bytes to dest slice
    pub fn serializeToSlice(self: *Self, dest: []u8) !void {
        try assert(dest.len >= ROW_SIZE, DbError.General);
        mem.writePackedIntNative(@TypeOf(self.id), dest[ID_OFF .. ID_OFF + ID_SIZE], 0, self.id);
        mem.writePackedIntNative(@TypeOf(self.username_len), dest[USERNAME_LEN_OFF .. USERNAME_LEN_OFF + USERNAME_LEN_SIZE], 0, self.username_len);
        mem.copyForwards(u8, dest[USERNAME_OFF .. USERNAME_OFF + USERNAME_SIZE], self.username[0..]);
        mem.writePackedIntNative(@TypeOf(self.email_len), dest[EMAIL_LEN_OFF .. EMAIL_LEN_OFF + EMAIL_LEN_SIZE], 0, self.email_len);
        mem.copyForwards(u8, dest[EMAIL_OFF .. EMAIL_OFF + EMAIL_SIZE], self.email[0..]);
    }

    /// Read in a Row from a previously serialized Row
    pub fn deserializeFromSlice(self: *Self, src: []u8) !void {
        try assert(src.len >= ROW_SIZE, DbError.General);
        const idSlice = src[ID_OFF .. ID_OFF + ID_SIZE];
        const usernameLenSlice = src[USERNAME_LEN_OFF .. USERNAME_LEN_OFF + USERNAME_LEN_SIZE];
        const usernameSlice = src[USERNAME_OFF .. USERNAME_OFF + USERNAME_SIZE];
        const emailLenSlice = src[EMAIL_LEN_OFF .. EMAIL_LEN_OFF + EMAIL_LEN_SIZE];
        const emailSlice = src[EMAIL_OFF .. EMAIL_OFF + EMAIL_SIZE];

        return try self.fillFromBytes(idSlice, usernameLenSlice, usernameSlice, emailLenSlice, emailSlice);
    }

    fn fillFromBytes(self: *Self, idBytes: []const u8, usernameLenBytes: []const u8, usernameBytes: []const u8, emailLenBytes: []const u8, emailBytes: []const u8) !void {
        try assert(idBytes.len == ID_SIZE, DbError.InvalidInput);
        try assert(usernameBytes.len <= USERNAME_SIZE, DbError.InvalidInput);
        try assert(emailBytes.len <= EMAIL_SIZE, DbError.InvalidInput);

        self.id = mem.readPackedIntNative(u32, idBytes, 0);
        self.username_len = mem.readPackedIntNative(u8, usernameLenBytes, 0);
        self.email_len = mem.readPackedIntNative(u8, emailLenBytes, 0);
        mem.copyForwards(u8, self.username[0..], usernameBytes);
        mem.copyForwards(u8, self.email[0..], emailBytes);
    }
};

const Insert = struct {
    const Self = @This();

    row: *Row,
    cli: *Cli,

    pub fn exec(self: Self, table: *Table) !void {
        try table.insertRow(self.row);
        try self.cli.writer.writer().print("INSERTED {d}, {s}, {s}\n", .{ self.row.id, self.row.username[0..self.row.username_len], self.row.email[0..self.row.email_len] });
    }

    pub fn init(idUtf8: []const u8, usernameUtf8: []const u8, emailUtf8: []const u8, arena: *Allocator, cli: *Cli) !Self {
        return Self{ .row = try Row.initFromUtf8(idUtf8, usernameUtf8, emailUtf8, arena), .cli = cli };
    }
};

const Select = struct {
    const Self = @This();

    arena: *Allocator,
    cli: *Cli,

    pub fn exec(self: Self, table: *Table) !void {
        const row = try Row.init(self.arena);
        for (0..table.numRows) |i| {
            const validRead = try table.readRow(@intCast(i), row);
            if (validRead) {
                try self.cli.writer.writer().print(Row.rowFormat, .{ row.id, row.username[0..row.username_len], row.email[0..row.email_len] });
            }
        }
    }

    pub fn init(arena: *Allocator, cli: *Cli) Self {
        return Self{ .arena = arena, .cli = cli };
    }
};

const Exit = struct {
    const Self = @This();

    cli: *Cli,

    pub fn exec(self: Self, table: *Table) !void {
        try self.cli.writer.writer().print("Shutting down the db...\n", .{});
        try table.close();
        std.posix.exit(0);
    }

    pub fn init(cli: *Cli) Self {
        return Self{ .cli = cli };
    }
};

// Basic "interface" using tagged enum approach
// See: https://zig.news/kristoff/easy-interfaces-with-zig-0100-2hc5
const Statement = union(enum) {
    const Self = @This();

    insert: Insert,
    select: Select,
    exit: Exit,

    pub fn init(inbuf: *InputBuf, arena: *Allocator, cli: *Cli) !Self {
        var tokens = mem.splitScalar(u8, inbuf.getInput(), ' ');

        const cmd = tokens.next() orelse return DbError.InvalidInput;
        if (mem.eql(u8, cmd, "insert")) {
            const id = tokens.next() orelse return DbError.InvalidInput;
            const username = tokens.next() orelse return DbError.InvalidInput;
            const email = tokens.next() orelse return DbError.InvalidInput;
            const insert = try Insert.init(id, username, email, arena, cli);
            return Self{
                .insert = insert,
            };
        }

        if (mem.eql(u8, cmd, "select")) {
            const select = Select.init(arena, cli);
            return Self{
                .select = select,
            };
        }

        if (mem.eql(u8, cmd, ".exit")) {
            return Self{
                .exit = Exit{ .cli = cli },
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

const InputBuf = struct {
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

    pub fn startsWith(self: *Self, tst: []const u8) bool {
        if (std.mem.startsWith(u8, self.getInput(), tst)) {
            return true;
        } else {
            return false;
        }
    }
};

fn printPromptAndFlush(cli: *Cli) !void {
    try cli.writer.writer().print("db > ", .{});
    try cli.writer.flush();
}

pub fn runDb(cli: *Cli, filename: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();
    var inbuf = try InputBuf.init(&allocator, cli);
    defer inbuf.deinit();
    var pager = try Pager.init(&allocator, filename);
    defer pager.deinit();
    var table = Table.init(&pager);
    while (true) {
        // statement allocations are alloc/freed once per exec, all statement
        // execs can alloc without freeing
        var statementAllocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer statementAllocator.deinit();

        var arena = statementAllocator.allocator();

        try printPromptAndFlush(cli);
        try inbuf.read();
        if (Statement.init(&inbuf, &arena, cli)) |s| {
            try s.exec(&table);
        } else |err| {
            try cli.writer.writer().print("ERROR <<<{any}>>> FOR INPUT <<<{s}>>>\n", .{ err, inbuf.getInput() });
        }
    }
}

pub fn main() !void {
    var inStream = std.io.StreamSource{ .file = std.io.getStdIn() };
    var outStream = std.io.StreamSource{ .file = std.io.getStdOut() };
    var cli = Cli.init(inStream.reader(), outStream.writer());
    var args = std.process.args();
    _ = args.skip();
    if (args.next()) |filename| {
        try runDb(&cli, filename);
    }
    try cli.writer.writer().print("Please provide a db filename\n", .{});
    try cli.writer.flush();
}

test "inserts, selects, and exits for large number of rows" {
    // Create the test IO things
    const inPipeReadEnd, const inPipeWriteEnd = try std.posix.pipe();
    const outPipeReadEnd, const outPipeWriteEnd = try std.posix.pipe();
    var cliInStream = std.io.StreamSource{ .file = std.fs.File{ .handle = inPipeReadEnd } };
    var cliOutStream = std.io.StreamSource{ .file = std.fs.File{ .handle = outPipeWriteEnd } };
    var userInputStream = std.io.StreamSource{ .file = std.fs.File{ .handle = inPipeWriteEnd } };
    var dbOutputStream = std.io.StreamSource{ .file = std.fs.File{ .handle = outPipeReadEnd } };
    var cli = Cli.init(cliInStream.reader(), cliOutStream.writer());

    // Create the test temp db file
    // var tmpDir = std.testing.tmpDir(.{});
    // defer tmpDir.cleanup();
    // var buffer: [1000]u8 = undefined;
    // var fba = std.heap.FixedBufferAllocator.init(&buffer);
    // defer fba.reset();
    // const tmpDirPath = try tmpDir.dir.realpathAlloc(fba.allocator(), ".");
    // const filename = "/temp.db";
    // var tmpDbFilePath = try fba.allocator().alloc(u8, tmpDirPath.len + filename.len);
    // std.mem.copyForwards(u8, tmpDbFilePath, tmpDirPath);
    // std.mem.copyForwards(u8, tmpDbFilePath[tmpDirPath.len..], filename);

    const tmpDbFilePath = "/home/rmcdonald/random/test.db";
    // Fork the process, and run the db in the child
    const fork_pid = try std.posix.fork();
    if (fork_pid == 0) {
        // Child process
        try runDb(&cli, tmpDbFilePath);
    } else {
        // Parent process
        var buf: [4096]u8 = undefined;
        var fbs = std.io.fixedBufferStream(buf[0..]);
        const usersToInsert = 1000;

        // insert all the users
        for (0..usersToInsert) |i| {
            try userInputStream.writer().print("insert {d} someusername some@email.com\n", .{i});
            try dbOutputStream.reader().streamUntilDelimiter(fbs.writer(), '\n', null);
            const answer = fbs.getWritten();
            fbs.reset();

            try std.fmt.format(fbs.writer(), "db > INSERTED {d}, someusername, some@email.com", .{i});
            const correctAnswer = fbs.getWritten();
            fbs.reset();
            try std.testing.expect(std.mem.eql(u8, answer, correctAnswer));
        }

        // remove the 'db > ' from the first line
        try dbOutputStream.reader().skipBytes(5, .{});
        try userInputStream.writer().print("select\n", .{});
        for (0..usersToInsert) |i| {
            try dbOutputStream.reader().streamUntilDelimiter(fbs.writer(), '\n', null);
            const answer = fbs.getWritten();
            fbs.reset();

            try std.fmt.format(fbs.writer(), Row.rowFormat, .{ i, "someusername", "some@email.com" });
            const correctAnswer = fbs.getWritten();
            fbs.reset();
            try std.testing.expect(std.mem.eql(u8, answer, correctAnswer[0 .. correctAnswer.len - 1]));
        }

        // Confirm exit persists the data
        try userInputStream.writer().print(".exit\n", .{});
    }
}
