const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const c = @import("constants.zig");
const e = @import("errors.zig");
const Pager = @import("pager.zig");
const Page = Pager.Page;
const Row = @import("row.zig");
const Table = @import("table.zig");
const InputBuf = @import("input_buffer.zig");
const Statement = @import("statement.zig").Statement;
const n = @import("node.zig");

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
    var table = try Table.init(&pager);
    while (true) {
        // statement allocations are alloc/freed once per exec, all statement
        // execs can alloc without freeing
        var statementAllocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer statementAllocator.deinit();

        var arena = statementAllocator.allocator();

        try printPromptAndFlush(cli);
        try inbuf.read();
        if (Statement.init(&inbuf, &arena, cli)) |s| {
            const keepGoing = try s.exec(&table);
            if (!keepGoing) break;
        } else |err| {
            try cli.writer.writer().print("ERROR <<<{any}>>> FOR INPUT <<<{s}>>>\n", .{ err, inbuf.getInput() });
        }
    }
    try cli.writer.flush();
}

pub fn main() !void {
    var node: n.Node = undefined;
    for (0..4096) |i| {
        var intBytes: [@sizeOf(usize)]u8 = undefined;
        mem.writePackedIntNative(usize, intBytes[0..], 0, i);
        node[i] = intBytes[0];
    }

    const nodePtr: *n.Node = &node;
    std.debug.print("NODE {*}\n", .{nodePtr});
    const ptr = n.leafNodeNumCells(&node);
    std.debug.print("PTR {d}\n", .{ptr});
    std.debug.print("PTR {d}\n", .{ptr.*});
    n.initializeLeafeNode(&node);
    std.debug.print("PTR {d}\n", .{ptr.*});
    // var inStream = std.io.StreamSource{ .file = std.io.getStdIn() };
    // var outStream = std.io.StreamSource{ .file = std.io.getStdOut() };
    // var cli = Cli.init(inStream.reader(), outStream.writer());
    // var args = std.process.args();
    // _ = args.skip();
    // if (args.next()) |filename| {
    //     try runDb(&cli, filename);
    // } else {
    //     try cli.writer.writer().print("Please provide a db filename\n", .{});
    // }
}

test "inserts, selects, and persists large number of rows" {
    // Create the test IO things
    const inPipeReadEnd, const inPipeWriteEnd = try std.posix.pipe();
    const outPipeReadEnd, const outPipeWriteEnd = try std.posix.pipe();
    var cliInStream = std.io.StreamSource{ .file = std.fs.File{ .handle = inPipeReadEnd } };
    var cliOutStream = std.io.StreamSource{ .file = std.fs.File{ .handle = outPipeWriteEnd } };
    var userInputStream = std.io.StreamSource{ .file = std.fs.File{ .handle = inPipeWriteEnd } };
    var dbOutputStream = std.io.StreamSource{ .file = std.fs.File{ .handle = outPipeReadEnd } };
    var cli = Cli.init(cliInStream.reader(), cliOutStream.writer());

    // Create the test temp db file
    var tmpDir = std.testing.tmpDir(.{});
    defer tmpDir.cleanup();
    var buffer: [1000]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buffer);
    defer fba.reset();
    const tmpDirPath = try tmpDir.dir.realpathAlloc(fba.allocator(), ".");
    const filename = "/temp.db";
    var tmpDbFilePath = try fba.allocator().alloc(u8, tmpDirPath.len + filename.len);
    std.mem.copyForwards(u8, tmpDbFilePath, tmpDirPath);
    std.mem.copyForwards(u8, tmpDbFilePath[tmpDirPath.len..], filename);

    // Fork the process, and run the db in the child
    var fork_pid = try std.posix.fork();
    if (fork_pid == 0) {
        // Child process
        try runDb(&cli, tmpDbFilePath);
        std.posix.exit(0);
    }

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

    // Confirm exit
    try userInputStream.writer().print(".exit\n", .{});
    try dbOutputStream.reader().streamUntilDelimiter(fbs.writer(), '\n', null);
    var exitText = fbs.getWritten();
    fbs.reset();

    try std.testing.expect(std.mem.eql(u8, exitText, "db > Shutting down the db..."));
    var dbstatus = std.posix.waitpid(fork_pid, 0).status;
    try std.testing.expect(dbstatus == 0);

    // confirm data was persisted
    fork_pid = try std.posix.fork();
    if (fork_pid == 0) {
        // Child process
        try runDb(&cli, tmpDbFilePath);
        std.posix.exit(0);
    }

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

    // Confirm exit
    try userInputStream.writer().print(".exit\n", .{});
    try dbOutputStream.reader().streamUntilDelimiter(fbs.writer(), '\n', null);
    exitText = fbs.getWritten();
    fbs.reset();

    try std.testing.expect(std.mem.eql(u8, exitText, "db > Shutting down the db..."));
    dbstatus = std.posix.waitpid(fork_pid, 0).status;
    try std.testing.expect(dbstatus == 0);
}
