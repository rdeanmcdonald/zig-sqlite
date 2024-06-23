const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const InputBuf = @import("input_buffer.zig");
const Table = @import("table.zig");
const Row = @import("row.zig");
const e = @import("errors.zig");

// Basic "interface" using tagged enum approach
// See: https://zig.news/kristoff/easy-interfaces-with-zig-0100-2hc5
pub const Statement = union(enum) {
    const Self = @This();

    insert: Insert,
    select: Select,
    exit: Exit,

    pub fn init(inbuf: *InputBuf, arena: *Allocator, cli: *Cli) !Self {
        var tokens = mem.splitScalar(u8, inbuf.getInput(), ' ');

        const cmd = tokens.next() orelse return e.DbError.InvalidInput;
        if (mem.eql(u8, cmd, "insert")) {
            const id = tokens.next() orelse return e.DbError.InvalidInput;
            const username = tokens.next() orelse return e.DbError.InvalidInput;
            const email = tokens.next() orelse return e.DbError.InvalidInput;
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
        return e.DbError.General;
    }

    pub fn exec(self: Self, table: *Table) !bool {
        switch (self) {
            inline else => |case| return try case.exec(table),
        }
    }
};

const Insert = struct {
    const Self = @This();

    row: *Row,
    cli: *Cli,

    pub fn exec(self: Self, table: *Table) !bool {
        try table.insertRow(self.row);
        try self.cli.writer.writer().print("INSERTED {d}, {s}, {s}\n", .{ self.row.id, self.row.username[0..self.row.username_len], self.row.email[0..self.row.email_len] });
        return true;
    }

    pub fn init(idUtf8: []const u8, usernameUtf8: []const u8, emailUtf8: []const u8, arena: *Allocator, cli: *Cli) !Self {
        return Self{ .row = try Row.initFromUtf8(idUtf8, usernameUtf8, emailUtf8, arena), .cli = cli };
    }
};

const Select = struct {
    const Self = @This();

    arena: *Allocator,
    cli: *Cli,

    pub fn exec(self: Self, table: *Table) !bool {
        const row = try Row.init(self.arena);
        for (0..table.numRows) |i| {
            const validRead = try table.readRow(@intCast(i), row);
            if (validRead) {
                try self.cli.writer.writer().print(Row.rowFormat, .{ row.id, row.username[0..row.username_len], row.email[0..row.email_len] });
            }
        }
        return true;
    }

    pub fn init(arena: *Allocator, cli: *Cli) Self {
        return Self{ .arena = arena, .cli = cli };
    }
};

const Exit = struct {
    const Self = @This();

    cli: *Cli,

    pub fn exec(self: Self, table: *Table) !bool {
        try self.cli.writer.writer().print("Shutting down the db...\n", .{});
        try table.close();
        return false;
    }

    pub fn init(cli: *Cli) Self {
        return Self{ .cli = cli };
    }
};
