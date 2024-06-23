const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Cli = @import("cli.zig");
const c = @import("constants.zig");
const e = @import("errors.zig");
const Pager = @import("pager.zig");
const Page = Pager.Page;
const assert = @import("utils.zig").assert;

const Self = @This();

id: u32,
username: [c.USERNAME_SIZE]u8,
username_len: u8, // username can't be > 32 bits
email: [c.EMAIL_SIZE]u8,
email_len: u8, // email can't be > 255 bits

pub const rowFormat = "({d}, {s}, {s})\n";

pub fn init(allocator: *Allocator) !*Self {
    return try allocator.create(Self);
}

pub fn initFromUtf8(idUtf8: []const u8, usernameUtf8: []const u8, emailUtf8: []const u8, allocator: *Allocator) !*Self {
    const id = try std.fmt.parseInt(u32, idUtf8, 10);

    var idBytes: [c.ID_SIZE]u8 = .{ 0, 0, 0, 0 };
    mem.writePackedIntNative(u32, idBytes[0..], 0, id);

    var usernameLenBytes: [c.USERNAME_LEN_SIZE]u8 = .{0};
    mem.writePackedIntNative(u8, usernameLenBytes[0..], 0, @intCast(usernameUtf8.len));

    var emailLenBytes: [c.EMAIL_LEN_SIZE]u8 = .{0};
    mem.writePackedIntNative(u8, emailLenBytes[0..], 0, @intCast(emailUtf8.len));

    const row = try Self.init(allocator);
    try row.fillFromBytes(idBytes[0..], usernameLenBytes[0..], usernameUtf8, emailLenBytes[0..], emailUtf8);

    return row;
}

/// Write Row bytes to dest slice
pub fn serializeToSlice(self: *Self, dest: []u8) !void {
    try assert(dest.len >= c.ROW_SIZE, e.DbError.General);
    mem.writePackedIntNative(@TypeOf(self.id), dest[c.ID_OFF .. c.ID_OFF + c.ID_SIZE], 0, self.id);
    mem.writePackedIntNative(@TypeOf(self.username_len), dest[c.USERNAME_LEN_OFF .. c.USERNAME_LEN_OFF + c.USERNAME_LEN_SIZE], 0, self.username_len);
    mem.copyForwards(u8, dest[c.USERNAME_OFF .. c.USERNAME_OFF + c.USERNAME_SIZE], self.username[0..]);
    mem.writePackedIntNative(@TypeOf(self.email_len), dest[c.EMAIL_LEN_OFF .. c.EMAIL_LEN_OFF + c.EMAIL_LEN_SIZE], 0, self.email_len);
    mem.copyForwards(u8, dest[c.EMAIL_OFF .. c.EMAIL_OFF + c.EMAIL_SIZE], self.email[0..]);
}

/// Read in a Row from a previously serialized Row
pub fn deserializeFromSlice(self: *Self, src: []u8) !void {
    try assert(src.len >= c.ROW_SIZE, e.DbError.General);
    const idSlice = src[c.ID_OFF .. c.ID_OFF + c.ID_SIZE];
    const usernameLenSlice = src[c.USERNAME_LEN_OFF .. c.USERNAME_LEN_OFF + c.USERNAME_LEN_SIZE];
    const usernameSlice = src[c.USERNAME_OFF .. c.USERNAME_OFF + c.USERNAME_SIZE];
    const emailLenSlice = src[c.EMAIL_LEN_OFF .. c.EMAIL_LEN_OFF + c.EMAIL_LEN_SIZE];
    const emailSlice = src[c.EMAIL_OFF .. c.EMAIL_OFF + c.EMAIL_SIZE];

    return try self.fillFromBytes(idSlice, usernameLenSlice, usernameSlice, emailLenSlice, emailSlice);
}

fn fillFromBytes(self: *Self, idBytes: []const u8, usernameLenBytes: []const u8, usernameBytes: []const u8, emailLenBytes: []const u8, emailBytes: []const u8) !void {
    try assert(idBytes.len == c.ID_SIZE, e.DbError.InvalidInput);
    try assert(usernameBytes.len <= c.USERNAME_SIZE, e.DbError.InvalidInput);
    try assert(emailBytes.len <= c.EMAIL_SIZE, e.DbError.InvalidInput);

    self.id = mem.readPackedIntNative(u32, idBytes, 0);
    self.username_len = mem.readPackedIntNative(u8, usernameLenBytes, 0);
    self.email_len = mem.readPackedIntNative(u8, emailLenBytes, 0);
    mem.copyForwards(u8, self.username[0..], usernameBytes);
    mem.copyForwards(u8, self.email[0..], emailBytes);
}
