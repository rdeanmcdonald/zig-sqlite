const std = @import("std");

const Self = @This();
pub const Reader = std.io.StreamSource.Reader;
pub const Writer = std.io.StreamSource.Writer;

reader: std.io.BufferedReader(4096, Reader),
writer: std.io.BufferedWriter(4096, Writer),

pub fn init(r: Reader, w: Writer) Self {
    return .{
        .reader = std.io.bufferedReader(r),
        .writer = std.io.bufferedWriter(w),
    };
}
