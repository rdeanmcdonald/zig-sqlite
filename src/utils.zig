const e = @import("errors.zig");

pub fn assert(ok: bool, err: e.DbError) !void {
    if (!ok) {
        return err;
    }
}
