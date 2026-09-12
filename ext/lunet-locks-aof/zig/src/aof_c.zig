//! The C ABI over the vendored TigerBeetle AOF (0.17.9).
//!
//! The ABI exposes the write-behind log's blocking surface:
//! open (create a file in a directory), append (wrap one record into an
//! AOFEntry with a fresh Prepare header and a valid Aegis checksum chain),
//! flush (the explicit checkpoint fsync), close, and the read-back iterator
//! the smoke tests use. The record bytes ride inside the AOF entry; the
//! on-disk format is byte-identical to upstream's AOF file layout, so the
//! vendored `Iterator` (per-entry header/body checksum validation and the
//! checksum chain) reads every file back.
//!
//! The force knob is a construction-time flag, exactly the "optional
//! durability force" contract: with force off (the default, and the mode
//! the standby learner runs in) appends are page-cached blocking writes and
//! durability lands only when the caller flushes — or when the entry
//! window reaches the vendored `journal_slot_count` cap, which preserves
//! upstream's "never buffer more than the WAL can hold" bound (here the
//! bound closes itself with a flush, since there is no WAL to borrow
//! durability from). With force on, every append is immediately followed
//! by the fsync — the upstream checkpoint's flush discipline applied per
//! entry.
const std = @import("std");
const assert = std.debug.assert;

const aof = @import("aof.zig");
const constants = @import("constants.zig");
const vsr = @import("vsr.zig");
const io_backend = @import("io.zig");

const AOF = aof.AOFType(io_backend.IO);
const Header = vsr.Header;
const IO = io_backend.IO;
const MessagePool = vsr.MessagePool;

const log = std.log.scoped(.aof_c);

/// One open AOF file plus the C ABI's own header-builder state: the entry
/// chain (each record's Prepare header names the previous entry's checksum
/// as its parent), the op counter, and the force knob. The message pool
/// carries the one Prepare message the append path re-uses.
pub const AofFile = struct {
    aof: AOF,
    io: *IO,
    pool: *MessagePool,
    op: u64,
    /// Records appended since the last durable flush. The vendored write
    /// path keeps its own window counter with the same cap; this shadow
    /// drives the wrap-side flush when the force knob is off.
    unflushed: u64,
    force_flush: bool,
};

/// C ABI result codes. 0 is success; negative are errors.
pub const OK: i32 = 0;
pub const INVALID: i32 = -1;
pub const TOO_LARGE: i32 = -6;
pub const SERVICE: i32 = -7;

/// Open (create or open, never truncate) an AOF file at `path`.
///
/// `force_flush` is the optional durability force: 0 (the default for the
/// telemetry standby) keeps appends page-cached, with durability only on
/// explicit flushes and at the entry-window cap; non-zero fsyncs after
/// every append. The path must end in `.aof` and its parent directory must
/// exist — the caller owns directory creation.
export fn lunet_aof_open(
    path_data: [*]const u8,
    path_len: usize,
    force_flush: u8,
    out: *?*AofFile,
) i32 {
    if (path_len == 0 or path_len > std.fs.max_path_bytes) return INVALID;
    if (force_flush > 1) return INVALID;
    const path = path_data[0..path_len];

    if (!std.mem.endsWith(u8, path, ".aof")) {
        return INVALID;
    }

    const io = std.heap.c_allocator.create(IO) catch return SERVICE;
    io.* = IO.init(32, 0) catch {
        std.heap.c_allocator.destroy(io);
        return SERVICE;
    };

    const pool = std.heap.c_allocator.create(MessagePool) catch {
        std.heap.c_allocator.destroy(io);
        return SERVICE;
    };
    pool.* = MessagePool.init_capacity(std.heap.c_allocator, 1) catch {
        std.heap.c_allocator.destroy(pool);
        std.heap.c_allocator.destroy(io);
        return SERVICE;
    };

    const file = std.heap.c_allocator.create(AofFile) catch {
        std.heap.c_allocator.destroy(pool);
        std.heap.c_allocator.destroy(io);
        return SERVICE;
    };
    file.* = .{
        .aof = AOF.init(io, path) catch {
            std.heap.c_allocator.destroy(file);
            std.heap.c_allocator.destroy(pool);
            std.heap.c_allocator.destroy(io);
            return SERVICE;
        },
        .io = io,
        .pool = pool,
        .op = 0,
        .unflushed = 0,
        .force_flush = force_flush != 0,
    };
    out.* = file;
    return OK;
}

/// Append one record. The record bytes become the body of a fresh Prepare
/// entry: the header carries the AOF's chain (parent = the previous
/// entry's checksum), a monotonic op, the current unix-milliseconds
/// timestamp, and valid Aegis checksums — the exact on-disk shape an
/// upstream `aof debug` run parses.
export fn lunet_aof_append(
    file: *AofFile,
    data: [*]const u8,
    len: usize,
    out_op: ?*u64,
) i32 {
    if (len > constants.message_body_size_max) return TOO_LARGE;
    if (file.unflushed >= constants.journal_slot_count) return SERVICE;

    const message = file.pool.get_message(.prepare);
    defer file.pool.unref(message);

    header: {
        message.header.* = .{
            .op = file.op + 1,
            .commit = 0,
            .view = 0,
            .client = 0,
            .request = 0,
            .parent = file.aof.last_checksum orelse 0,
            .request_checksum = 0,
            .cluster = 0,
            .timestamp = @intCast(@max(std.time.milliTimestamp(), 0)),
            .checkpoint_id = 0,
            .release = vsr.Release.minimum,
            .command = .prepare,
            .operation = .pulse,
            .size = @intCast(@sizeOf(Header) + len),
        };
        break :header;
    }
    @memcpy(message.body_used(), data[0..len]);
    message.header.set_checksum_body(data[0..len]);
    message.header.set_checksum();

    file.aof.write(message) catch |err| {
        log.warn("aof append failed: {}", .{err});
        return SERVICE;
    };

    file.op = message.header.op;
    file.unflushed += 1;
    if (out_op) |op| op.* = message.header.op;

    if (file.force_flush or file.unflushed >= constants.journal_slot_count) {
        return flush(file);
    }
    return OK;
}

/// The explicit flush: one fsync through the vendored checkpoint path.
/// `AOF.checkpoint` hands its completion to `IO.fsync`, which is blocking
/// in this backend, so the checkpoint completes synchronously.
export fn lunet_aof_flush(file: *AofFile) i32 {
    return flush(file);
}

fn flush(file: *AofFile) i32 {
    file.aof.checkpoint(
        @ptrCast(&flush_state),
        struct {
            fn callback(_: *anyopaque) void {
                // The blocking fsync has completed: AOF.on_fsync already
                // reset the unflushed window; the flag reports completion
                // only after the inode-change checks ran.
                flush_state.done = true;
            }
        }.callback,
    );
    if (!flush_state.done) return SERVICE;
    file.unflushed = 0;
    return OK;
}

/// The synchronous flush flag the checkpoint callback sets. The blocking
/// IO backend serializes checkpoints, so one static flag is safe: the C
/// ABI surface is documented as single-threaded (the lease-sequencer host
/// drives it from the UDP pump's thread).
var flush_state = struct {
    done: bool = false,
}{};

export fn lunet_aof_close(file: *AofFile) i32 {
    // Graceful close: the checkpoint flush lands, then the fd releases.
    const rc = flush(file);
    file.aof.close();
    std.heap.c_allocator.destroy(file);
    if (rc != OK) return rc;
    return OK;
}

/// The read-back iterator over an AOF file: per-entry header/body checksum
/// validation plus the checksum chain, upstream semantics verbatim. One
/// entry per call; the body bytes land in `out_data` and the entry's op in
/// `out_op`. Call repeatedly until it reports 0 (end of file).
export fn lunet_aof_iter_open(
    path_data: [*]const u8,
    path_len: usize,
    out: *?*AofIter,
) i32 {
    if (path_len == 0 or path_len > std.fs.max_path_bytes) return INVALID;
    const path = path_data[0..path_len];

    const io = std.heap.c_allocator.create(IO) catch return SERVICE;
    io.* = IO.init(32, 0) catch {
        std.heap.c_allocator.destroy(io);
        return SERVICE;
    };

    const it = std.heap.c_allocator.create(AofIter) catch {
        std.heap.c_allocator.destroy(io);
        return SERVICE;
    };
    it.* = .{
        .iterator = AOF.Iterator.init(io, path) catch {
            std.heap.c_allocator.destroy(it);
            std.heap.c_allocator.destroy(io);
            return SERVICE;
        },
        .io = io,
    };
    out.* = it;
    return OK;
}

pub const AofIter = struct {
    iterator: AOF.Iterator,
    io: *IO,
    entry: aof.AOFEntry align(constants.sector_size) = undefined,
};

export fn lunet_aof_iter_next(
    it: *AofIter,
    out_data: [*]u8,
    cap: usize,
    out_len: *usize,
    out_op: *u64,
) i32 {
    const entry = (it.iterator.next(&it.entry) catch |err| switch (err) {
        error.AOFShortRead => return 0, // Torn tail: a partial final entry ends iteration.
        error.AOFMagicNumberMismatch,
        error.AOFChecksumMismatch,
        error.AOFBodyChecksumMismatch,
        error.AOFChecksumChainMismatch,
        => return SERVICE,
        else => return SERVICE,
    }) orelse return 0;

    const body = it.entry.message[@sizeOf(Header)..entry.header().size];
    if (body.len > cap) return TOO_LARGE;
    @memcpy(out_data[0..body.len], body);
    out_len.* = @intCast(body.len);
    out_op.* = entry.header().op;
    return 1;
}

export fn lunet_aof_iter_close(it: *AofIter) void {
    it.iterator.close();
    std.heap.c_allocator.destroy(it);
}
