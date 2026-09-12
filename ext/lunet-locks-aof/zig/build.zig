const std = @import("std");

// The vendored AOF build: one cdylib exposing the C ABI (src/aof_c.zig),
// plus a unit-test step running the vendored AOF's own test and the
// C-ABI bridge tests. The AOF files carry TigerBeetle 0.17.9's pinned
// toolchain (Zig 0.14.1, see the repo's mise.toml).
//
// `vsr_options` mirrors upstream build.zig's options module: the vendored
// config.zig reads the release identity from it.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const vsr_options = b.addOptions();
    // The AOF writes no cluster identity of its own; the release stamp keeps
    // the config.zig plumbing identical to upstream's default production
    // values (see config.zig `configs.current`).
    vsr_options.addOption(?[40]u8, "git_commit", null);
    vsr_options.addOption(bool, "config_verify", true);
    vsr_options.addOption([]const u8, "release", "65535.0.0");
    vsr_options.addOption([]const u8, "release_client_min", "65535.0.0");

    const stdx_module = b.addModule("stdx", .{
        .root_source_file = b.path("src/stdx/stdx.zig"),
    });

    const srcs_module = b.createModule(.{
        .root_source_file = b.path("src/aof_c.zig"),
        .target = target,
        .optimize = optimize,
    });
    srcs_module.addImport("stdx", stdx_module);
    srcs_module.addOptions("vsr_options", vsr_options);
    srcs_module.link_libc = true;

    const cdylib = b.addSharedLibrary(.{
        .name = "lunet_locks_aof",
        .root_module = srcs_module,
        .version = std.SemanticVersion{ .major = 0, .minor = 1, .patch = 0 },
    });
    b.installArtifact(cdylib);

    const tests = b.addTest(.{
        .root_module = srcs_module,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run the vendored AOF tests");
    test_step.dependOn(&run_tests.step);
}
