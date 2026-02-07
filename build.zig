const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- packages (modules) ---

    const tokenizer_mod = b.addModule("tokenizer", .{
        .root_source_file = b.path("packages/tokenizer/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const parser_mod = b.addModule("parser", .{
        .root_source_file = b.path("packages/parser/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tokenizer", .module = tokenizer_mod },
        },
    });

    const core_mod = b.addModule("core", .{
        .root_source_file = b.path("packages/core/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tokenizer", .module = tokenizer_mod },
            .{ .name = "parser", .module = parser_mod },
        },
    });

    // --- app ---

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("app/src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "core", .module = core_mod },
            .{ .name = "tokenizer", .module = tokenizer_mod },
            .{ .name = "parser", .module = parser_mod },
        },
    });

    const exe = b.addExecutable(.{
        .name = "sqlite0",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run sqlite0");
    run_step.dependOn(&run_cmd.step);

    // --- tests ---

    const tokenizer_test_mod = b.createModule(.{
        .root_source_file = b.path("packages/tokenizer/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const tokenizer_tests = b.addTest(.{
        .root_module = tokenizer_test_mod,
    });

    const parser_test_mod = b.createModule(.{
        .root_source_file = b.path("packages/parser/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tokenizer", .module = tokenizer_mod },
        },
    });
    const parser_tests = b.addTest(.{
        .root_module = parser_test_mod,
    });

    const core_test_mod = b.createModule(.{
        .root_source_file = b.path("packages/core/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tokenizer", .module = tokenizer_mod },
            .{ .name = "parser", .module = parser_mod },
        },
    });
    const core_tests = b.addTest(.{
        .root_module = core_test_mod,
    });

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&b.addRunArtifact(tokenizer_tests).step);
    test_step.dependOn(&b.addRunArtifact(parser_tests).step);
    test_step.dependOn(&b.addRunArtifact(core_tests).step);
}
