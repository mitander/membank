//! Git Repository Source Connector
//!
//! Implements the Source interface for Git repositories. This connector can
//! fetch content from local Git repositories, providing file content along
//! with Git metadata like commit hashes, file paths, and timestamps.
//!
//! Design Notes:
//! - Uses VFS abstraction for simulation testing compatibility
//! - Focuses on simplicity over full Git feature support
//! - Arena-based memory management for lifecycle safety
//! - Single-threaded execution model

const std = @import("std");

const assert_mod = @import("../core/assert.zig");
const concurrency = @import("../core/concurrency.zig");
const error_context = @import("../core/error_context.zig");
const glob_matcher = @import("glob_matcher.zig");
const ingestion = @import("pipeline.zig");
const vfs = @import("../core/vfs.zig");

const testing = std.testing;

const IngestionError = ingestion.IngestionError;
const Source = ingestion.Source;
const SourceContent = ingestion.SourceContent;
const SourceIterator = ingestion.SourceIterator;
const VFS = vfs.VFS;

/// Configuration for Git repository source
pub const GitSourceConfig = struct {
    /// Path to the Git repository (local path)
    repository_path: []const u8,
    /// Optional branch/commit to checkout (defaults to HEAD)
    ref: ?[]const u8 = null,
    /// File patterns to include (e.g., "*.zig", "*.md")
    include_patterns: []const []const u8,
    /// File patterns to exclude (e.g., "*.bin", ".git/*")
    exclude_patterns: []const []const u8,
    /// Maximum file size to process (bytes)
    max_file_size: u64 = 10 * 1024 * 1024, // 10MB default
    /// Whether to follow symbolic links
    follow_symlinks: bool = false,

    pub fn init(allocator: std.mem.Allocator, repository_path: []const u8) !GitSourceConfig {
        var include_patterns = std.array_list.Managed([]const u8).init(allocator);
        try include_patterns.append(try allocator.dupe(u8, "**/*.zig"));
        try include_patterns.append(try allocator.dupe(u8, "**/*.md"));
        try include_patterns.append(try allocator.dupe(u8, "**/*.txt"));
        try include_patterns.append(try allocator.dupe(u8, "**/*.json"));
        try include_patterns.append(try allocator.dupe(u8, "**/*.toml"));
        try include_patterns.append(try allocator.dupe(u8, "**/*.yaml"));
        try include_patterns.append(try allocator.dupe(u8, "**/*.yml"));

        var exclude_patterns = std.array_list.Managed([]const u8).init(allocator);
        try exclude_patterns.append(try allocator.dupe(u8, ".git/*"));
        try exclude_patterns.append(try allocator.dupe(u8, "*.bin"));
        try exclude_patterns.append(try allocator.dupe(u8, "*.exe"));
        try exclude_patterns.append(try allocator.dupe(u8, "*.so"));
        try exclude_patterns.append(try allocator.dupe(u8, "*.dylib"));
        try exclude_patterns.append(try allocator.dupe(u8, "*.dll"));
        try exclude_patterns.append(try allocator.dupe(u8, "zig-cache/*"));
        try exclude_patterns.append(try allocator.dupe(u8, "zig-out/*"));

        return GitSourceConfig{
            .repository_path = try allocator.dupe(u8, repository_path),
            .include_patterns = try include_patterns.toOwnedSlice(),
            .exclude_patterns = try exclude_patterns.toOwnedSlice(),
        };
    }

    pub fn deinit(self: *GitSourceConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.repository_path);
        for (self.include_patterns) |pattern| {
            allocator.free(pattern);
        }
        allocator.free(self.include_patterns);
        for (self.exclude_patterns) |pattern| {
            allocator.free(pattern);
        }
        allocator.free(self.exclude_patterns);
        if (self.ref) |ref| {
            allocator.free(ref);
        }
    }
};

/// Git repository metadata
pub const GitMetadata = struct {
    /// Fixed buffer for commit hash (40 chars + null terminator + padding)
    commit_hash_buf: [64]u8,
    commit_hash_len: usize,
    /// Fixed buffer for branch name (most branch names are short)
    branch_buf: [256]u8,
    branch_len: usize,
    /// Repository root path (allocated since it can be long)
    repository_root: []const u8,
    /// When the repository was last scanned
    scan_timestamp_ns: u64,

    pub fn commit_hash(self: *const GitMetadata) []const u8 {
        return self.commit_hash_buf[0..self.commit_hash_len];
    }

    pub fn branch(self: *const GitMetadata) []const u8 {
        return self.branch_buf[0..self.branch_len];
    }

    fn update_commit_hash(self: *GitMetadata, hash: []const u8) void {
        const len = @min(hash.len, self.commit_hash_buf.len - 1);
        @memcpy(self.commit_hash_buf[0..len], hash[0..len]);
        self.commit_hash_len = len;
    }

    fn update_branch(self: *GitMetadata, branch_name: []const u8) void {
        const len = @min(branch_name.len, self.branch_buf.len - 1);
        @memcpy(self.branch_buf[0..len], branch_name[0..len]);
        self.branch_len = len;
    }

    pub fn deinit(self: *GitMetadata, allocator: std.mem.Allocator) void {
        allocator.free(self.repository_root);
    }
};

/// File information from Git repository
pub const GitFileInfo = struct {
    /// Relative path from repository root
    relative_path: []const u8,
    /// File content
    content: []const u8,
    /// File size in bytes
    size: u64,
    /// Last modified timestamp
    modified_time_ns: u64,
    /// Content type based on file extension
    content_type: []const u8,

    pub fn deinit(self: *GitFileInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.relative_path);
        allocator.free(self.content);
        allocator.free(self.content_type);
    }
};

/// Iterator over files from a Git repository
pub const GitSourceIterator = struct {
    /// Reference to parent GitSource for metadata
    git_source: *GitSource,
    /// Pre-discovered list of matching files
    files: []GitFileInfo,
    /// Current index in the files array
    current_index: usize,
    /// Base allocator for cleanup
    allocator: std.mem.Allocator,

    pub fn init(git_source: *GitSource, files: []GitFileInfo, allocator: std.mem.Allocator) GitSourceIterator {
        return GitSourceIterator{
            .git_source = git_source,
            .files = files,
            .current_index = 0,
            .allocator = allocator,
        };
    }

    /// Get the next SourceContent item, or null if finished
    pub fn next(self: *GitSourceIterator, allocator: std.mem.Allocator) IngestionError!?SourceContent {
        if (self.current_index >= self.files.len) {
            return null; // Iterator exhausted
        }

        const file = &self.files[self.current_index];
        self.current_index += 1;

        var metadata_map = std.StringHashMap([]const u8).init(allocator);
        try metadata_map.put("repository_path", try allocator.dupe(u8, self.git_source.config.repository_path));
        try metadata_map.put("file_path", try allocator.dupe(u8, file.relative_path));
        try metadata_map.put("file_size", try std.fmt.allocPrint(allocator, "{d}", .{file.size}));

        if (self.git_source.metadata) |meta| {
            try metadata_map.put("commit_hash", try allocator.dupe(u8, meta.commit_hash()));
            try metadata_map.put("branch", try allocator.dupe(u8, meta.branch()));
        }

        return SourceContent{
            .data = try allocator.dupe(u8, file.content),
            .content_type = try allocator.dupe(u8, file.content_type),
            .metadata = metadata_map,
            .timestamp_ns = file.modified_time_ns,
        };
    }

    pub fn deinit(self: *GitSourceIterator, allocator: std.mem.Allocator) void {
        _ = allocator; // Iterator doesn't own the allocator, files are owned by GitSource
        for (self.files) |*file| {
            file.deinit(self.allocator);
        }
        self.allocator.free(self.files);
    }

    fn next_impl(ptr: *anyopaque, allocator: std.mem.Allocator) IngestionError!?SourceContent {
        const self = @as(*GitSourceIterator, @ptrCast(@alignCast(ptr)));
        return self.next(allocator);
    }

    fn deinit_impl(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self = @as(*GitSourceIterator, @ptrCast(@alignCast(ptr)));
        self.deinit(allocator);
        allocator.destroy(self); // Free the iterator instance itself
    }

    pub fn as_source_iterator(self: *GitSourceIterator) SourceIterator {
        return SourceIterator{
            .ptr = self,
            .vtable = &.{
                .next = next_impl,
                .deinit = deinit_impl,
            },
        };
    }
};

/// Git repository source implementation
pub const GitSource = struct {
    /// Source configuration
    config: GitSourceConfig,
    /// Repository metadata
    metadata: ?GitMetadata = null,
    /// Allocator for runtime allocations
    allocator: std.mem.Allocator,

    /// Initialize Git source with configuration
    pub fn init(allocator: std.mem.Allocator, config: GitSourceConfig) GitSource {
        return GitSource{
            .config = config,
            .allocator = allocator,
        };
    }

    /// Clean up Git source resources
    pub fn deinit(self: *GitSource, allocator: std.mem.Allocator) void {
        if (self.metadata) |*metadata| {
            metadata.deinit(allocator);
        }
        self.config.deinit(allocator);
    }

    /// Create Source interface wrapper
    pub fn source(self: *GitSource) Source {
        return Source{
            .ptr = self,
            .vtable = &.{
                .fetch = fetch_impl,
                .describe = describe_impl,
                .deinit = deinit_impl,
            },
        };
    }

    /// Fetch content from the Git repository
    fn fetch_iterator(self: *GitSource, allocator: std.mem.Allocator, file_system: *VFS) IngestionError!SourceIterator {
        concurrency.assert_main_thread();

        const repo_stat = file_system.stat(self.config.repository_path) catch |err| {
            error_context.log_ingestion_error(err, error_context.repository_context(
                "stat_repository",
                self.config.repository_path,
            ));
            return IngestionError.SourceFetchFailed;
        };
        if (!repo_stat.is_directory) {
            error_context.log_ingestion_error(IngestionError.SourceFetchFailed, error_context.repository_context(
                "validate_repository_directory",
                self.config.repository_path,
            ));
            return IngestionError.SourceFetchFailed;
        }

        try self.scan_repository_metadata(self.allocator, file_system);

        const files = try self.find_matching_files(allocator, file_system);

        // Note: We still do the file discovery up front, but yield files one by one
        // discover file paths. Future optimization could make discovery lazy too.
        var iterator = try allocator.create(GitSourceIterator);
        iterator.* = GitSourceIterator.init(self, files, allocator);

        return iterator.as_source_iterator();
    }

    /// Scan repository for Git metadata
    fn scan_repository_metadata(self: *GitSource, allocator: std.mem.Allocator, file_system: *VFS) !void {
        const git_dir = try std.fs.path.join(allocator, &.{ self.config.repository_path, ".git" });
        defer allocator.free(git_dir);

        const git_stat = file_system.stat(git_dir) catch null;
        const git_exists = if (git_stat) |stat| stat.is_directory else false;
        if (!git_exists) {
            var metadata = GitMetadata{
                .commit_hash_buf = undefined,
                .commit_hash_len = 0,
                .branch_buf = undefined,
                .branch_len = 0,
                .repository_root = try allocator.dupe(u8, self.config.repository_path),
                .scan_timestamp_ns = @intCast(std.time.nanoTimestamp()),
            };
            metadata.update_commit_hash("unknown");
            metadata.update_branch("unknown");
            self.metadata = metadata;
            return;
        }

        const head_path = try std.fs.path.join(allocator, &.{ git_dir, "HEAD" });
        defer allocator.free(head_path);

        var metadata = GitMetadata{
            .commit_hash_buf = undefined,
            .commit_hash_len = 0,
            .branch_buf = undefined,
            .branch_len = 0,
            .repository_root = try allocator.dupe(u8, self.config.repository_path),
            .scan_timestamp_ns = @intCast(std.time.nanoTimestamp()),
        };
        metadata.update_commit_hash("unknown");
        metadata.update_branch("unknown");

        if (file_system.read_file_alloc(allocator, head_path, 1024)) |head_content| {
            defer allocator.free(head_content);

            const trimmed = std.mem.trim(u8, head_content, " \t\n\r");
            if (std.mem.startsWith(u8, trimmed, "ref: refs/heads/")) {
                metadata.update_branch(trimmed[16..]);

                const branch_ref_path = try std.fs.path.join(allocator, &.{ git_dir, "refs", "heads", metadata.branch() });
                defer allocator.free(branch_ref_path);

                if (file_system.read_file_alloc(allocator, branch_ref_path, 64)) |ref_content| {
                    defer allocator.free(ref_content);
                    metadata.update_commit_hash(std.mem.trim(u8, ref_content, " \t\n\r"));
                } else |_| {
                    metadata.update_commit_hash("unknown");
                }
            } else if (trimmed.len >= 40) {
                metadata.update_commit_hash(trimmed[0..40]);
                metadata.update_branch("detached");
            }
        } else |_| {
            // Can't read HEAD file, keep fallback values
        }

        self.metadata = metadata;
    }

    /// Find all files matching the configured patterns
    fn find_matching_files(self: *GitSource, allocator: std.mem.Allocator, file_system: *VFS) ![]GitFileInfo {
        var files = std.array_list.Managed(GitFileInfo).init(allocator);
        try self.scan_directory_recursive(allocator, file_system, self.config.repository_path, "", &files);

        const file_slice = try files.toOwnedSlice();
        std.sort.block(GitFileInfo, file_slice, {}, struct {
            fn less_than(context: void, a: GitFileInfo, b: GitFileInfo) bool {
                _ = context;
                return std.mem.lessThan(u8, a.relative_path, b.relative_path);
            }
        }.less_than);

        return file_slice;
    }

    /// Recursively scan directory for matching files
    fn scan_directory_recursive(
        self: *GitSource,
        allocator: std.mem.Allocator,
        file_system: *VFS,
        base_path: []const u8,
        relative_path: []const u8,
        files: *std.array_list.Managed(GitFileInfo),
    ) !void {
        const full_path = if (relative_path.len == 0)
            try allocator.dupe(u8, base_path)
        else
            try std.fs.path.join(allocator, &.{ base_path, relative_path });
        defer allocator.free(full_path);

        var dir_iterator = file_system.iterate_directory(full_path, allocator) catch {
            return;
        };
        defer dir_iterator.deinit(allocator);

        while (dir_iterator.next()) |entry| {
            const entry_name = entry.name;
            const entry_relative = if (relative_path.len == 0)
                try allocator.dupe(u8, entry_name)
            else
                try std.fs.path.join(allocator, &.{ relative_path, entry_name });
            defer allocator.free(entry_relative);

            if (self.is_excluded(entry_relative)) {
                continue;
            }

            switch (entry.kind) {
                .directory => {
                    try self.scan_directory_recursive(allocator, file_system, base_path, entry_relative, files);
                },
                .file => {
                    if (self.is_included(entry_relative)) {
                        const file_info = self.load_file_info(allocator, file_system, base_path, entry_relative) catch |err| {
                            error_context.log_ingestion_error(err, error_context.ingestion_file_context(
                                "load_file_info",
                                self.config.repository_path,
                                entry_relative,
                                null,
                            ));
                            continue;
                        };
                        try files.append(file_info);
                    }
                },
                else => {}, // Skip other types (symlinks, etc.)
            }
        }
    }

    /// Check if file path matches include patterns using robust glob matching
    fn is_included(self: *GitSource, file_path: []const u8) bool {
        for (self.config.include_patterns) |pattern| {
            if (glob_matcher.matches_pattern(pattern, file_path)) {
                return true;
            }
        }
        return false;
    }

    /// Check if file path matches exclude patterns using robust glob matching
    fn is_excluded(self: *GitSource, file_path: []const u8) bool {
        for (self.config.exclude_patterns) |pattern| {
            if (glob_matcher.matches_pattern(pattern, file_path)) {
                return true;
            }
        }
        return false;
    }

    /// Load file information and content
    fn load_file_info(
        self: *GitSource,
        allocator: std.mem.Allocator,
        file_system: *VFS,
        base_path: []const u8,
        relative_path: []const u8,
    ) !GitFileInfo {
        const full_path = try std.fs.path.join(allocator, &.{ base_path, relative_path });
        defer allocator.free(full_path);

        const stat = try file_system.stat(full_path);
        if (stat.size > self.config.max_file_size) {
            error_context.log_ingestion_error(IngestionError.SourceFetchFailed, error_context.file_size_context(
                "validate_file_size",
                full_path,
                stat.size,
                self.config.max_file_size,
            ));
            return IngestionError.SourceFetchFailed;
        }

        const content = try file_system.read_file_alloc(allocator, full_path, @intCast(stat.size));
        const content_type = detect_content_type(relative_path);

        return GitFileInfo{
            .relative_path = try allocator.dupe(u8, relative_path),
            .content = content,
            .size = stat.size,
            .modified_time_ns = @intCast(stat.modified_time),
            .content_type = try allocator.dupe(u8, content_type),
        };
    }

    fn fetch_impl(ptr: *anyopaque, allocator: std.mem.Allocator, file_system: *VFS) IngestionError!SourceIterator {
        const self: *GitSource = @ptrCast(@alignCast(ptr));
        return self.fetch_iterator(allocator, file_system);
    }

    fn describe_impl(ptr: *anyopaque) []const u8 {
        const self: *GitSource = @ptrCast(@alignCast(ptr));
        return self.config.repository_path;
    }

    fn deinit_impl(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *GitSource = @ptrCast(@alignCast(ptr));
        self.deinit(allocator);
    }
};

/// Detect content type from file extension
fn detect_content_type(file_path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, file_path, ".zig")) return "text/zig";
    if (std.mem.endsWith(u8, file_path, ".md")) return "text/markdown";
    if (std.mem.endsWith(u8, file_path, ".txt")) return "text/plain";
    if (std.mem.endsWith(u8, file_path, ".json")) return "application/json";
    if (std.mem.endsWith(u8, file_path, ".toml")) return "application/toml";
    if (std.mem.endsWith(u8, file_path, ".yaml") or std.mem.endsWith(u8, file_path, ".yml")) return "application/yaml";
    if (std.mem.endsWith(u8, file_path, ".c") or std.mem.endsWith(u8, file_path, ".h")) return "text/c";
    if (std.mem.endsWith(u8, file_path, ".cpp") or std.mem.endsWith(u8, file_path, ".hpp")) return "text/cpp";
    if (std.mem.endsWith(u8, file_path, ".rs")) return "text/rust";
    if (std.mem.endsWith(u8, file_path, ".py")) return "text/python";
    if (std.mem.endsWith(u8, file_path, ".js")) return "text/javascript";
    if (std.mem.endsWith(u8, file_path, ".ts")) return "text/typescript";
    return "text/plain";
}

test "git source creation and cleanup" {
    const allocator = testing.allocator;

    const config = try GitSourceConfig.init(allocator, "/test/repo");

    var git_source = GitSource.init(allocator, config);
    defer git_source.deinit(allocator);

    const source = git_source.source();
    try testing.expectEqualStrings("/test/repo", source.describe());
}

test "pattern matching" {
    try testing.expect(glob_matcher.matches_pattern("file.zig", "file.zig"));
    try testing.expect(!glob_matcher.matches_pattern("other.zig", "file.zig"));

    try testing.expect(!glob_matcher.matches_pattern("*.zig", "src/main.zig"));
    try testing.expect(!glob_matcher.matches_pattern("*.zig", "src/main.rs"));

    try testing.expect(glob_matcher.matches_pattern(".git/*", ".git/config"));
    try testing.expect(!glob_matcher.matches_pattern(".git/*", "src/.git"));

    try testing.expect(glob_matcher.matches_pattern("src/**/*.zig", "src/parser/lexer.zig"));
    try testing.expect(glob_matcher.matches_pattern("test[0-9].zig", "test1.zig"));
    try testing.expect(!glob_matcher.matches_pattern("src/**/*.zig", "tests/main.zig"));
}

test "content type detection" {
    try testing.expectEqualStrings("text/zig", detect_content_type("main.zig"));
    try testing.expectEqualStrings("text/markdown", detect_content_type("README.md"));
    try testing.expectEqualStrings("application/json", detect_content_type("package.json"));
    try testing.expectEqualStrings("text/plain", detect_content_type("unknown.ext"));
}
