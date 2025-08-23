//! Zig language parser for KausalDB ingestion pipeline.
//!
//! Provides semantic parsing capabilities for Zig source code, extracting
//! functions, structs, constants, imports, and other language constructs
//! into structured ParsedUnits for the ingestion pipeline.
//!
//! Key features:
//! - Pattern-based parsing without compiler dependencies
//! - Cross-file dependency resolution for import relationships
//! - Configurable parsing depth (function bodies, private members, tests)
//! - Batch processing support for large codebases
//!
//! Design rationale: Uses pattern matching instead of AST walking to avoid
//! Zig compiler version dependencies while maintaining semantic accuracy.
//! Arena-based memory management ensures efficient processing of large files.

pub const ZigParser = @import("zig/parser.zig").ZigParser;
pub const ZigParserConfig = @import("zig/context.zig").ZigParserConfig;
pub const BatchParseResult = @import("zig/context.zig").BatchParseResult;
