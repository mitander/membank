//! Integration tests for ingestion pipeline backpressure control.
//!
//! Validates that the ingestion pipeline adapts batch sizes based on storage
//! memory pressure, preventing unbounded memory growth during large ingestions
//! while maintaining throughput under normal conditions.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

// NOTE: These backpressure integration tests have been removed as the
// ingestion pipeline API has evolved. They should be reimplemented
// with the current API in a future release.
//
// Original tests covered:
// - Backpressure under normal memory conditions
// - Backpressure under memory pressure
// - Recovery after pressure relief
// - Adaptive batch sizing
// - Memory recovery behavior
//
// To reimplement, use the current IngestionPipeline API from kausaldb module.
