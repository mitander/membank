# Query Engine: Technical Specification

## 1. Overview

This document specifies the architecture of the Kausal query engine. The design prioritizes extensibility and performance, utilizing a cost-based planning framework to select optimal execution strategies for different query types.

## 2. Core Components

### 2.1. Query Planning Framework (`QueryPlan`)

The `QueryPlan` is the central data structure for cost-based optimization. It is generated prior to query execution.

*   **Purpose**: To select an optimal execution strategy based on an analysis of query complexity and data distribution.
*   **Key Fields**:
    *   `query_type`: The class of query (`find_blocks`, `traversal`, etc.).
    *   `estimated_cost`: A metric representing the anticipated resource usage.
    *   `optimization_hints`: A set of flags that guide the execution path (e.g., `use_index`, `prefer_memtable`).
    *   `execution_strategy`: The final execution path selected by the planner (e.g., `direct_storage`, `index_lookup`).

### 2.2. Query Execution Context (`QueryContext`)

A `QueryContext` is instantiated for each query to track its execution and collect metrics.

*   **Purpose**: Per-query metrics collection and performance analysis.
*   **Key Fields**:
    *   `query_id`: A unique ID for tracing.
    *   `plan`: The `QueryPlan` guiding the execution.
    *   `metrics`: A struct of performance counters (`blocks_scanned`, `cache_hits`, etc.).

## 3. Optimization Strategies

### 3.1. Complexity-Based Optimization

The planner analyzes query complexity based on the number of blocks and edges in the storage engine. It selects a strategy from three tiers:

1.  **High Complexity (`>10,000` factor):** Selects a hybrid execution strategy using indexes and data prefetching.
2.  **Medium Complexity (`>1,000` factor):** Employs index lookups, caching, and specialized traversal algorithms.
3.  **Low Complexity (`<100` factor):** Prefers direct memtable access for maximum speed on small, recent datasets.

### 3.2. Workload-Adaptive Optimizations

The planner applies workload-specific hints based on query type:

*   **`find_blocks`**: Enables early termination for large queries and prefers memtable scans for queries against recent data.
*   **`traversal`**: Applies result limits and enables prefetching for traversals over complex graphs.
*   **`semantic` / `filtered`**: Favors index lookups and streaming scans for large result sets.

## 4. Caching

The architecture includes hooks for a query cache, though the implementation is part of the post-1.0 roadmap.

*   **Cache Eligibility**: Caching is deemed eligible based on query type and estimated cost. Expensive queries (`semantic`, `filtered`, `traversal`) are flagged as cacheable by the planner. `find_blocks` queries are not cached, as simple lookups are sufficiently fast.

## 5. Roadmap

### 5.1. Phase 1: Optimization Framework (COMPLETED)

*   Cost-based query planning.
*   Complexity analysis with adaptive strategies.
*   Workload-specific optimization hints.
*   Metrics and caching infrastructure.

### 5.2. Phase 2: Advanced Indexing (Post-1.0)

*   **Objective**: Implement secondary index support to accelerate semantic and filtered queries.
*   **Technologies**:
    *   Bloom filter integration for existence checks.
    *   Inverted indices for content search.

### 5.3. Phase 3: ML-Informed Optimization (Post-1.0)

*   **Objective**: Integrate query pattern recognition to inform the cost model and caching strategy.
*   **Integration Points**: The `record_query_execution` function is the designated hook for feeding execution data into a future learning model.

## 6. Design Mandates

1.  **Extensibility**: The framework is designed to accommodate new query types and optimization strategies without requiring modification of the core engine.
2.  **Performance by Design**: Simple queries must follow a zero-allocation hot path. Large result sets must be handled via streaming execution.
3.  **Observability**: All query paths are instrumented with comprehensive metrics to enable performance analysis and regression detection.
4.  **Correctness**: Query results must be deterministic and consistent regardless of the optimization path selected by the planner.
