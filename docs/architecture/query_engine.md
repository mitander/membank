# Query Engine Specification

The Query Engine is the primary interface for retrieving data from CortexDB. It is designed for low-latency retrieval of structured context suitable for consumption by Large Language Models.

## Query Language

The engine accepts queries via the binary client protocol. A query consists of a command and parameters.

### Query Types:

1.  **GetBlocksByID**
    *   **Description:** The simplest query. A direct key-value lookup.
    *   **Syntax:** `(command: GET_BLOCKS, ids: []u128)`
    *   **Execution:** A direct lookup in the LSM-Tree's index. Extremely fast.

2.  **TraversalQuery**
    *   **Description:** The core graph query. Starts from a set of nodes and follows edges.
    *   **Syntax:** `(command: TRAVERSE, starts: []u128, steps: []{edge_type: u16, direction: IN/OUT}, depth: u8)`
    *   **Execution:** A Breadth-First Search (BFS) starting from the `starts` blocks.

3.  **MetadataQuery**
    *   **Description:** Finds all blocks matching a metadata filter.
    *   **Syntax:** `(command: FILTER, filter: {key: string, op: EQ/NE, value: string})`
    *   **Execution:** Uses a secondary index on metadata fields to find matching blocks.

## Execution Flow

1.  **Parsing:** The API layer deserializes the binary command into a query struct.
2.  **Planning:** The Query Planner analyzes the query. For complex queries (e.g., a traversal followed by a filter), it creates an optimal plan to minimize data retrieval. The goal is to perform filtering as early as possible.
3.  **Execution:** The Execution Engine runs the plan.
    *   It fetches the necessary data from the Storage Engine (block headers, edges, index data).
    *   For traversals, it performs the graph search in memory, accumulating a set of resulting block IDs.
    *   It performs the final filtering on the results.
4.  **Payload Construction:**
    *   Once the final set of block IDs is determined, the engine fetches the full content for each block.
    *   It assembles the content into a structured text payload. By default, it uses a simple format with clear separators that an LLM can parse:

    ```
    --- BEGIN CONTEXT BLOCK ---
    ID: 01H8X...
    Source: git://.../src/main.zig
    [Block Content Here]
    --- END CONTEXT BLOCK ---
    ```

This structured output prevents "context drift" and allows the LLM to understand the boundaries and sources of the information provided.
