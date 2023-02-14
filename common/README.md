# Common Module @ FinBench Transaction Reference Implementation

This module is written to define the abstract class or the interface for specific implementation.

- `QueryStore` and `QueryType` are defined for DSL implementation like Neo4j, etc.
- For other SUTs that implement the benchmark with procedure API, just extend `BaseDb`.