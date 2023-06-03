# Neo4j FinBench implementation

A Neo4j reference implementation for the [LDBC Financial Benchmark](https://ldbcouncil.org/benchmarks/finbench/).

Note: This is the draft implementation by Dr. Gabor Szarnyas with Neo4j Cypher. It is not yet a finished implementation.
It will be validated and improved in the following versions of FinBench.

# Usage

## Start the database

```bash
neo/init-and-load.sh
```

## Implementation tricks

[Simulating the truncation limit using Cypher](truncation-limit-simulation-cypher-example.md)
