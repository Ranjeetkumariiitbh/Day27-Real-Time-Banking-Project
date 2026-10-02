# Day 27 Architecture

```text
                    +--------------------------+
                    | TCP Transaction Source   |
                    | CSV lines / socket:9999  |
                    +------------+-------------+
                                 |
                                 v
                    +--------------------------+
                    | Spark Streaming Context  |
                    | 5-second micro-batches   |
                    +------------+-------------+
                                 |
                    +------------v-------------+
                    | Parse + Validate Events  |
                    | Transaction schema       |
                    +------------+-------------+
                                 |
               +-----------------+------------------+
               |                                    |
               v                                    v
    +-----------------------+             +------------------------+
    | Account Aggregation   |             | Window Burst Detection |
    | reduceByKey           |             | 30s window / 10s slide |
    +-----------+-----------+             +-----------+------------+
                |                                     |
                v                                     v
       Account totals output                 Suspicious alerts

                                 |
                                 v
                    +--------------------------+
                    | Reference Enrichment     |
                    | Branch + Risk data       |
                    | broadcast + partitioning |
                    +------------+-------------+
                                 |
                                 v
                    +--------------------------+
                    | Enriched transaction log |
                    +--------------------------+
```

## Main Spark concepts

- **Transformation:** `map`, `flatMap`, `filter`, `reduceByKey`, `reduceByKeyAndWindow`, `transform`.
- **Actions:** `collect`, `take`, `count`, plus streaming output operations.
- **Partitioning:** account and enrichment keys use `HashPartitioner(4)`.
- **Shuffle boundaries:** `reduceByKey`, `reduceByKeyAndWindow`, and `partitionBy` can create shuffle stages.
- **Persistence:** incoming parsed transactions are cached with `MEMORY_ONLY` because the same DStream is consumed by multiple branches.
- **Broadcast:** branch and risk reference datasets are broadcast because they are small and reused by all executors.
- **Window:** a 30-second window sliding every 10 seconds detects short transaction bursts.
