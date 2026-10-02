# Day27-Real-Time-Banking-Project

A practical **Scala + Apache Spark DStreams** project for real-time banking transaction processing.

## What this project demonstrates

- Transaction event schema design.
- Real-time transaction ingestion through a TCP socket.
- Per-account transaction aggregation.
- Suspicious transaction-burst detection using a 30-second window with a 10-second slide.
- Joining/enriching transactions with small branch and account-risk reference data.
- `persist(MEMORY_ONLY)` for a DStream reused by multiple processing branches.
- Hash partitioning for account-based operations.
- Broadcast variables for small reference datasets.
- Spark transformations, actions, shuffle boundaries and optimization considerations.
- Checkpointing for streaming state.
- How the same application can be submitted to YARN.

## Project structure

```text
Day27-Real-Time-Banking-Project/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/main/scala/com/day27/banking/
│   ├── BankingModels.scala
│   └── BankingStreamingApp.scala
├── data/
│   ├── branches.csv
│   ├── risk_accounts.csv
│   └── sample_transactions.csv
├── scripts/
│   ├── send_transactions.py
│   ├── start_socket.sh
│   └── start_stream.sh
├── docs/
│   ├── architecture.md
│   └── yarn.md
└── README.md
```

## Prerequisites

- Java 17
- Scala 2.12
- sbt 1.10+
- Apache Spark 3.5.x
- Python 3 for the transaction sender
- `nc`/netcat for the TCP source

## 1. Start the stream

From the project root:

```bash
sbt "run localhost 9999 data/branches.csv data/risk_accounts.csv"
```

The application creates a local Spark Streaming context and waits for TCP events.

## 2. Start the socket source

In another terminal:

```bash
nc -lk 9999
```

Then send transaction lines manually, for example:

```text
T001,A1001,B001,1200.00,DEBIT,1760000001
T002,A1002,B003,45000.00,DEBIT,1760000002
```

Or use the Python sender:

```bash
python3 scripts/send_transactions.py localhost 9999 data/sample_transactions.csv
```

## 3. Expected processing

### Account aggregation

Transactions are mapped to:

```text
(account_id, (amount, count))
```

and reduced by account.

### Suspicious burst detection

The same account stream is evaluated in:

- Window: **30 seconds**
- Slide: **10 seconds**
- Alert when transaction count is at least **3**, or window amount is at least **100000**

This is intentionally a simple educational rule. A production fraud engine would use richer rules/features and a durable event source.

### Branch/risk enrichment

Each transaction is enriched with:

- branch name
- branch city
- branch risk level
- account risk score
- account risk category

Branch and risk reference datasets are small, so they are loaded once and broadcast to executors.

## Performance notes

| Area | Choice | Why |
|---|---|---|
| Persistence | `MEMORY_ONLY` | Parsed transactions are consumed by multiple downstream branches |
| Partitioning | `HashPartitioner(4)` | Keeps same account keys together for aggregations |
| Reference data | Broadcast | Avoids repeatedly shuffling small lookup datasets |
| Aggregation | `reduceByKey` | Performs map-side combining before shuffle |
| Windowing | 30s / 10s | Limits state to a bounded recent interval |
| Serialization | Kryo | Lower serialization overhead than Java serialization |
| Checkpointing | `checkpoint/day27` | Supports streaming state recovery |

## Transformation vs action

### Transformations
`flatMap`, `filter`, `map`, `mapValues`, `reduceByKey`, `reduceByKeyAndWindow`, `transform`, `partitionBy`.

### Actions / output
`collect`, `take`, `count`, and DStream output operations.

## Shuffle boundaries

Potential shuffles occur at:

- `reduceByKey`
- `reduceByKeyAndWindow`
- `partitionBy`

`map`, `filter`, and `mapPartitions` are narrow transformations.

## YARN

See [`docs/yarn.md`](docs/yarn.md) for the YARN deployment flow and `spark-submit` example.

## Learning outcome

This project ties together the Day 27 goals:

**Scala → RDD/Pair RDD → partitioning → persistence → joins/enrichment → aggregation → windows → DStreams → real-time processing → YARN.**

## Important note

Spark DStreams are used here because this exercise explicitly focuses on DStreams. For a new production system, Spark Structured Streaming is generally the preferred API.
