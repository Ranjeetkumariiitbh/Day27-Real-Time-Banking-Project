# Running on YARN

The application is packaged as a Spark application and can be submitted to a Hadoop YARN cluster.

Example:

```bash
sbt package

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.day27.banking.BankingStreamingApp \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  target/scala-2.12/day27-real-time-banking-project_2.12-1.0.jar \
  <stream-host> 9999 <branch-path> <risk-path>
```

## YARN flow

1. `spark-submit --master yarn` sends the application to the YARN ResourceManager.
2. The Spark driver runs in the selected deploy mode.
3. YARN allocates executor containers.
4. Executors process micro-batches in parallel.
5. Partitioned account keys reduce unnecessary data movement.
6. Small branch/risk reference data is broadcast to executors.
7. Checkpointing stores streaming state required by window operations.

For production, replace the demo TCP source with Kafka or another durable streaming source and store checkpoints on reliable distributed storage such as HDFS.
