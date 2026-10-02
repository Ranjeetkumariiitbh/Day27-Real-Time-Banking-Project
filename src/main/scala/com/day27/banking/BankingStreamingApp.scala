package com.day27.banking

import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BankingStreamingApp {

  private val DefaultHost = "localhost"
  private val DefaultPort = 9999
  private val BatchSeconds = 5
  private val WindowSeconds = 30
  private val SlideSeconds = 10
  private val BurstCountThreshold = 3
  private val BurstAmountThreshold = 100000.0

  def main(args: Array[String]): Unit = {
    val host = if (args.nonEmpty) args(0) else DefaultHost
    val port = if (args.length > 1) args(1).toInt else DefaultPort
    val branchPath = if (args.length > 2) args(2) else "data/branches.csv"
    val riskPath = if (args.length > 3) args(3) else "data/risk_accounts.csv"

    val conf = new SparkConf()
      .setAppName("Day27-Real-Time-Banking-Project")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val branchMap = loadBranches(sc, branchPath).collectAsMap()
    val riskMap = loadRiskReferences(sc, riskPath).collectAsMap()

    // Small reference datasets are broadcast so every streaming task can enrich records
    // without repeatedly shuffling the reference data.
    val branchBc = sc.broadcast(branchMap)
    val riskBc = sc.broadcast(riskMap)

    val ssc = new StreamingContext(sc, Seconds(BatchSeconds))
    ssc.checkpoint("checkpoint/day27")

    val raw = ssc.socketTextStream(host, port)
      .filter(_.trim.nonEmpty)

    val transactions = raw
      .flatMap(BankingModels.parseTransaction)
      .persist(StorageLevel.MEMORY_ONLY)

    // 1) Aggregate transactions by account in every micro-batch.
    val accountTotals = transactions
     .map(t => (t.accountId, (t.amount, 1L)))
     .reduceByKey(
       (a: (Double, Long), b: (Double, Long)) =>
        (a._1 + b._1, a._2 + b._2),
       4
     )

    accountTotals.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty()) {
        println(s"\\n===== ACCOUNT AGGREGATION @ ${time.milliseconds} =====")
        rdd.sortByKey().collect().foreach { case (account, (amount, count)) =>
          println(f"account=$account%-10s transactions=$count%-4d totalAmount=$amount%.2f")
        }
      }
    }

    // 2) Detect suspicious bursts over a 30-second window sliding every 10 seconds.
    val accountEvents = transactions.map(t => (t.accountId, t))
    val burstWindow = accountEvents.reduceByKeyAndWindow(
      (a: Transaction, b: Transaction) => Transaction(
        a.transactionId,
        a.accountId,
        a.branchId,
        a.amount + b.amount,
        a.transactionType,
        math.max(a.eventTime, b.eventTime)
      ),
      Seconds(WindowSeconds),
      Seconds(SlideSeconds)
    )

    val burstCount = accountEvents
      .mapValues(_ => 1L)
      .reduceByKeyAndWindow(
        (a: Long, b: Long) => a + b,
        Seconds(WindowSeconds),
        Seconds(SlideSeconds)
      )

    burstWindow.join(burstCount).filter {
      case (_, (tx, count)) =>
        count >= BurstCountThreshold || tx.amount >= BurstAmountThreshold
    }.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty()) {
        println(s"\\n===== SUSPICIOUS BURSTS @ ${time.milliseconds} =====")
        rdd.collect().foreach { case (account, (tx, count)) =>
          println(
            f"ALERT account=$account%-10s windowCount=$count%-4d " +
            f"windowAmount=${tx.amount}%.2f branch=${tx.branchId}"
          )
        }
      }
    }

    // 3) Enrich each transaction with branch + risk reference data.
    // transform() gives access to the underlying RDD, where normal RDD joins can be used.
    transactions.transform { rdd =>
      val partitioner = new HashPartitioner(4)

      val branchPairs = rdd
        .map(t => (t.branchId, t))
        .partitionBy(partitioner)

      val branchEnriched = branchPairs.mapPartitions { iter =>
        val b = branchBc.value
        iter.map { case (branchId, tx) =>
          val branch = b.getOrElse(
            branchId,
            Branch(branchId, "UNKNOWN", "UNKNOWN", "UNKNOWN")
          )
          (tx, branch)
        }
      }

      val riskPairs = branchEnriched
        .map { case (tx, branch) => (tx.accountId, (tx, branch)) }
        .partitionBy(partitioner)

      val enriched = riskPairs.mapPartitions { iter =>
        val r = riskBc.value
        iter.map { case (accountId, (tx, branch)) =>
          val risk = r.getOrElse(
            accountId,
            RiskReference(accountId, 0, "UNKNOWN")
          )
          (tx, branch, risk)
        }
      }

      enriched
    }.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty()) {
        println(s"\\n===== ENRICHED TRANSACTIONS @ ${time.milliseconds} =====")
        rdd.take(10).foreach { case (tx, branch, risk) =>
          println(
            f"${tx.transactionId} account=${tx.accountId} amount=${tx.amount}%.2f " +
            s"branch=${branch.branchName}/${branch.city} " +
            s"branchRisk=${branch.riskLevel} accountRisk=${risk.riskCategory}(${risk.riskScore})"
          )
        }
      }
    }

    transactions.foreachRDD { rdd =>
      println(s"Processed batch records: ${rdd.count()}")
    }

    ssc.start()
    println(s"Banking stream started. Listening on $host:$port")
    println(s"Window=${WindowSeconds}s, slide=${SlideSeconds}s, batch=${BatchSeconds}s")
    println("Press Ctrl+C to stop.")
    ssc.awaitTermination()
  }

  private def loadBranches(sc: SparkContext, path: String) = {
    sc.textFile(path)
      .filter(line => !line.toLowerCase.startsWith("branch_id"))
      .flatMap { line =>
        val p = line.split(",", -1).map(_.trim)
        if (p.length == 4) Some(p(0) -> Branch(p(0), p(1), p(2), p(3))) else None
      }
  }

  private def loadRiskReferences(sc: SparkContext, path: String) = {
    sc.textFile(path)
      .filter(line => !line.toLowerCase.startsWith("account_id"))
      .flatMap { line =>
        val p = line.split(",", -1).map(_.trim)
        try {
          if (p.length == 3) {
            Some(p(0) -> RiskReference(p(0), p(1).toInt, p(2)))
          } else None
        } catch {
          case _: NumberFormatException => None
        }
      }
  }
}
