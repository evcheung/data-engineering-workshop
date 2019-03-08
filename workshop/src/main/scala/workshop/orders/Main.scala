package workshop.orders

import java.time.Clock

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .appName("Order Job").getOrCreate()

    run(spark)

    spark.stop()
  }

  def run(spark: SparkSession): Unit = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "orders")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}
