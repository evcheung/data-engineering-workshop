package workshop.station

import java.time.Clock

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Station {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .appName("Spark Workshop Station").getOrCreate()

    run(spark, "hdfs://hadoop:9000/workshop/data")

    spark.stop()
  }

  def run(spark: SparkSession, outputPath: String): Unit = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "station_information")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .writeStream
      .outputMode("append")
      .format("csv")
      .option("path", outputPath)
      .option("checkpointLocation", "hdfs://hadoop:9000/workshop/checkpoints")
      .start()
      .awaitTermination()
  }
}
