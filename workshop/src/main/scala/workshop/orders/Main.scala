package workshop.orders

import java.time.Clock

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "orders")
      .option("startingOffsets", "latest")
      .load()

    process(spark, dataFrame)
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }

  def process(spark: SparkSession, dataFrame: DataFrame): DataFrame = {

    val schema = ArrayType(StructType(Seq(
      StructField("orderId", DataTypes.StringType),
      StructField("itemId", DataTypes.StringType),
      StructField("quantity", DataTypes.DoubleType),
      StructField("price", DataTypes.createDecimalType(10, 2)),
      StructField("timestamp", DataTypes.TimestampType)
    )))

    import spark.implicits._

    dataFrame
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .withColumn("values", explode(from_json($"raw_payload", schema)))
      .select($"values.*")
  }
}
