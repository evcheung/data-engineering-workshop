package workshop.orders

import java.time.Clock

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.codehaus.jackson.JsonNode
import org.codehaus.jettison.json.{JSONArray, JSONObject}
import org.joda.time.DateTime

object Main {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    log.setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .appName("Orders Job").getOrCreate()

    run(spark)

    spark.stop()
  }

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "source-orders")
      .option("startingOffsets", "latest")
      .load()

    val extractedDF = process(spark, dataFrame)

    val processedDF = extractedDF
      .withWatermark("timestamp", "20 seconds")
      .groupBy(
        window(
          $"timestamp",
          "30 seconds",
          "20 seconds"),
        $"itemId"
      )
      .count()

    toJson(spark, processedDF)
      .writeStream
      .format("kafka")
      .outputMode(OutputMode.Append())
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("checkpointLocation", "hdfs://hadoop:9000/checkpointLocation")
      .option("topic", "orders")
      .start()

    processedDF
      .writeStream
      .format("console")
      .option("truncate", value = false)
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  def toJson(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    val convert = udf((itemId: Integer, count: Integer, startTime: java.sql.Timestamp, endTime: java.sql.Timestamp) => {
      val schema = new JSONObject()
        .put("type", "struct")
        .put("fields", new JSONArray()
          .put(
            new JSONObject()
              .put("type", "int64")
              .put("optional", false)
              .put("field", "itemId")
          )
          .put(
            new JSONObject()
              .put("type", "int64")
              .put("optional", false)
              .put("field", "count")
          )
          .put(
            new JSONObject()
              .put("type", "string")
              .put("optional", false)
              .put("field", "startTime")
          )
          .put(
            new JSONObject()
              .put("type", "string")
              .put("optional", false)
              .put("field", "endTime")
          )
        )
      val payload = new JSONObject()
        .put("itemId", itemId)
        .put("count", count)
        .put("startTime", startTime.toString)
        .put("endTime", startTime.toString)

      new JSONObject()
        .put("schema", schema)
        .put("payload", payload)
        .toString
    })

    df.withColumn("value", convert($"itemId", $"count", $"window.start", $"window.end"))
      .select($"value")
  }

  def process(spark: SparkSession, dataFrame: DataFrame): DataFrame = {

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("orderId", DataTypes.StringType),
      StructField("itemId", DataTypes.StringType),
      StructField("quantity", DataTypes.IntegerType),
      StructField("price", DataTypes.IntegerType),
      StructField("timestamp", DataTypes.createDecimalType(20, 0))
    ))

    import spark.implicits._

    val frame = dataFrame
      .withColumn("body", from_json($"value".cast("string"), schema))
    frame
      .select($"body.*")
      .withColumn("timestamp", ($"timestamp" / 1000).cast("timestamp"))
  }
}
