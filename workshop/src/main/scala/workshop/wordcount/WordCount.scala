package workshop.wordcount

import java.time.Clock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.appName("Spark Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = if(!args.isEmpty) args(0) else conf.getString("apps.WordCount.input")
    val outputPath = if(args.length > 1) args(1) else conf.getString("apps.WordCount.output")

    run(spark, inputPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    log.info("Reading data: " + inputPath)
    log.info("Writing data: " + outputPath)

    import spark.implicits._
    spark.read
      .text(inputPath)  // Read file
      .as[String] // As a data set
      .write
      .option("quoteAll", false)
      .option("quote", " ")
      .csv(outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
  }
}
