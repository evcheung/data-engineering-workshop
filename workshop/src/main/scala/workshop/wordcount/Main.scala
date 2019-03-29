package workshop.wordcount

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)

    val spark = SparkSession.builder
      .appName("Spark Word Count")
      .config("spark.driver.host","127.0.0.1")
      .master("local")
      .getOrCreate()

    log.info("Application Initialized: " + spark.sparkContext.appName)

    run(spark, "/tmp/data.txt", "/tmp/word-count.csv")

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
