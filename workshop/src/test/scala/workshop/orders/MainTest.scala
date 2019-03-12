package workshop.orders

import workshop.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.functions._

class MainTest extends DefaultFeatureSpecWithSpark {
  feature("Orders Application") {
    scenario("Acceptance test for basic use") {
      Given("A Dataframe")

      When("Process extract fields from the raw data")

      import spark.implicits._

      val json =
        """
          |
          |{
          | "id": 1,
          | "orderId": "20190101010101",
          | "itemId": "12345",
          | "quantity": 5,
          | "price": 10,
          | "timestamp": 1552262400000
          |}
          |
        """.stripMargin
      val dataFrame = Seq(json).toDF("value")
      val frame = Main.process(spark, dataFrame)

      frame.columns should equal(Array("id", "orderId", "itemId", "quantity", "price", "timestamp"))

      frame.count() should be(1)

      val row = frame.first()

      row.getAs[String]("orderId") should be("20190101010101")
      row.getAs[String]("itemId") should be("12345")
      row.getAs[Double]("quantity") should be(5)
      row.getAs[Integer]("price") should equal(10)
      row.getAs[java.sql.Timestamp]("timestamp") should be(java.sql.Timestamp.valueOf("2019-03-11 08:00:00.0"))
    }
  }

  feature("ToJSON") {
    scenario("Acceptance test for basic use") {
      Given("A Dataframe")

      When("parse to json with schema")

      import spark.implicits._

      val df = Seq(
        (1, 2, "2019-03-11 08:00:00.0", "2019-03-11 08:00:20.0")
      ).toDF("itemId", "count", "start", "end")
        .select($"itemId", $"count", struct($"start", $"end").alias("window"))

      val frameWithSchema = Main.toJson(spark, df)
      frameWithSchema.columns should equal(Array("value"))
    }
  }

}