package workshop.orders

import workshop.DefaultFeatureSpecWithSpark

class MainTest extends DefaultFeatureSpecWithSpark {
  feature("Orders Application") {
    scenario("Acceptance test for basic use") {
      Given("A Dataframe")

      When("Process extract fields from the raw data")

      import spark.implicits._

      val json =
        """
          |[
          |{
          | "orderId": "20190101010101",
          | "itemId": "12345",
          | "quantity": 5,
          | "price": 10.6,
          | "timestamp": "2019-03-08T11:11:27.373652"
          |}
          |]
        """.stripMargin
      val dataFrame = Seq(json).toDF("value")
      val frame = Main.process(spark, dataFrame)

      frame.columns should equal(Array("orderId", "itemId", "quantity", "price", "timestamp"))

      frame.count() should be(1)

      val row = frame.first()

      row.getAs[String]("orderId") should be("20190101010101")
      row.getAs[String]("itemId") should be("12345")
      row.getAs[Double]("quantity") should be(5)
      row.getAs[java.math.BigDecimal]("price").toString should equal("10.60")
      row.getAs[java.sql.Timestamp]("timestamp") should be(java.sql.Timestamp.valueOf("2019-03-08 11:11:27.373"))
    }
  }
}