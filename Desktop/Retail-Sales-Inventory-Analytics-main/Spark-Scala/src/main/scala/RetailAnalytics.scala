import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

object RetailAnalyticsFinal {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RetailInventoryAnalytics")
      .master("local[*]")
      .config("spark.mongodb.database", "inventory_db")
      .config("spark.mongodb.collection", "alerts")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val bf = BloomFilter.create(5000, 0.01)
    (1 to 50).map(_.toString).foreach(id => bf.put(id))
    val broadcastFilter = spark.sparkContext.broadcast(bf)

    val checkPromoEligibility = udf((itemId: String) => {
      if (itemId != null && broadcastFilter.value.mightContain(itemId))
        "🔥 FLASH SALE - 20% OFF"
      else
        "Regular Price"
    })

    val schema = StructType(Seq(
      StructField("item_id", StringType, true),
      StructField("item_name", StringType, true),
      StructField("event_type", StringType, true),
      StructField("reported_stock", IntegerType, true)
    ))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "first-topic")
      .load()

    val processedDF = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "10 minutes")

    val REORDER_POINT = 50

    val cleanFinalDF = processedDF
      .withColumn("Offer", checkPromoEligibility(col("item_id")))
      .withColumn(
        "Inventory_State",
        when(col("reported_stock") < REORDER_POINT, "⚠️ Low Stock - REORDER NOW")
          .when(col("reported_stock") > 1000, "📢 OVERSTOCK")
          .otherwise("Normal")
      )
      .withColumn(
        "Behavior_Analysis",
        when(col("event_type") === "SALE" && col("reported_stock") < 10, "🚨 Critical Anomalies")
          .otherwise("Normal")
      )
      .withColumn(
        "Suggested_Order_Qty",
        when(col("reported_stock") < REORDER_POINT, lit(100))
          .otherwise(lit(0))
      )
      .select(
        col("item_id").as("_id"),
        col("item_name").as("Product"),
        col("event_type").as("Type"),
        col("reported_stock").as("Stock"),
        col("Offer"),
        col("Inventory_State"),
        col("Behavior_Analysis"),
        col("Suggested_Order_Qty"),
        col("timestamp")
      )

    val alertsQuery = cleanFinalDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.drop("timestamp")
          .write
          .format("mongodb")
          .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1:27017")
          .option("spark.mongodb.database", "inventory_db")
          .option("spark.mongodb.collection", "alerts")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "C:/bigdata/checkpoints/final_production_v7")
      .start()

    val dailySummaryDF = processedDF
      .withColumn("date", to_date(col("timestamp")))
      .groupBy(col("date"))
      .agg(
        count(when(col("event_type") === "SALE", true)).as("Total_Sales"),
        sum(col("reported_stock")).as("Total_Stock_Reported"),
        count(when(col("reported_stock") < REORDER_POINT, true)).as("Low_Stock_Items")
      )
      .withColumnRenamed("date", "_id")

    val dailySummaryQuery = dailySummaryDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("mongodb")
          .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1:27017")
          .option("spark.mongodb.database", "inventory_db")
          .option("spark.mongodb.collection", "daily_sales_summary")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "C:/bigdata/checkpoints/daily_summary")
      .outputMode("complete")
      .start()
    val consoleAlerts = cleanFinalDF
      .select(
        col("_id"),
        col("Product"),
        col("Stock"),
        col("Inventory_State"),
        col("Behavior_Analysis")
      )
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()


    val consoleDailySummary = dailySummaryDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
