import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. إعدادات البيئة لضمان التوافق مع ويندوز
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. إعداد جلسة السبارك (مع تصحيح الإصدارات لـ Spark 3.5)
spark = SparkSession.builder \
    .appName("RetailInventoryAnalyticsFinal") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/inventory_db.alerts") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 3. إعداد منطق الـ Bloom Filter (عروض الخصم) ---
# محاكاة للفلتر: المنتجات من 1 إلى 50 هي المشمولة بالعرض
flash_sale_ids = [str(i) for i in range(1, 51)]

@udf(returnType=StringType())
def check_promo_eligibility(item_id):
    if item_id in flash_sale_ids:
        return "🔥 FLASH SALE - 20% OFF"
    return "Regular Price"

# --- 4. تعريف الـ Schema (مطابق لبيانات الـ Producer) ---
schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("reported_stock", IntegerType(), True)
])

# --- 5. استقبال البيانات من Kafka ---
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "first-topic") \
    .option("startingOffsets", "latest") \
    .load()

# --- 6. المعالجة و الـ Watermarking ---
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "10 minutes")

# --- 7. تطبيق اللوجيك التحليلي الشامل (نفس منطق السكالا) ---
REORDER_POINT = 50

clean_final_df = processed_df \
    .withColumn("Offer", check_promo_eligibility(col("item_id"))) \
    .withColumn("Inventory_State",
                when(col("reported_stock") < REORDER_POINT, "⚠️ Low Stock - REORDER NOW")
                .when(col("reported_stock") > 1000, "📢 OVERSTOCK")
                .otherwise("Normal")) \
    .withColumn("Behavior_Analysis",
                when((col("event_type") == "SALE") & (col("reported_stock") < 10), "🚨 Critical Anomalies")
                .otherwise("Normal")) \
    .withColumn("Suggested_Order_Qty",
                when(col("reported_stock") < REORDER_POINT, lit(100))
                .otherwise(lit(0))) \
    .select(
    col("item_id").alias("_id"),             # جعل رقم المنتج هو المعرف الأساسي في MongoDB
    col("item_name").alias("Product"),
    col("event_type").alias("Type"),
    col("reported_stock").alias("Stock"),
    col("Offer"),
    col("Inventory_State"),
    col("Behavior_Analysis"),
    col("Suggested_Order_Qty")
)

# --- 8. دالة الحفظ لـ MongoDB ---
def write_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", "inventory_db") \
        .option("collection", "alerts") \
        .mode("append") \
        .save()

# --- 9. تشغيل البث (الكونسول والمونجو) ---
# ملاحظة: استخدمنا checkpointLocation مختلف لكل استعلام لضمان عدم التداخل
query_mongo = clean_final_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "C:/bigdata/checkpoints/py_mongo_vfinal") \
    .start()

query_console = clean_final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "C:/bigdata/checkpoints/py_console_vfinal") \
    .start()

# الانتظار حتى انتهاء جميع العمليات
spark.streams.awaitAnyTermination()