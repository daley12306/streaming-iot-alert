from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, expr, count as _count, sum as _sum, to_timestamp, round as _round
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
import logging
from datetime import datetime

spark = (SparkSession.builder
         .appName("DefectRate-1m-ByStation")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.spark_project").setLevel(logging.ERROR)

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers","kafka:9092")
       .option("subscribe","sensors-telemetry")
       .option("startingOffsets","earliest")
       .load())

schema = StructType([
    StructField("item_id", StringType()),
    StructField("station_id", StringType()),
    StructField("state", StringType()),
    StructField("defect", BooleanType()),
    StructField("ts", StringType())
])

events = (raw.selectExpr("CAST(value AS STRING) AS json")
              .select(from_json(col("json"), schema).alias("d"))
              .select("d.*")
              .withColumn("event_time", to_timestamp("ts"))
              .filter(col("station_id").isNotNull()))

"""
Tính tỉ lệ phần trăm lỗi theo từng trạm để phát hiện ra trạm sản xuất nào có tỷ lệ lỗi >= 5%, watermark 2 phút để xử lý late data.
"""
by_station = (
    # BEGIN YOUR CODE
    ...
    # END YOUR CODE
)

def print_results(df, epoch_id):
    if df.limit(1).count() == 0:
        return
    print(f"====== Time: {datetime.now()} ======")
    df.orderBy("defect_pct", ascending=False).show(truncate=False)

q = (by_station.writeStream
     .outputMode("update")
     .foreachBatch(print_results)
     .start())

spark.streams.awaitAnyTermination()