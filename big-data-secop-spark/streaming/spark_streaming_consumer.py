# -*- coding: utf-8 -*-
"""Consumidor con Spark Streaming: lee el topic sensor_data y calcula
temperatura y humedad promedio por sensor en ventanas de 1 minuto."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# El productor envía el timestamp como número (segundos): se lee como Long y se convierte
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType()),
])

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor_data")
      .load())

parsed_df = (df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
               .select("data.*")
               .withColumn("timestamp", F.col("timestamp").cast("timestamp")))

windowed_stats = (parsed_df
                  .groupBy(F.window(F.col("timestamp"), "1 minute"), "sensor_id")
                  .agg(F.avg("temperature").alias("temp_promedio"),
                       F.avg("humidity").alias("humedad_promedio"),
                       F.count("*").alias("eventos")))

query = (windowed_stats.writeStream
         .outputMode("complete")
         .format("console")
         .option("truncate", "false")
         .start())

query.awaitTermination()
