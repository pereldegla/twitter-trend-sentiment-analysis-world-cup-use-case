confluentApiKey = ""
confluentSecret = ""
host = ""

from pyspark.sql.types import StringType, LongType, StructField, StructType, ArrayType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp, regexp_replace, expr

streamingInputDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", host)  \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
  .option("kafka.ssl.endpoint.identification.algorithm", "https") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false") \
  .option("subscribe", "topic_0") \
  .load()

outputPath = "/stream_output/output"
checkpointPath = "/stream_output/checkpoint/" 
schema = StructType([StructField("id", StringType(), True), StructField("tweet", StringType(), True), StructField("created_at", StringType(), True)])

query = (
  streamingInputDF
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema, {"mode" : "PERMISSIVE"}).alias("data"))
    .select("data.*")
    .withColumn("created_at", to_timestamp(col("created_at"))) 
    .withColumn("id", col("id").cast('long'))    
    .dropna()
    .writeStream
    .format("delta")        # delta for conveniance 
    .queryName("earth")        # name for sql queries
    .outputMode("append")    #adding new events  
    .option("checkpointLocation", checkpointPath) 
    .trigger(processingTime='10 seconds') # adjust to be > flushed time from producer.py
    .start(outputPath)
)
