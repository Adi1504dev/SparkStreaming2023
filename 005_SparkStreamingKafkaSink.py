from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# By Default schema inference is false for streaming data sources --> although we could set it to true(.config("spark.sql.streaming.schemaInference","true"))
# we can set configuration in spark session to download dependencies of kafka but it is not recommended I have set is in sparkDefault.conf
# .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Working on Kafka data Source and sink") \
        .master("local[3]") \
        .config("spark.streaming,stopGracefullyOnShutDown", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    #To show and work on dev u can set readStream to read and do show
    raw_df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.Servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    raw_df.printSchema()

    value_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
        .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

    # kafka_target_df = notification_df.selectExpr("InvoiceNumber as key", "to_json(struct(*)) as value")

    kafka_target_df = notification_df.selectExpr("InvoiceNumber as key",
                                                 """to_json(named_struct(
                                                 'CustomerCardNo', CustomerCardNo,
                                                 'TotalAmount', TotalAmount,
                                                 'EarnedLoyaltyPoints', TotalAmount * 0.2)) as value""")
    #kafka_target_df.show() #can be used with read while development
    '''
    notification_writer_query = kafkaTarget_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()
    '''

    notification_writer_query = kafka_target_df \
        .writeStream \
        .queryName("Notification Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "notifications") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir_005") \
        .start()

    notification_writer_query.awaitTermination()
