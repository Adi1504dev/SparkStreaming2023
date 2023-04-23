from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

#By Default schema inference is false for streaming data sources --> although we could set it to true(.config("spark.sql.streaming.schemaInference","true"))
# we can set configuration in spark session to download dependencies of kafka but it is not recommended I have set is in sparkDefault.conf
            # .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
if __name__=="__main__":
    spark=SparkSession\
        .builder\
        .appName("Working on Kafka data Source")\
        .master("local[3]")\
        .config("spark.streaming,stopGracefullyOnShutDown","true")\
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

    #earliest will take effect only incase of first run when spark do not have offset value in checkpoint post that it will read offset from
    #checkpoint only
    raw_df=spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.Servers","localhost:9092")\
                .option("subscribe","invoices")\
                .option("startingOffsets","earliest")\
                .load()


    value_df=raw_df.select(from_json(col("value").cast("string"),schema).alias("value"))

    # value_df.printSchema()

    explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
                                     "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType",
                                     "value.DeliveryAddress.City",
                                     "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode",
                                     "explode(value.InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")


    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "output_004") \
        .option("checkpointLocation", "chk-point-dir_004") \
        .trigger(processingTime="1 second") \
        .start()

    invoice_writer_query.awaitTermination()


