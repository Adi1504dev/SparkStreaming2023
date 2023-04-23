from pyspark.sql import *
from pyspark.sql.functions import *


#By Default schema inference is false for streaming data sources --> although we could set it to true(.config("spark.sql.streaming.schemaInference","true"))
if __name__=="__main__":
    spark=SparkSession\
        .builder\
        .appName("Working on file data Source")\
        .master("local[3]")\
        .config("spark.streaming,stopGracefullyOnShutDown","true")\
        .config("spark.sql.shuffle.partitions", 2)\
        .config("spark.sql.streaming.schemaInference","true")\
        .getOrCreate()

    # Max file per trigger allows you to configure number of files that would be taken up per trigger--> To get even workload
    # cleanSource ==> takes 2 option archive and delete (archive does not works on window)---> Will add some overhead--> will also clean source dir which is imp to keep out job run fast
    #                 if you have to provide very low latency then have a separate cleaning job to clean source dir daily
    # for archiving u need to setup archiving directory 'sourceArchiveDir' -->should be set
    raw_df=spark.readStream \
        .format("json") \
        .option("path","data_003") \
        .option("maxFilesPerTrigger", "1") \
        .option("cleanSource", "delete") \
        .load()
    #raw_df.printSchema()

    explode_df=raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                   "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                   "DeliveryAddress.State",
                                   "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    #Note: Time differance between 1st and second microbatch is not 1 min as spark tries to round off time
    invoice_write_query=flattened_df.writeStream \
                        .format("json") \
                        .option("path","output_003") \
                        .option("checkpointLocation","chek-point-dir_003")\
                        .outputMode("append")\
                        .queryName("Flattened invoice Writer") \
                        .trigger(processingTime="1 minute")\
                        .start()

    invoice_write_query.awaitTermination()
    