from pyspark.sql import *
from pyspark.sql.functions import *
if __name__=="__main__":
    spark=SparkSession\
        .builder\
        .appName("word count spark application")\
        .master("local[3]")\
        .config("spark.streaming,stopGracefullyOnShutDown","true")\
        .config("spark.sql.shuffle.partitions", 2)\
        .getOrCreate()


    line_df=spark.readStream \
        .format("socket") \
        .option("host","localhost") \
        .option("port", "9999") \
        .load()

 # You can safely assume that all kind of transformations are allowed and all kind of actions are not allowed(as of now-- will be elaborated later)
 #line_df.printSchema()
    count_df=line_df.select(expr("explode(split(value,' ')) as word")) \
     .groupby("word")\
     .count()

#check point location  is used by datastream writer to store the progress information about the streaming job
    word_count_query=count_df\
     .writeStream\
     .format("console")\
     .option("checkpointLocation","chek-point-dir_002")\
     .outputMode("complete")\
     .start()

    word_count_query.awaitTermination()

#Micro Batch(time based trigger)
#Unspecified--> Tigger as soon as one batch is finished but it will wait for data once data come it starts
#Time interval--> Will wait for some mentioned time (5 min). To tigger next microbatch there should be 2 conditions -->
                  # previous microbatch should be finished and the time should be more than 5 min from start of previous micro batch.
                  # If previous microbatch took more than 5 min then next microbatch will start immediately.
#one time--> like batch processing --> process everthing available since last run and shutdown.
#Continuous --> For milli-second latency

#########################################################

#Streaming source
#1. Socket source--> learning or dev
#2. Rate source
#3. File data Source
#4. Kafka Source