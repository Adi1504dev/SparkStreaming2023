import sys
from pyspark.sql import *


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()

    spark.read.option("Header","True").csv("data/*").show()