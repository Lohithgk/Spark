# import pyspark module
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = (
    SparkSession.builder.appName("Python Spark example")
    .config("spark.master", "local[*]")
    .getOrCreate()
)

# Load CSV data from local file system
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:/Users/user/Desktop/input/Employee1.csv")
)

# Show top 5 rows
df.show(n=5)
