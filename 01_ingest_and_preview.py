# notebooks/01_ingest_and_preview.py
import os
from pyspark.sql import SparkSession

# Setup environment
os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
os.environ["SPARK_LOCAL_DIRS"] = "E:/Sales_Analyzer/tmp"

# Start Spark
spark = SparkSession.builder \
    .appName("IngestOnlineRetail") \
    .config("spark.hadoop.io.nativeio.enable", "false") \
    .getOrCreate()

# Load dataset (fixed filename)
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(r"E:\Sales_Analyzer\data\online_retail_II.csv")

print("Preview first 5 rows:")
df.show(5)

print("Schema:")
df.printSchema()

print(f"Total records: {df.count()}")

spark.stop()
