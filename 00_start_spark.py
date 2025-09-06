# notebooks/00_start_spark.py
# notebooks/00_start_spark.py
import os
from pyspark.sql import SparkSession

# Setup environment
os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
os.environ["SPARK_LOCAL_DIRS"] = "E:/Sales_Analyzer/tmp"

# Start Spark session
spark = SparkSession.builder \
    .appName("SalesAnalyzer") \
    .config("spark.hadoop.io.nativeio.enable", "false") \
    .getOrCreate()

print("Spark session started.")

spark.stop()
