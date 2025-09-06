# notebooks/02_clean.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, year, month, dayofmonth, hour

# ----------------------------
# Setup environment
# ----------------------------
os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
os.environ["SPARK_LOCAL_DIRS"] = "E:/Sales_Analyzer/tmp"

# ----------------------------
# Start Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("CleanOnlineRetail") \
    .config("spark.hadoop.io.nativeio.enable", "false") \
    .getOrCreate()

# ----------------------------
# Load dataset
# ----------------------------
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(r"E:\Sales_Analyzer\data\online_retail_II.csv")

print("Raw data schema:")
df.printSchema()

# ----------------------------
# Cleaning & Transformation
# ----------------------------
# Trim descriptions
df = df.withColumn("Description", trim(col("Description")))

# Cast numeric columns
df = df.withColumn("Quantity", col("Quantity").cast("double")) \
       .withColumn("Price", col("Price").cast("double"))

# Cast InvoiceDate to proper timestamp (important for year/month extraction)
df = df.withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))

# Filter out invalid records
clean = df.filter(
    (col("Quantity") > 0) & (~col("Invoice").startswith("C"))
)

# Ensure Customer ID is not null
clean = clean.filter(col("Customer ID").isNotNull())

# Add derived fields
clean = clean.withColumn("Revenue", col("Quantity") * col("Price")) \
             .withColumn("year", year(col("InvoiceDate"))) \
             .withColumn("month", month(col("InvoiceDate"))) \
             .withColumn("day", dayofmonth(col("InvoiceDate"))) \
             .withColumn("hour", hour(col("InvoiceDate")))

# ----------------------------
# Save cleaned dataset
# ----------------------------
output_path = r"E:\Sales_Analyzer\spark_output\clean_online_retail_parquet"

clean.write.mode("overwrite").parquet(output_path)

print(f"Cleaned data saved to: {output_path}")
print("Preview of cleaned data:")
clean.show(5, truncate=False)

# ----------------------------
# Stop Spark
# ----------------------------
spark.stop()
