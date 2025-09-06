# set env (same)
# notebooks/04_plots.py
import os
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

# Setup environment
os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]

# Start Spark
spark = SparkSession.builder.master("local[*]").appName("Plots").getOrCreate()

# Load cleaned parquet
df = spark.read.parquet(r"E:\Sales_Analyzer\spark_output\clean_online_retail_parquet")
df.createOrReplaceTempView("retail")

# Monthly revenue plot
monthly = spark.sql("""
    SELECT year, month, SUM(Revenue) AS revenue
    FROM retail
    GROUP BY year, month
    ORDER BY year, month
""").toPandas()
monthly['ym'] = monthly.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)

plt.figure(figsize=(10,5))
sns.lineplot(data=monthly, x='ym', y='revenue', marker='o')
plt.xticks(rotation=45)
plt.title("Monthly Revenue")
plt.tight_layout()
plt.savefig("docs/monthly_revenue.png", dpi=150)
plt.close()

print("Plots saved to docs/")

spark.stop()
