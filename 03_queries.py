# set env (same)
# notebooks/03_queries.py
import os
from pyspark.sql import SparkSession

# Setup environment
os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
os.environ["SPARK_LOCAL_DIRS"] = "E:/Sales_Analyzer/tmp"

# Start Spark
spark = SparkSession.builder \
    .appName("AnalyzeOnlineRetail") \
    .config("spark.hadoop.io.nativeio.enable", "false") \
    .getOrCreate()

# Load cleaned parquet
df = spark.read.parquet(r"E:\Sales_Analyzer\spark_output\clean_online_retail_parquet")
df.createOrReplaceTempView("sales")

# Monthly revenue
monthly_revenue = spark.sql("""
    SELECT year, month, SUM(Revenue) AS TotalRevenue
    FROM sales
    GROUP BY year, month
    ORDER BY year, month
""")
monthly_revenue.show()
monthly_revenue.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("python_reports/monthly_revenue")

# Top products
top_products = spark.sql("""
    SELECT Description, SUM(Revenue) AS ProductRevenue
    FROM sales
    GROUP BY Description
    ORDER BY ProductRevenue DESC
    LIMIT 10
""")
top_products.show()
top_products.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("python_reports/top_products")

# Top customers
top_customers = spark.sql("""
    SELECT `Customer ID`, SUM(Revenue) AS CustomerRevenue
    FROM sales
    GROUP BY `Customer ID`
    ORDER BY CustomerRevenue DESC
    LIMIT 10
""")
top_customers.show()
top_customers.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("python_reports/top_customers")

print("Queries executed and CSVs saved in python_reports/")

import pandas as pd
import os

def export_to_excel(sales_df, excel_dir="excel_output"):
    os.makedirs(excel_dir, exist_ok=True)

    # Monthly revenue
    monthly = sales_df.groupby(["year", "month"])["Revenue"].sum().reset_index()

    # Top 10 products
    top_products = sales_df.groupby("Description")["Revenue"].sum().reset_index()\
                           .sort_values("Revenue", ascending=False).head(10)

    # Top 10 customers
    top_customers = sales_df.groupby("CustomerID")["Revenue"].sum().reset_index()\
                            .sort_values("Revenue", ascending=False).head(10)

    excel_path = os.path.join(excel_dir, "Sales_Report.xlsx")

    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        monthly.to_excel(writer, sheet_name="Monthly Revenue", index=False)
        top_products.to_excel(writer, sheet_name="Top Products", index=False)
        top_customers.to_excel(writer, sheet_name="Top Customers", index=False)

    print(f"Excel report saved: {excel_path}")


spark.stop()
