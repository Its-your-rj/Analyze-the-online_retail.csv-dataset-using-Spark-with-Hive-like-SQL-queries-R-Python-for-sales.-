# Sales Analyzer

Analyze the `online_retail.csv` dataset using Spark (with Hive-like SQL queries), R, Python, and Excel to derive sales performance insights for an e-commerce business.

---

## Project Goals

- **Ingest, clean, and transform** raw sales data for robust analysis.
- **Leverage multiple tools** (Spark, R, Python, Excel) for comprehensive insights.
- **Derive actionable sales performance metrics** to inform business decisions.

---

## Tools & Libraries Used

| Tool    | Purpose                                                                 | Key Libraries/Features                |
|---------|-------------------------------------------------------------------------|---------------------------------------|
| Spark   | Scalable data processing, SQL queries, fast aggregations                | PySpark, Spark SQL                    |
| Python  | Data wrangling, scripting, automation                                   | pandas, numpy, matplotlib, seaborn     |
| R       | Statistical analysis, advanced visualization                            | tidyverse, ggplot2, dplyr             |
| Excel   | Quick ad-hoc analysis, pivot tables, manual exploration                 | Built-in Excel features                |

---

## Project Structure

```
Sales_Analyzer/
├── data/
│   └── online_retail.csv
├── notebooks/
│   ├── 00_start_spark.py
│   ├── 01_ingest.py
│   ├── 02_clean.py
│   ├── 03_analysis.py
│   └── 04_visualize.R
├── spark_output/
│   └── clean_online_retail_csv/
├── excel/
│   └── sales_analysis.xlsx
├── logs/
│   └── 02_clean.log
├── run_all.ps1
└── README.md
```

---

## Stage-wise Workflow & Insights

### 1. Ingest & Preview (`01_ingest.py`)
- **Action:** Load raw CSV data into Spark DataFrame.
- **Tools:** Spark, Python
- **Libraries:** PySpark
- **Insights:** Initial data quality, missing values, schema overview, basic stats.

### 2. Clean & Transform (`02_clean.py`)
- **Action:** Remove invalid records, trim text, cast types, compute revenue, extract date parts.
- **Tools:** Spark, Python
- **Libraries:** PySpark, pandas (optional for small samples)
- **Insights:** Cleaned dataset ready for analysis, accurate revenue calculations, time-based features for trend analysis.

### 3. Analyze (`03_analysis.py`, Spark SQL, R)
- **Action:** Run SQL queries for aggregations (sales by month, top products, customer segments), statistical summaries in R.
- **Tools:** Spark SQL, R
- **Libraries:** PySpark SQL, tidyverse, dplyr
- **Insights:** 
  - Monthly/weekly sales trends
  - Top-selling products
  - Customer segmentation (e.g., repeat vs. one-time buyers)
  - Revenue breakdowns

### 4. Visualize (`04_visualize.R`, Excel)
- **Action:** Create charts, pivot tables, dashboards.
- **Tools:** R, Excel
- **Libraries:** ggplot2, Excel built-in charts
- **Insights:** 
  - Visual sales trends
  - Product/category performance
  - Customer behavior patterns

---

## Why These Tools?

- **Spark:** Handles large datasets efficiently, enables SQL-like queries, scalable for big data.
- **Python:** Flexible scripting, integrates with Spark, good for automation and quick data wrangling.
- **R:** Powerful for statistical analysis and advanced visualizations.
- **Excel:** Familiar interface for business users, quick manual exploration and reporting.

---

## Example Insights

- **Sales Trends:** Identify peak months, seasonal effects, and growth patterns.
- **Top Products:** Discover bestsellers and underperforming items.
- **Customer Segments:** Find loyal customers, high-value buyers, and churn risks.
- **Revenue Drivers:** Analyze which products, regions, or time periods drive the most revenue.

---

## How to Run

1. Place `online_retail.csv` in the `data/` folder.
2. Open a terminal in VS Code.
3. Run the pipeline:
   ```powershell
   .\run_all.ps1
   ```
4. Explore outputs in `spark_output/`, `excel/`, and visualizations in R.

---

## Contribution

Feel free to fork, open issues, or submit pull requests for improvements!
