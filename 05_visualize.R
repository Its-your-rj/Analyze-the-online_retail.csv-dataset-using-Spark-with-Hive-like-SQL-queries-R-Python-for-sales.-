# notebooks/05_visualize.R
# -------------------------------
# Setup: Install/load packages
# -------------------------------
packages <- c("tidyverse", "arrow")
for (p in packages) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org/")
  }
  library(p, character.only = TRUE)
}

# -------------------------------
# Load Spark parquet output
# -------------------------------
# Use open_dataset() for parquet folders
sales <- open_dataset("E:/Sales_Analyzer/spark_output/clean_online_retail_parquet") %>%
  collect()   # pull data into memory as a tibble

# Ensure docs folder exists
if (!dir.exists("docs")) {
  dir.create("docs")
}

# -------------------------------
# Monthly revenue plot
# -------------------------------
monthly <- sales %>%
  group_by(year, month) %>%
  summarise(TotalRevenue = sum(Revenue, na.rm = TRUE), .groups = "drop") %>%
  mutate(YearMonth = paste0(year, "-", sprintf("%02d", month)))

ggplot(monthly, aes(x = YearMonth, y = TotalRevenue)) +
  geom_col(fill = "steelblue") +
  labs(title = "Monthly Revenue", x = "Year-Month", y = "Revenue") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))

ggsave("docs/monthly_revenue_R.png", width = 10, height = 5, dpi = 150)

# -------------------------------
# Top 10 products by revenue
# -------------------------------
top_products <- sales %>%
  group_by(Description) %>%
  summarise(ProductRevenue = sum(Revenue, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(ProductRevenue)) %>%
  slice_head(n = 10)

ggplot(top_products, aes(x = reorder(Description, ProductRevenue), y = ProductRevenue)) +
  geom_col(fill = "darkorange") +
  coord_flip() +
  labs(title = "Top 10 Products by Revenue", x = "Product", y = "Revenue")

ggsave("docs/top_products_R.png", width = 8, height = 6, dpi = 150)

# -------------------------------
# Done
# -------------------------------
print("R visualizations saved in docs/")
