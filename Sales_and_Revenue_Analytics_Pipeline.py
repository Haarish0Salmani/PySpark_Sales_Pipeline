# Sales and Revenue Analytics Pipeline with PySpark (DataFrame API & Spark SQL)

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col

# Initialize Spark session
spark = SparkSession.builder     .appName("Sales and Revenue Analytics Pipeline")     .getOrCreate()

# ==============================
# Data Loading Section
# ==============================

# Load sales data
sales_df = spark.read.format("csv")     .option("header", "true")     .option("inferSchema", "true")     .load("dataset/sales_data.csv")

# Load product data
product_df = spark.read.format("csv")     .option("header", "true")     .option("inferSchema", "true")     .load("dataset/product_data.csv")

# Load customer data
customer_df = spark.read.format("csv")     .option("header", "true")     .option("inferSchema", "true")     .load("dataset/customer_data.csv")

# Load inventory data
inventory_df = spark.read.format("csv")     .option("header", "true")     .option("inferSchema", "true")     .load("dataset/inventory_data.csv")

# ==============================
# DataFrame API - Enrich Sales Data Using Joins
# ==============================

sales_enriched_df = sales_df.join(product_df, 'product_id', 'left')     .join(customer_df, 'customer_id', 'left')     .join(inventory_df, 'product_id', 'left')

# ==============================
# Spark SQL - Enrich Sales Data Using SQL Joins
# ==============================

# Register DataFrames as temporary views for Spark SQL usage
sales_df.createOrReplaceTempView("sales")
product_df.createOrReplaceTempView("product")
customer_df.createOrReplaceTempView("customer")
inventory_df.createOrReplaceTempView("inventory")

# Spark SQL Query to enrich sales data
sales_enriched_sql = spark.sql("""
    SELECT s.*, p.*, c.*, i.*
    FROM sales s
    LEFT JOIN product p ON s.product_id = p.product_id
    LEFT JOIN customer c ON s.customer_id = c.customer_id
    LEFT JOIN inventory i ON s.product_id = i.product_id
""")

# ==============================
# Filtering Section
# ==============================

# DataFrame API - Filter for Completed Sales
sales_completed_df = sales_enriched_df.filter(col("status") == "Completed")

# Spark SQL - Filter for Completed Sales
sales_completed_sql = spark.sql("""
    SELECT *
    FROM sales_enriched_sql
    WHERE status = 'Completed'
""")

# ==============================
# Aggregation Section
# ==============================

# DataFrame API - Aggregate Revenue Metrics
agg_df = sales_completed_df.groupBy("city", "category", "sale_date")     .agg(
        sum("Price").alias("total_revenue"),
        avg("Price").alias("avg_order_value"),
        count("*").alias("total_sales")
    )

# Spark SQL - Aggregate Revenue Metrics
agg_sql = spark.sql("""
    SELECT 
        city, 
        category, 
        sale_date, 
        SUM(Price) AS total_revenue, 
        AVG(Price) AS avg_order_value, 
        COUNT(*) AS total_sales
    FROM sales_completed_sql
    GROUP BY city, category, sale_date
""")

# ==============================
# Data Output Section
# ==============================

# Save DataFrame API result to CSV
agg_df.coalesce(1).write     .format("csv")     .option("header", "true")     .mode("overwrite")     .save("dataset/sales/output_df/")

# Save Spark SQL result to CSV
agg_sql.coalesce(1).write     .format("csv")     .option("header", "true")     .mode("overwrite")     .save("dataset/sales/output_sql/")

print("Sales and Revenue Analytics Pipeline executed successfully!")
