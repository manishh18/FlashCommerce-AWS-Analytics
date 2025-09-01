import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, sum, count, lit, when, row_number
from pyspark.sql.window import Window
# Initialize job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Paths
silver_path = "s3://e-com-lake/Silver_Layer/"
gold_path = "s3://e-com-lake/Gold_Layer/"
# Read Silver Layer
silver_df = spark.read.option("mode", "PERMISSIVE").json(silver_path)
# Cache for performance
silver_df.cache()
# Log silver layer row count for debugging
print(f"Silver layer row count: {silver_df.count()}")
# Create Gold Layer Tables
# 1: Product Metrics (Top 5 by Profit)
product_metrics_df = (
silver_df
.filter(col("event_type") == "purchase")
.groupBy("seller_id", "product_id", "product_name", "category")
.agg(
sum(col("revenue")).cast("double").alias("total_revenue"),
sum((col("final_price") - col("cost_price")) * col("quantity")).cast("double").alias("total_profit"),
sum(col("quantity")).cast("integer").alias("total_quantity_sold")
)
.withColumn("rank", row_number().over(Window.partitionBy("seller_id").orderBy(col("total_profit").desc())))
.filter(col("rank") <= 5)
.select("seller_id", "product_id", "product_name", "category", "total_revenue", "total_profit", "total_quantity_sold", "rank")
)
# 2: Order Details (Top 5 Orders)
order_details_df = (
silver_df
.filter(col("event_type") == "purchase")
.withColumn("rank", row_number().over(Window.partitionBy("seller_id").orderBy(col("event_time_ist").desc())))
.filter(col("rank") <= 5)
.select(
"seller_id", "event_time_ist", "user_id", "product_id", "product_name",
"final_price", "quantity", "city", "payment_type", "rank"
)
)
# 3: Event Counts (Pivoted for One Row per Seller)
event_counts_df = (
silver_df
.filter(col("event_type").isin(["view", "add_to_cart", "purchase"]))
.groupBy("seller_id")
.pivot("event_type")
.agg(count(lit(1)).cast("integer"))
.withColumnRenamed("view", "view_count")
.withColumnRenamed("add_to_cart", "add_to_cart_count")
.withColumnRenamed("purchase", "purchase_count")
.na.fill(0)
)
# 4: City Sales (Seller-Specific)
city_sales_df = (
silver_df
.filter(col("event_type") == "purchase")
.groupBy("seller_id", "city", "region", "city_tier", "latitude", "longitude")
.agg(
sum(col("revenue")).cast("double").alias("total_revenue"),
sum(col("order_flag")).cast("integer").alias("total_orders")
)
)
# 5: Geo Heatmap (City-Level Across All Sellers)
geo_heatmap_df = (
silver_df
.filter(col("event_type") == "purchase")
.groupBy("city", "region", "city_tier", "latitude", "longitude")
.agg(
sum(col("revenue")).cast("double").alias("total_revenue"),
sum(col("order_flag")).cast("integer").alias("total_orders")
)
)
# 6: Daily Metrics
daily_metrics_df = (
silver_df
.filter(col("event_type") == "purchase")
.groupBy("seller_id", "event_date")
.agg(
sum(col("revenue")).cast("double").alias("total_revenue"),
sum((col("final_price") - col("cost_price")) * col("quantity")).cast("double").alias("total_profit"),
sum(col("order_flag")).cast("integer").alias("total_orders")
)
)
# Write to Gold Layer
tables = [
("product_metrics", product_metrics_df, ["seller_id"]),
("order_details", order_details_df, ["seller_id"]),
("event_counts", event_counts_df, ["seller_id"]),
("city_sales", city_sales_df, ["seller_id"]),
("geo_heatmap", geo_heatmap_df, []),
("daily_metrics", daily_metrics_df, ["seller_id", "event_date"])
]
for table_name, df, partition_keys in tables:
print(f"Writing {table_name} with {df.count()} rows")
dyf = DynamicFrame.fromDF(df, glueContext, f"{table_name}_dyf")
glueContext.write_dynamic_frame.from_options(
frame=dyf,
connection_type="s3",
connection_options={
"path": f"{gold_path}{table_name}/",
"partitionKeys": partition_keys
},
format="parquet",
format_options={"compression": "snappy"}
)
# Uncache and Commit
silver_df.unpersist()
job.commit()
