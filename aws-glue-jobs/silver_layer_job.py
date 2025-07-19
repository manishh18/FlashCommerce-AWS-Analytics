import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
col, when, to_timestamp, lit, lower, trim, to_date, hour, expr, concat, udf
)
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime
# Initialize job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Paths
bronze_path = "s3://e-com-lake/Bronze_Layer/year=2025/month=06/day=05/"
silver_path = "s3://e-com-lake/Silver_Layer/"
bad_records_path = "s3://e-com-lake/BadRecords/"
invalid_path = "s3://e-com-lake/Invalid_Layer/"
# Constants
INDIAN_CITIES = [
'Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad',
'Jaipur', 'Lucknow', 'Surat', 'Kanpur', 'Nagpur', 'Indore', 'Thane', 'Bhopal',
'Visakhapatnam', 'Patna', 'Vadodara', 'Ghaziabad', 'Ludhiana', 'Agra', 'Nashik'
]
VALID_EVENT_TYPES = ['view', 'add_to_cart', 'purchase']
VALID_PAYMENT_METHODS = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'Cash on Delivery']
VALID_AGE_GROUPS = ['18-24', '25-34', '35-44', '45-54', '55+']
VALID_GENDERS = ['female', 'male', 'other']
FESTIVAL_DATES = ['2025-10-23', '2025-03-14']
CITY_REGION = {
'Delhi': 'North', 'Lucknow': 'North', 'Kanpur': 'North', 'Ghaziabad': 'North', 'Agra': 'North', 'Jaipur': 'North', 'Ludhiana': 'North',
'Mumbai': 'West', 'Pune': 'West', 'Ahmedabad': 'West', 'Surat': 'West', 'Nagpur': 'West', 'Thane': 'West', 'Vadodara': 'West', 'Nashik': 'West',
'Bangalore': 'South', 'Hyderabad': 'South', 'Chennai': 'South', 'Visakhapatnam': 'South',
'Kolkata': 'East', 'Patna': 'East',
'Bhopal': 'Central', 'Indore': 'Central'
}
CITY_TIER = {
'Mumbai': 'Tier 1', 'Delhi': 'Tier 1', 'Bangalore': 'Tier 1', 'Hyderabad': 'Tier 1', 'Chennai': 'Tier 1', 'Kolkata': 'Tier 1',
'Pune': 'Tier 2', 'Ahmedabad': 'Tier 2', 'Jaipur': 'Tier 2', 'Lucknow': 'Tier 2', 'Surat': 'Tier 2', 'Kanpur': 'Tier 2',
'Nagpur': 'Tier 2', 'Indore': 'Tier 2', 'Thane': 'Tier 2', 'Bhopal': 'Tier 2', 'Visakhapatnam': 'Tier 2', 'Patna': 'Tier 2',
'Vadodara': 'Tier 2', 'Ghaziabad': 'Tier 2', 'Ludhiana': 'Tier 2', 'Agra': 'Tier 2', 'Nashik': 'Tier 2'
}
CITY_GEO = {
'Bangalore': {'lat': 12.9716, 'lon': 77.5946}, 'Mumbai': {'lat': 19.0760, 'lon': 72.8777},
'Delhi': {'lat': 28.7041, 'lon': 77.1025}, 'Hyderabad': {'lat': 17.3850, 'lon': 78.4867},
'Chennai': {'lat': 13.0827, 'lon': 80.2707}, 'Kolkata': {'lat': 22.5726, 'lon': 88.3639},
'Pune': {'lat': 18.5204, 'lon': 73.8567}, 'Ahmedabad': {'lat': 23.0225, 'lon': 72.5714},
'Jaipur': {'lat': 26.9124, 'lon': 75.7873}, 'Lucknow': {'lat': 26.8467, 'lon': 80.9462},
'Surat': {'lat': 21.1702, 'lon': 72.8311}, 'Kanpur': {'lat': 26.4499, 'lon': 80.3319},
'Nagpur': {'lat': 21.1458, 'lon': 79.0882}, 'Indore': {'lat': 22.7196, 'lon': 75.8577},
'Thane': {'lat': 19.2183, 'lon': 72.9781}, 'Bhopal': {'lat': 23.2599, 'lon': 77.4126},
'Visakhapatnam': {'lat': 17.6868, 'lon': 83.2185}, 'Patna': {'lat': 25.5941, 'lon': 85.1376},
'Vadodara': {'lat': 22.3072, 'lon': 73.1812}, 'Ghaziabad': {'lat': 28.6692, 'lon': 77.4538},
'Ludhiana': {'lat': 30.9010, 'lon': 75.8573}, 'Agra': {'lat': 27.1767, 'lon': 78.0081},
'Nashik': {'lat': 19.9975, 'lon': 73.7898}
}
@udf(returnType=StringType())
def get_region(city): return CITY_REGION.get(city, "Unknown")
@udf(returnType=StringType())
def get_city_tier(city): return CITY_TIER.get(city, "NULL")
@udf(returnType=DoubleType())
def get_lat(city): return CITY_GEO.get(city, {'lat': 0.0})['lat']
@udf(returnType=DoubleType())
def get_lon(city): return CITY_GEO.get(city, {'lon': 0.0})['lon']
# Read JSON
bronze_df = (
spark.read
.option("mode", "PERMISSIVE")
.option("badRecordsPath", bad_records_path)
.json(bronze_path)
)
# Clean & transform
bronze_df_cleaned = (
bronze_df
.withColumn("price", when(col("price").cast("double").isNotNull(), col("price").cast("double")).otherwise(lit(0.0)))
.withColumn("cost_price", when(col("cost_price").cast("double").isNotNull(), col("cost_price").cast("double")).otherwise(lit(0.0)))
.withColumn("discount_percentage", when(col("discount_percentage").cast("double").isNotNull(), col("discount_percentage").cast("double")).otherwise(lit(0.0)))
.withColumn("quantity", when(col("quantity").cast("integer").isNotNull(), col("quantity").cast("integer")).otherwise(lit(0)))
.withColumn("stock_quantity", when(col("stock_quantity").cast("integer").isNotNull(), col("stock_quantity").cast("integer")).otherwise(lit(0)))
.withColumn("rating", when(col("rating").cast("double").isNotNull(), col("rating").cast("double")).otherwise(lit(1.0)))
.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
.withColumn("product_name", when(col("product_name").isNull(), concat(lit("Unknown "), col("category"))).otherwise(lower(trim(col("product_name")))))
.withColumn("payment_type", when(col("event_type") != "purchase", lit("other")).otherwise(col("payment_type")))
.dropDuplicates(["event_time", "user_id", "session_id", "product_id"])
.filter(col("price") > 0)
.filter(col("cost_price") > 0)
.filter(col("quantity") >= 0)
.filter(col("stock_quantity") >= 0)
.filter(col("rating").between(1, 5))
.filter(col("city").isin(INDIAN_CITIES))
.filter(col("event_type").isin(VALID_EVENT_TYPES))
.filter(col("payment_type").isin(VALID_PAYMENT_METHODS + ['other']))
.filter(col("customer_age_group").isin(VALID_AGE_GROUPS))
.filter(col("customer_gender").isin(VALID_GENDERS))
.withColumn("city", when(col("city") == "Bengaluru", "Bangalore").otherwise(col("city")))
.withColumn("final_price", (col("price") * (1 - col("discount_percentage") / 100)).cast("double"))
.withColumn("margin", when((col("event_type") == "purchase") & (col("cost_price") > 0),
((col("final_price") - col("cost_price")) * col("quantity") / (col("cost_price") * col("quantity")) * 100)).otherwise(lit(0.0)))
.withColumn("revenue", when(col("event_type") == "purchase", col("final_price") * col("quantity")).otherwise(lit(0.0)))
.withColumn("order_flag", when(col("event_type") == "purchase", lit(1)).otherwise(lit(0)))
.withColumn("conversion_eligible", when(col("event_type") == "view", lit(1)).otherwise(lit(0)))
.withColumn("region", get_region(col("city")))
.withColumn("city_tier", get_city_tier(col("city")))
.withColumn("latitude", get_lat(col("city")))
.withColumn("longitude", get_lon(col("city")))
.withColumn("event_time_ist", col("event_time") + expr("INTERVAL 5 HOURS 30 MINUTES"))
.withColumn("event_date", to_date(col("event_time_ist")))
.withColumn("hour_of_day", hour(col("event_time_ist")))
.withColumn("is_peak_hour", when(col("hour_of_day").between(18, 21), lit(True)).otherwise(lit(False)))
.withColumn("is_festival_day", when(col("event_date").isin([to_date(lit(d)) for d in FESTIVAL_DATES]), lit(True)).otherwise(lit(False)))
)
# Log invalid records
invalid_df = bronze_df.join(
bronze_df_cleaned.select("event_time", "user_id", "session_id", "product_id"),
on=["event_time", "user_id", "session_id", "product_id"],
how="left_anti"
)
if invalid_df.count() > 0:
invalid_key = f"year=2025/month=06/day=05/invalid_records_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
invalid_df.write.mode("overwrite").parquet(f"{invalid_path}{invalid_key}")
# Select final columns
silver_df = bronze_df_cleaned.select(
"event_time", "event_type", "user_id", "session_id", "device_type",
"referral_source", "campaign_id", "product_id", "product_name", "category",
"price", "cost_price", "final_price", "discount_percentage", "quantity",
"stock_quantity", "seller_id", "city", "region", "city_tier", "latitude",
"longitude", "payment_type", "customer_age_group", "customer_gender",
"rating", "promotion_active", "margin", "revenue", "order_flag",
"conversion_eligible", "event_time_ist", "event_date", "hour_of_day",
"is_peak_hour", "is_festival_day"
)
# Write to Silver Layer
silver_dyf = DynamicFrame.fromDF(silver_df, glueContext, "silver_dyf")
glueContext.write_dynamic_frame.from_options(
frame=silver_dyf,
connection_type="s3",
connection_options={"path": silver_path, "partitionKeys": ["seller_id"]},
format="json"
)
# Commit
job.commit()
