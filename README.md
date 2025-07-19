# FlashCommerce: Real-time E-commerce Analytics on AWS

## Table of Contents
- [Project Overview](#project-overview)
- [Problem Statement](#problem-statement)
- [Project Goal](#project-goal)
- [Objectives](#objectives)
- [AWS Services Used](#aws-services-used)
- [Architecture Diagram](#architecture-diagram)
- [Data Pipeline Layers (Lakehouse Approach)](#data-pipeline-layers-lakehouse-approach)
- [Key Features & Functionality](#key-features--functionality)
- [Dashboard Snapshots](#dashboard-snapshots)
- [Technical Implementation Details](#technical-implementation-details)
    - [Bronze Layer Ingestion (Kinesis & Lambda)](#bronze-layer-ingestion-kinesis--lambda)
    - [Silver Layer Transformation (AWS Glue)](#silver-layer-transformation-aws-glue)
    - [Gold Layer Aggregations (AWS Glue)](#gold-layer-aggregations-aws-glue)
    - [Data Cataloging & Querying (AWS Glue Crawler & Athena)](#data-cataloging--querying-aws-glue-crawler--athena)
    - [Visualization (Amazon QuickSight)](#visualization-amazon-quicksight)
- [AWS Console Screenshots](#aws-console-screenshots)
- [Setup and Deployment](#setup-and-deployment)
- [Challenges and Learnings](#challenges-and-learnings)
- [Future Scope](#future-scope)
- [Contributors](#contributors)
- [License](#license)

---

## Project Overview

FlashCommerce is a real-time e-commerce analytics solution designed to empower sellers with immediate insights into customer behavior and product sales. This project leverages a robust AWS-based data pipeline to capture, process, and analyze transactional and event data, providing actionable dashboards for informed decision-making.

## Problem Statement

E-commerce sellers often struggle with obtaining quick insights into customer behavior and product performance. Traditional sales reports are delayed and static, hindering real-time decision-making regarding stock, pricing, and product strategies. It's hard for them to track which products are performing well in real time.

## Project Goal

The primary goal of this project is to create a real-time dashboard that enables sellers to analyze their sales and identify trending products using various AWS services.

## Objectives

* Help sellers see their product performance in real time.
* Capture and process customer interaction events (e.g., product views, add-to-cart, purchases) in real time.
* Show trends like best-selling products, total sales, and geographical revenue generation.
* Build a live dashboard using AWS tools for easy access and actionable insights.
* Make the system fast, reliable, and scalable for many sellers.

## AWS Services Used

This project extensively utilizes the following AWS services:

* **Amazon Kinesis:** For real-time data streaming and ingestion.
* **Amazon S3 (Simple Storage Service):** For scalable and durable data storage across different layers (Bronze, Silver, Gold).
* **AWS Lambda:** To trigger data processing workflows and move data from Kinesis to S3.
* **AWS Glue:** For ETL (Extract, Transform, Load) operations, including data cleaning, transformation, and aggregation across data layers, and for cataloging data.
* **Amazon Athena:** For serverless interactive querying of data stored in S3.
* **Amazon QuickSight:** For creating interactive dashboards and visualizations from the processed data.

## Architecture Diagram

The following diagram illustrates the end-to-end architecture of the FlashCommerce real-time analytics pipeline:

![AWS Architecture Diagram](screenshots/architecture.png)
*Data flows from Kinesis Stream to S3 Bronze, processed by AWS Glue (Silver, Gold layers), cataloged by Athena, and visualized in QuickSight.*

## Data Pipeline Layers (Lakehouse Approach)

The project implements a Medallion Architecture (Bronze, Silver, Gold layers) within an S3-based data lakehouse for structured data processing and analytics.

* **Bronze Layer (Raw Data Ingestion):** Stores unprocessed or minimally processed raw data.
* **Silver Layer (Cleaned & Structured Data):** Contains cleaned and structured data, ready for querying and further transformations. This layer addresses data quality issues and applies initial enrichment.
* **Gold Layer (Business-Ready Data):** Optimized for analytics and reporting, this layer holds aggregated and business-specific metrics derived from the Silver layer.

## Key Features & Functionality

* **Real-time Event Capture:** Ingests customer interaction events (views, add-to-cart, purchases) via Kinesis.
* **Data Validation & Cleansing:** AWS Glue jobs clean and validate raw data, handling missing values, standardizing formats, and filtering invalid records.
* **Feature Engineering:** Derives new features like `final_price`, `margin`, `revenue`, `order_flag`, `conversion_eligible`, `region`, `city_tier`, `latitude`, `longitude`, `event_time_ist`, `event_date`, `hour_of_day`, `is_peak_hour`, and `is_festival_day`.
* **Data Aggregation for Analytics:**
    * **Product Metrics:** Identifies top 5 products by profit per seller, including total revenue and quantity sold.
    * **Order Details:** Captures top 5 recent orders per seller.
    * **Event Counts:** Provides pivoted counts for view, add-to-cart, and purchase events per seller.
    * **City Sales:** Aggregates total revenue and orders per city, region, and city tier, specific to sellers.
    * **Geo Heatmap:** Aggregates total revenue and total orders per city (across all sellers).
    * **Daily Metrics:** Tracks daily total revenue, profit, and order counts per seller.
* **Interactive Dashboards:** Visualizes key metrics and trends using Amazon QuickSight.

## Dashboard Snapshots

Here are some snapshots from the Amazon QuickSight dashboard, providing real-time insights:

### Total Revenue
![Total Revenue](screenshots/dashboard_total_revenue.png)
*A snapshot showing the total revenue generated.*

### Cities Revenue Map
![Cities Revenue Map](screenshots/dashboard_cities_revenue_map.png)
*A geographical heatmap illustrating revenue distribution across different Indian cities.*

### Conversion Ratio
![Conversion Ratio](screenshots/dashboard_conversion_ratio.png)
*This funnel chart displays the conversion rates from product views to add-to-cart and ultimately to purchases.*

### Total Profit By Product Category
![Total Profit By Product Category](screenshots/dashboard_profit_by_category.png)
*A pie chart showing the distribution of total profit across different product categories.*

### Latest Orders
![Latest Orders](screenshots/dashboard_latest_orders.png)
*A table showcasing recent customer orders with key details like product, price, and payment type.*

### Average Profit Margin
![Average Profit Margin](screenshots/dashboard_average_profit_margin.png)
*An indicator showing the average profit margin across all sales.*

## Technical Implementation Details

### Bronze Layer Ingestion (Kinesis & Lambda)
* Raw event data is streamed into **Amazon Kinesis**.
* An **AWS Lambda function** (`kinesis_to_s3_lambda.py`) is triggered by the Kinesis stream. The code for this function is available in `aws-lambda-functions/kinesis_to_s3_lambda.py`.
* The Lambda function reads records from Kinesis, decodes the base64 payload, parses the JSON data, and uploads it to the **Bronze Layer in S3** with a timestamped path (e.g., `Bronze_Layer/year=YYYY/month=MM/day=DD/timestamp.json`).
* After data ingestion, the Lambda function initiates the `bronze-crawler`, `sil_job` (Silver Glue Job), `gold_job` (Gold Glue Job), and `gold_crawler` to process and catalog the data.

### Silver Layer Transformation (AWS Glue)
* An **AWS Glue Job** (`silver_layer_job.py`) reads raw JSON data from the Bronze Layer in S3. The code for this job is available in `aws-glue-jobs/silver_layer_job.py`.
* It performs extensive data cleaning and transformation:
    * Casting numerical columns (price, cost_price, discount_percentage, quantity, stock_quantity, rating) to appropriate types and handling nulls by defaulting to 0.0 or 0.
    * Converting `event_time` to timestamp.
    * Standardizing `product_name` and `payment_type`.
    * Dropping duplicate records.
    * Filtering out invalid records based on various business rules (e.g., price > 0, valid cities, event types, payment methods, age groups, genders).
    * Correcting city names (e.g., "Bengaluru" to "Bangalore").
    * Calculating `final_price`, `margin`, `revenue`, `order_flag`, `conversion_eligible`.
    * Enriching data with `region`, `city_tier`, `latitude`, `longitude` using UDFs and predefined mappings.
    * Adding time-based features: `event_time_ist` (IST conversion), `event_date`, `hour_of_day`, `is_peak_hour`, and `is_festival_day`.
* Invalid records are written to a separate `Invalid_Layer` S3 bucket for auditing.
* The cleaned and transformed data is written to the **Silver Layer in S3** as JSON, partitioned by `seller_id`.

### Gold Layer Aggregations (AWS Glue)
* Another **AWS Glue Job** (`gold_layer_job.py`) reads the processed data from the Silver Layer. The code for this job is available in `aws-glue-jobs/gold_layer_job.py`.
* It performs various aggregations and transformations to create business-ready tables:
    * **Product Metrics:** Top 5 products by profit per seller.
    * **Order Details:** Top 5 latest orders per seller.
    * **Event Counts:** Aggregated counts of view, add-to-cart, and purchase events, pivoted by `event_type` for each seller.
    * **City Sales:** Total revenue and total orders grouped by seller and city/region/tier.
    * **Geo Heatmap:** Total revenue and total orders grouped by city (across all sellers).
    * **Daily Metrics:** Daily total revenue, profit, and orders per seller.
* These aggregated tables are written to the **Gold Layer in S3** as Parquet files, with appropriate partitioning for optimized querying.

### Data Cataloging & Querying (AWS Glue Crawler & Athena)
* **AWS Glue Crawlers** (`bronze-crawler`, `gold_crawler`) automatically discover the schema of the data in S3 (Bronze and Gold layers, respectively) and populate the **AWS Glue Data Catalog**.
* The Glue Data Catalog acts as a central metadata repository.
* **Amazon Athena** uses the Glue Data Catalog to enable serverless SQL querying directly on the data in S3 without needing to load it into a database. This allows QuickSight to easily access the Gold Layer data.

### Visualization (Amazon QuickSight)
* **Amazon QuickSight** connects to the Gold Layer tables (exposed via Athena) to create interactive dashboards.
* The dashboards provide insights into:
    * Total Revenue, Total Profit, Average Profit Margin.
    * Product performance (e.g., top products by profit).
    * Customer conversion funnels (View, Add to Cart, Purchase).
    * Geographical sales distribution.
    * Daily trends and latest orders.

---

## AWS Console Screenshots

This section provides direct screenshots from the AWS console, demonstrating the deployed components and data states.

### S3 Data Lake Buckets Overview
![S3 E-commerce Lake Bucket Overview](screenshots/s3_ecom_lake_bucket_overview.jpg)
*An overview of the `e-com-lake` S3 bucket, showing the various data layers: `Bronze_Layer/`, `Silver_Layer/`, `Gold_Layer/`, `Invalid_Layer/`, and `athena_result/`.*

### Bronze Layer in S3
![Bronze Layer in S3](screenshots/s3_bronze_layer.jpg)
*Screenshot of the `Bronze_Layer` in S3, showing daily partitions (e.g., `day=05/`, `day=06/`) where raw Kinesis data is stored.*

### Silver Layer in S3
![Silver Layer in S3](screenshots/s3_silver_layer.jpg)
*Screenshot of the `Silver_Layer` in S3, demonstrating data partitioned by `seller_id`, indicating cleaned and structured data ready for further processing.*

### Gold Layer in S3
![Gold Layer in S3](screenshots/s3_gold_layer.jpg)
*Screenshot of the `Gold_Layer` in S3, displaying the various aggregated tables (e.g., `city_sales/`, `daily_metrics/`, `event_counts/`, `geo_heatmap/`, `order_details/`, `product_metrics/`) prepared for analytics.*

### AWS Glue Bronze Crawler Runs
![AWS Glue Bronze Crawler Runs](screenshots/glue_bronze_crawler_runs.jpg)
*View of the `bronze-crawler` runs in AWS Glue, showing successful completion for schema discovery in the Bronze Layer.*

### AWS Glue Gold Crawler Configuration
![AWS Glue Gold Crawler Configuration](screenshots/glue_gold_crawler_config.jpg)
*Configuration details for the `gold_crawler` in AWS Glue, highlighting its S3 data source (`s3://e-com-lake/Gold_Layer/`) and the target database (`gold_db`).*

### AWS Glue Gold Crawler Runs
![AWS Glue Gold Crawler Runs](screenshots/glue_gold_crawler_runs.jpg)
*Overview of the `gold_crawler` runs, confirming successful schema updates and partition discoveries for the Gold Layer tables.*

### Amazon Athena Querying Gold Layer
![Amazon Athena Querying Gold Layer](screenshots/athena_gold_db_query.jpg)
*Screenshot from Amazon Athena, demonstrating a successful query against a table (`gold_db.goldorder_details`) in the Gold Layer, confirming data accessibility for analysis.*

---

## Setup and Deployment

To deploy and run this project:

1.  **AWS Account:** Ensure you have an active AWS account.
2.  **AWS CLI:** Configure your AWS CLI with appropriate credentials and permissions (IAM roles for Lambda, Glue, S3, Kinesis, Athena, QuickSight).
3.  **S3 Buckets:** Create an S3 bucket (e.g., `e-com-lake`) for the Bronze, Silver, Gold, and BadRecords layers.
4.  **Kinesis Stream:** Create an Amazon Kinesis Data Stream (e.g., `ecommerce-event-stream`).
5.  **Lambda Function Deployment:**
    * Package the `kinesis_to_s3_lambda.py` code located in `aws-lambda-functions/`.
    * Create an AWS Lambda function, configure its trigger from your Kinesis stream, and upload the packaged code. Ensure the Lambda execution role has permissions to read from Kinesis, write to S3, and start Glue jobs/crawlers.
6.  **AWS Glue Jobs Creation:**
    * Upload `silver_layer_job.py` and `gold_layer_job.py` (from `aws-glue-jobs/`) to an S3 bucket.
    * In the AWS Glue console, create two new Glue jobs:
        * `sil_job`: Point to `silver_layer_job.py`, specify the necessary IAM role, and configure job parameters (e.g., Spark version, worker type).
        * `gold_job`: Point to `gold_layer_job.py`, specify the necessary IAM role.
7.  **AWS Glue Crawlers Creation:**
    * In the AWS Glue console, create two new Glue Crawlers:
        * `bronze-crawler`: Point it to `s3://e-com-lake/Bronze_Layer/`.
        * `gold_crawler`: Point it to `s3://e-com-lake/Gold_Layer/`.
        * Configure them to run on demand or on a schedule, and create/update tables in the Glue Data Catalog.
8.  **QuickSight Setup:**
    * In Amazon QuickSight, create a new data set pointing to the tables created by the `gold_crawler` in the Glue Data Catalog via Athena.
    * Design and build your dashboards using the various aggregated metrics available in the Gold Layer.

## Challenges and Learnings

* **Data Quality and Validation:** A significant challenge was ensuring data quality from the raw Kinesis stream. This was addressed by implementing comprehensive validation and cleaning logic in the Silver layer Glue job, including handling missing values, standardizing formats, and filtering out invalid records based on business rules.
* **IAM Permissions:** Correctly configuring IAM roles and policies for Lambda, Glue, S3, and Kinesis was crucial and required careful attention to the principle of least privilege.
* **Performance Optimization (Glue):** Caching the `silver_df` in the Gold Glue job significantly improved performance for subsequent transformations.
* **Timezone Handling:** Ensuring consistent timezone handling (converting to IST) for accurate time-based analysis (`event_time_ist`, `event_date`, `hour_of_day`).
* **Dynamic Data Enrichment:** Leveraging UDFs in Glue to dynamically add `region`, `city_tier`, `latitude`, and `longitude` based on city data demonstrated effective data enrichment strategies.

## Future Scope

* **Machine Learning Integration:** Integrate machine learning models for advanced analytics, such as demand forecasting and dynamic pricing recommendations.
* **Customer Sentiment Analysis:** Incorporate customer sentiment analysis from product reviews to provide insights into customer satisfaction.
* **Enhanced User Interface:** Expand the dashboard to a mobile-friendly UI or embed it directly within seller portals for easier access and deeper integration.
* **Real-time Anomaly Detection:** Implement real-time anomaly detection on key metrics (e.g., sudden drops in sales, unusual traffic patterns) using AWS services like Kinesis Analytics or Amazon Lookout for Metrics.
* **Multi-Tenancy Improvements:** Further refine the architecture for robust multi-tenancy, ensuring data isolation and performance for a large number of sellers.

## Contributors

* [Your Name/GitHub Handle]

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
