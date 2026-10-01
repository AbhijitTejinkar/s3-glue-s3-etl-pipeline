import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, trim

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data from Glue Data Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="raw_db",
    table_name="raw_sales"
)
df = dyf.toDF()

# Data cleansing and transformation
df_clean = df.dropDuplicates(["order_id"]) \
    .na.drop(subset=["order_id", "customer_id"]) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("status", trim(col("status"))) \
    .filter(col("amount") > 0)

# Write to curated S3 bucket as partitioned Parquet
df_clean.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://curated-bucket/sales/")

job.commit()