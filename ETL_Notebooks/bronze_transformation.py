# Databricks notebook source
# MAGIC %md
# MAGIC BRONZE LAYER INGESTION NOTEBOOK
# MAGIC Project : Agricultural Crop Production & Yield _Optimization_
# MAGIC
# MAGIC

# COMMAND ----------

# Base path where raw CSV files are stored (Unity Catalog Volume)
BASE_PATH = "/Volumes/workspace/default/kaudata"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# Use dedicated analytics database
spark.sql("CREATE DATABASE IF NOT EXISTS agri_analytics")
spark.sql("USE agri_analytics")


# COMMAND ----------

# ------------------------------------------------------------
# Crop Production - Raw Ingestion
# ------------------------------------------------------------

df_crop_production = spark.read.csv(
    f"{BASE_PATH}/ca_crop_production.csv",
    header=True,
    inferSchema=True
)

# Write to Bronze Delta table
df_crop_production.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_crop_production")

print(" bronze_crop_production table created")


# COMMAND ----------

# ------------------------------------------------------------
# Rainfall Data - Raw Ingestion
# ------------------------------------------------------------

df_rainfall = spark.read.csv(
    f"{BASE_PATH}/ca_rainfall_data.csv",
    header=True,
    inferSchema=True
)

df_rainfall.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_rainfall")

print(" bronze_rainfall table created")


# COMMAND ----------

# ------------------------------------------------------------
# Soil Health Data - Raw Ingestion
# ------------------------------------------------------------

df_soil = spark.read.csv(
    f"{BASE_PATH}/ca_soil_health.csv",
    header=True,
    inferSchema=True
)

df_soil.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_soil_health")

print(" bronze_soil_health table created")


# COMMAND ----------

# ------------------------------------------------------------
# Fertilizer Usage Data - Raw Ingestion
# ------------------------------------------------------------

df_fertilizer = spark.read.csv(
    f"{BASE_PATH}/ca_fertilizer_usage.csv",
    header=True,
    inferSchema=True
)

df_fertilizer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_fertilizer_usage")

print(" bronze_fertilizer_usage table created")


# COMMAND ----------

# ------------------------------------------------------------
# Crop Master Data - Reference Ingestion
# ------------------------------------------------------------

df_crop_master = spark.read.csv(
    f"{BASE_PATH}/ca_crop_master.csv",
    header=True,
    inferSchema=True
)

df_crop_master.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_crop_master")

print("bronze_crop_master table created")


# COMMAND ----------

# ------------------------------------------------------------
# Basic Validation - Row Counts
# ------------------------------------------------------------

tables = [
    "bronze_crop_production",
    "bronze_rainfall",
    "bronze_soil_health",
    "bronze_fertilizer_usage",
    "bronze_crop_master"
]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
    print(f"{table} → {count} rows")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Displaying Crop_production inconsistencies --
# MAGIC SELECT *
# MAGIC FROM bronze_crop_production
# MAGIC WHERE Area_Hectares <= 0
# MAGIC    OR Production_Tonnes <= 0
# MAGIC    OR Crop IS NULL
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT Crop
# MAGIC FROM bronze_crop_production
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Displaying Rainfall_data inconsistencies --
# MAGIC SELECT
# MAGIC     State,
# MAGIC     District,
# MAGIC     Crop_Year,
# MAGIC     Annual_Rainfall_mm,
# MAGIC     Monsoon_Rainfall_mm
# MAGIC FROM bronze_rainfall
# MAGIC WHERE Annual_Rainfall_mm < 0
# MAGIC    OR Monsoon_Rainfall_mm < 0
# MAGIC    OR State IS NULL
# MAGIC    OR Crop_Year IS NULL
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     State,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM bronze_rainfall
# MAGIC GROUP BY State
# MAGIC ORDER BY cnt DESC
# MAGIC LIMIT 8;
# MAGIC