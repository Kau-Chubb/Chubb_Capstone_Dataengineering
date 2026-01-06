# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, when, lit,
    round as spark_round
)
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable


# COMMAND ----------

spark.sql("USE agri_analytics")


# COMMAND ----------

df_crop_prod_bronze = spark.table("bronze_crop_production")
df_rainfall_bronze  = spark.table("bronze_rainfall")
df_soil_bronze      = spark.table("bronze_soil_health")
df_fert_bronze      = spark.table("bronze_fertilizer_usage")
df_crop_master_bronze = spark.table("bronze_crop_master")


# COMMAND ----------

# MAGIC %md
# MAGIC CROP PRODUCTION – SILVER LAYER

# COMMAND ----------

#Base Schema (for Schema Evolution)
from pyspark.sql.types import *

crop_prod_schema = StructType([
    StructField("State", StringType()),
    StructField("District", StringType()),
    StructField("Crop_Year", IntegerType()),
    StructField("Season", StringType()),
    StructField("Crop", StringType()),
    StructField("Area_Hectares", DoubleType()),
    StructField("Production_Tonnes", DoubleType()),
    StructField("Yield_Tonnes_Per_Ha", DoubleType())
])


# COMMAND ----------

#Normalize Text Fields 

from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit
)

df_crop_prod_std = (
    df_crop_prod_bronze
    # Normalize region names
    .withColumn("state", upper(trim(col("State"))))
    .withColumn("district", upper(trim(col("District"))))
    
    # Normalize crop & season
    .withColumn("crop", upper(trim(col("Crop"))))
    .withColumn("season", upper(trim(col("Season"))))
    
    # Cast numeric columns safely
    .withColumn("crop_year", col("Crop_Year").cast("int"))
    .withColumn("area_hectares", col("Area_Hectares").cast("double"))
    .withColumn("production_tonnes", col("Production_Tonnes").cast("double"))
)


# COMMAND ----------

# Missing Value Handling

df_crop_prod_mv = (
    df_crop_prod_std
    # Replace missing season with UNKNOWN
    .withColumn(
        "season",
        when(col("season").isNull(), lit("UNKNOWN")).otherwise(col("season"))
    )
    
    # Replace missing district with UNKNOWN
    .withColumn(
        "district",
        when(col("district").isNull(), lit("UNKNOWN")).otherwise(col("district"))
    )
)


# COMMAND ----------

# ------------------------------------------------------------
# Data Quality Rules
# ------------------------------------------------------------
df_crop_prod_qc = df_crop_prod_mv.withColumn(
    "is_valid_record",
    when(
        (col("state").isNull()) |
        (col("crop").isNull()) |
        (col("crop_year").isNull()) |
        (col("area_hectares") <= 0) |      #  NAME CHANGES HERE CAPITAL A
        (col("production_tonnes") <= 0),
        lit(False)
    ).otherwise(lit(True))
)


# COMMAND ----------

# Separate Valid & Invalid Records  (Quarantine Data)

df_crop_prod_valid   = df_crop_prod_qc.filter(col("is_valid_record") == True)
df_crop_prod_invalid = df_crop_prod_qc.filter(col("is_valid_record") == False)

# COMMAND ----------

# Deduplication
df_crop_prod_dedup = df_crop_prod_valid.dropDuplicates([
    "State", "District", "Crop_Year", "Season", "Crop"
])

# COMMAND ----------

#Recalculate Yield (Trusted Metric)
df_crop_prod_silver = df_crop_prod_dedup.withColumn(
    "yield_tonnes_per_ha",
    col("production_tonnes") / col("area_hectares")
)

# COMMAND ----------

# Write Silver Crop Production
df_crop_prod_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_crop_production")


# Write Quarantine Table
df_crop_prod_invalid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_reject_crop_production")


# COMMAND ----------

# MAGIC %md
# MAGIC RAINFALL – SILVER LAYER

# COMMAND ----------

#Read Bronze Rainfall Table
df_rainfall_bronze = spark.table("bronze_rainfall")

# COMMAND ----------

#Normalize Regions & Cast Data Types
df_rainfall_std = (
    df_rainfall_bronze
    # Region normalization
    .withColumn("state", upper(trim(col("State"))))
    .withColumn("district", upper(trim(col("District"))))

    # Type casting
    .withColumn("crop_year", col("Crop_Year").cast("int"))
    .withColumn("annual_rainfall_mm", col("Annual_Rainfall_mm").cast("double"))
    .withColumn("monsoon_rainfall_mm", col("Monsoon_Rainfall_mm").cast("double"))
)


# COMMAND ----------

#Handle Missing Values
df_rainfall_handled = (
    df_rainfall_std
    # District is non-critical → default
    .withColumn(
        "district",
        when(col("district").isNull(), lit("UNKNOWN")).otherwise(col("district"))
    )
)


# COMMAND ----------

#Data Quality Rules (Incorrect Data Handling)
df_rainfall_qc = df_rainfall_std.withColumn(
    "is_valid_record",
    when(
        (col("state").isNull()) |
        (col("district").isNull()) |
        (col("crop_year").isNull()) |
        (col("annual_rainfall_mm") < 0) |
        (col("monsoon_rainfall_mm") < 0),
        lit(False)
    ).otherwise(lit(True))
)


# COMMAND ----------

#Quarantine vs Valid Split
df_rainfall_valid = df_rainfall_qc.filter(col("is_valid_record") == True)
df_rainfall_invalid = df_rainfall_qc.filter(col("is_valid_record") == False)


# COMMAND ----------

#Deduplication (Correct Grain)
df_rainfall_valid = df_rainfall_valid.dropDuplicates(
    ["state", "district", "crop_year"]
)


# COMMAND ----------

#Persist Silver Tables
df_rainfall_valid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_rainfall")

df_rainfall_invalid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_reject_rainfall")


# COMMAND ----------

# MAGIC %md
# MAGIC SOIL HEALTH – SILVER LAYER

# COMMAND ----------

df_soil_bronze = spark.table("bronze_soil_health")


# COMMAND ----------

#Standardization & Schema Enforcement
from pyspark.sql.functions import col, trim, upper
df_soil_std = (
    df_soil_bronze
    # Region normalization
    .withColumn("state", upper(trim(col("State"))))
    .withColumn("district", upper(trim(col("District"))))

    # Type casting
    .withColumn("crop_year", col("Crop_Year").cast("int"))
    .withColumn("soil_type", upper(trim(col("Soil_Type"))))
    .withColumn("ph", col("pH").cast("double"))
    .withColumn("organic_carbon", col("Organic_Carbon").cast("double"))
)



# COMMAND ----------

#Handle Missing Values
from pyspark.sql.functions import when, lit
df_soil_handled = (
    df_soil_std
    # Soil type is non-critical → default
    .withColumn(
        "soil_type",
        when(col("soil_type").isNull(), lit("UNKNOWN")).otherwise(col("soil_type"))
    )
)

# COMMAND ----------

#Data Quality Rules (Incorrect Data Handling)
df_soil_qc = df_soil_std.withColumn(
    "is_valid_record",
    when(
        (col("state").isNull()) |
        (col("district").isNull()) |
        (col("ph_level") < 3) |
        (col("ph_level") > 10) |
        (col("organic_carbon") < 0),
        lit(False)
    ).otherwise(lit(True))
)

# COMMAND ----------

#Quarantine vs Valid Split
df_soil_valid   = df_soil_qc.filter(col("is_valid_record") == True)
df_soil_invalid = df_soil_qc.filter(col("is_valid_record") == False)


# COMMAND ----------

#Deduplication
df_soil_silver = df_soil_valid.dropDuplicates(
    ["state", "district", "crop_year"]
)


# COMMAND ----------

#Persist Silver Tables
# Silver Soil Health
df_soil_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_soil_health")

# Quarantine Soil Health
df_soil_invalid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_reject_soil_health")

# COMMAND ----------

# MAGIC %md
# MAGIC FERTILIZER USAGE – SILVER LAYER

# COMMAND ----------

#Read Bronze Fertilizer Table
df_fert_bronze = spark.table("bronze_fertilizer_usage")


# COMMAND ----------

#Standardization & Schema Enforcement
from pyspark.sql.types import IntegerType, DoubleType

df_fert_std = (
    df_fert_bronze
    # Region normalization
    .withColumn("state", upper(trim(col("State"))))
    .withColumn("district", upper(trim(col("District"))))

    # Crop normalization
    .withColumn("crop", upper(trim(col("Crop"))))

    # Type casting
    .withColumn("crop_year", col("Crop_Year").cast("int"))
    .withColumn("fertilizer_type", upper(trim(col("Fertilizer_Type"))))
    .withColumn(
        "fertilizer_used_kg_per_ha",
        col("Fertilizer_Used_kg_per_ha").cast("double")
    )
)

# COMMAND ----------

#Handle Missing Values
df_fert_handled = (
    df_fert_std
    # District is non-critical → default
    .withColumn(
        "district",
        when(col("district").isNull(), lit("UNKNOWN")).otherwise(col("district"))
    )
)

# COMMAND ----------

#Data Quality Rules (Incorrect Data Handling)
df_fert_qc = df_fert_std.withColumn(
    "is_valid_record",
    when(
        (col("state").isNull()) |
        (col("district").isNull()) |
        (col("crop").isNull()) |
        (col("crop_year").isNull()) |
        (col("nitrogen_kg_ha") < 0) |
        (col("phosphorus_kg_ha") < 0) |
        (col("potassium_kg_ha") < 0),
        lit(False)
    ).otherwise(lit(True))
)


# COMMAND ----------

#Quarantine vs Valid Split
df_fert_valid   = df_fert_qc.filter(col("is_valid_record") == True)
df_fert_invalid = df_fert_qc.filter(col("is_valid_record") == False)


# COMMAND ----------

#Deduplication
df_fert_valid = df_fert_valid.dropDuplicates(
    ["state", "district", "crop", "crop_year"]
)


# COMMAND ----------

#Persist Silver Tables
df_fert_valid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_fertilizer_usage")

df_fert_invalid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_reject_fertilizer_usage")


# COMMAND ----------

# MAGIC %md
# MAGIC CROP MASTER – SILVER LAYER

# COMMAND ----------

df_crop_master_silver = df_crop_master_bronze.dropDuplicates(["Crop"])

df_crop_master_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_crop_master")


# COMMAND ----------

# MAGIC %md
# MAGIC PERFORMANCE & OPTIMIZATION 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize Delta tables
# MAGIC OPTIMIZE silver_crop_production;
# MAGIC OPTIMIZE silver_rainfall;
# MAGIC OPTIMIZE silver_soil_health;
# MAGIC OPTIMIZE silver_fertilizer_usage;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-Ordering for faster analytics
# MAGIC OPTIMIZE silver_crop_production ZORDER BY (State, Crop_Year, Crop);
# MAGIC OPTIMIZE silver_rainfall ZORDER BY (State, Crop_Year);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Crop Production NULL Counts after silver_transformation
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS state_nulls,
# MAGIC     SUM(CASE WHEN district IS NULL THEN 1 ELSE 0 END) AS district_nulls,
# MAGIC     SUM(CASE WHEN crop IS NULL THEN 1 ELSE 0 END) AS crop_nulls,
# MAGIC     SUM(CASE WHEN season IS NULL THEN 1 ELSE 0 END) AS season_nulls,
# MAGIC     SUM(CASE WHEN crop_year IS NULL THEN 1 ELSE 0 END) AS crop_year_nulls,
# MAGIC     SUM(CASE WHEN area_hectares IS NULL THEN 1 ELSE 0 END) AS area_nulls,
# MAGIC     SUM(CASE WHEN production_tonnes IS NULL THEN 1 ELSE 0 END) AS production_nulls,
# MAGIC     SUM(CASE WHEN yield_tonnes_per_ha IS NULL THEN 1 ELSE 0 END) AS yield_nulls
# MAGIC FROM silver_crop_production;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Rainfall NULL Counts after silver_transformation
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS state_nulls,
# MAGIC     SUM(CASE WHEN district IS NULL THEN 1 ELSE 0 END) AS district_nulls,
# MAGIC     SUM(CASE WHEN crop_year IS NULL THEN 1 ELSE 0 END) AS crop_year_nulls,
# MAGIC     SUM(CASE WHEN annual_rainfall_mm IS NULL THEN 1 ELSE 0 END) AS annual_rainfall_nulls,
# MAGIC     SUM(CASE WHEN monsoon_rainfall_mm IS NULL THEN 1 ELSE 0 END) AS monsoon_rainfall_nulls
# MAGIC FROM silver_rainfall;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- rainfall_data_bad_rows after silver_transformation
# MAGIC SELECT COUNT(*) AS bad_rows
# MAGIC FROM silver_rainfall
# MAGIC WHERE annual_rainfall_mm < 0
# MAGIC    OR monsoon_rainfall_mm < 0
# MAGIC    OR state IS NULL
# MAGIC    OR crop_year IS NULL;
# MAGIC