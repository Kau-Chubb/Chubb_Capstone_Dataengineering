# Databricks notebook source
spark.sql("USE agri_analytics")

# COMMAND ----------

df_crop_prod   = spark.table("silver_crop_production")
df_rainfall    = spark.table("silver_rainfall")
df_soil        = spark.table("silver_soil_health")
df_fertilizer  = spark.table("silver_fertilizer_usage")
df_crop_master = spark.table("silver_crop_master")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop Gold tables (analytics layer)
# MAGIC DROP TABLE IF EXISTS agri_analytics.gold_crop_yield_summary;
# MAGIC DROP TABLE IF EXISTS agri_analytics.gold_rainfall_yield_analysis;
# MAGIC DROP TABLE IF EXISTS agri_analytics.gold_fertilizer_efficiency;
# MAGIC DROP TABLE IF EXISTS agri_analytics.gold_soil_yield_analysis;
# MAGIC DROP TABLE IF EXISTS agri_analytics.gold_region_performance;

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD TABLE: CROP YIELD SUMMARY

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, avg as spark_avg
df_gold_crop_yield = (
    df_crop_prod
    .groupBy("state", "district", "crop", "crop_year")
    .agg(
        spark_sum("area_hectares").alias("total_area_hectares"),
        spark_sum("production_tonnes").alias("total_production_tonnes"),
        spark_avg("yield_tonnes_per_ha").alias("avg_yield_tonnes_per_ha")
    )
)

# COMMAND ----------

df_gold_crop_yield.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_crop_yield_summary")


# COMMAND ----------

# MAGIC %md
# MAGIC GOLD TABLE: RAINFALL VS YIELD ANALYSIS

# COMMAND ----------

from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import col


df_gold_rainfall_yield = (
    df_crop_prod.alias("cp")
    .join(
        df_rainfall.alias("rf"),
        on=["state", "district", "crop_year"],
        how="left"
    )
    .groupBy("state", "district", "crop", "crop_year")
    .agg(
        spark_avg("rf.annual_rainfall_mm").alias("avg_annual_rainfall_mm"),
        spark_avg("rf.monsoon_rainfall_mm").alias("avg_monsoon_rainfall_mm"),
        spark_avg("cp.yield_tonnes_per_ha").alias("avg_yield_tonnes_per_ha")
    )
)


# COMMAND ----------

df_gold_rainfall_yield.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_rainfall_yield_analysis")


# COMMAND ----------

# MAGIC %md
# MAGIC GOLD TABLE: FERTILIZER EFFICIENCY

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

df_gold_fertilizer_eff = (
    df_crop_prod.alias("cp")
    .join(
        df_fertilizer.alias("ft"),
        on=["state", "district", "crop", "crop_year"],
        how="left"
    )
    .groupBy("state", "district", "crop", "crop_year")
    .agg(
        spark_avg("yield_tonnes_per_ha").alias("avg_yield"),
        spark_avg("nitrogen_kg_ha").alias("avg_nitrogen"),
        spark_avg("phosphorus_kg_ha").alias("avg_phosphorus"),
        spark_avg("potassium_kg_ha").alias("avg_potassium")
    )
)


# COMMAND ----------

df_gold_fertilizer_eff.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_fertilizer_efficiency")


# COMMAND ----------

# MAGIC %md
# MAGIC GOLD TABLE: SOIL HEALTH IMPACT

# COMMAND ----------

from pyspark.sql.functions import avg as spark_avg

df_gold_soil_yield = (
    df_crop_prod.alias("cp")
    .join(
        df_soil.alias("sh"),
        (col("cp.state") == col("sh.state")) &
        (col("cp.district") == col("sh.district")) &
        (col("cp.crop_year") == col("sh.crop_year")),
        how="left"
    )
    .groupBy(
        col("cp.state").alias("state"),
        col("cp.district").alias("district"),
        col("cp.crop").alias("crop"),
        col("cp.crop_year").alias("crop_year")
    )
    .agg(
        spark_avg(col("cp.yield_tonnes_per_ha")).alias("avg_yield_tonnes_per_ha"),
        spark_avg(col("sh.ph")).alias("avg_soil_ph"),
        spark_avg(col("sh.organic_carbon")).alias("avg_organic_carbon")
    )
)


# COMMAND ----------

df_gold_soil_yield.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_soil_yield_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD TABLE: REGIONAL AGRICULTURE PERFORMANCE

# COMMAND ----------

df_gold_region_perf = (
    df_crop_prod
    .groupBy("state", "district")
    .agg(
        spark_sum("production_tonnes").alias("total_production"),
        spark_avg("yield_tonnes_per_ha").alias("avg_yield")
    )
)


# COMMAND ----------

df_gold_region_perf.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_region_performance")


# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_crop_yield_summary ZORDER BY (state, crop_year, crop);
# MAGIC OPTIMIZE gold_rainfall_yield_analysis ZORDER BY (state, crop_year);
# MAGIC OPTIMIZE gold_fertilizer_efficiency ZORDER BY (state, crop_year);
# MAGIC OPTIMIZE gold_soil_yield_analysis ZORDER BY (state, crop_year);
# MAGIC OPTIMIZE gold_region_performance ZORDER BY (state);
# MAGIC