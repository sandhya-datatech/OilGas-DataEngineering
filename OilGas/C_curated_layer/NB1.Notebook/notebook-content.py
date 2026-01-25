# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "894c2646-6861-4c02-9c4d-eb45e0734500",
# META       "default_lakehouse_name": "dev_Gld_OnG_Lkh",
# META       "default_lakehouse_workspace_id": "29b83063-d09f-4997-ab9d-c075a0f36775",
# META       "known_lakehouses": [
# META         {
# META           "id": "894c2646-6861-4c02-9c4d-eb45e0734500"
# META         },
# META         {
# META           "id": "f67b15ec-0153-4344-a458-411122433c29"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

slv_path="abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/f67b15ec-0153-4344-a458-411122433c29/Tables/dbo/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Silver tables path
#slv_path = "dev_Slv_OnG_Lkh.dbo"

# ========================================
# 1. fct_monthly_production
# Partition: year_month
# Grain: year_month + Well_ID
# ========================================

df_prod = spark.read.format("delta").load(f"{slv_path}dst_production")

fct_monthly_production = df_prod \
    .withColumn("year_month", date_format(to_date(col("Date")), "yyyy-MM")) \
    .groupBy("year_month", "Well_ID", "Well_Name", "Basin", "Well_Type", "Well_Status") \
    .agg(
        count("*").alias("days_produced"),
        sum("Oil_Production_BOPD").alias("total_oil_bopd"),
        sum("Gas_Production_MCF").alias("total_gas_mcf"),
        sum("Water_Production_BWPD").alias("total_water_bwpd"),
        sum("BOE").alias("total_boe"),
        avg("Oil_Production_BOPD").alias("avg_daily_oil"),
        avg("Water_Cut_PCT").alias("avg_water_cut"),
        avg("actual_water_cut").alias("avg_actual_water_cut"),
        sum("Gross_Revenue_USD").alias("total_gross_revenue"),
        sum("Net_Revenue_USD").alias("total_net_revenue"),
        avg("Oil_Price_USD").alias("avg_oil_price"),
        avg("Gas_Price_USD").alias("avg_gas_price"),
        avg("revenue_per_BOE").alias("avg_revenue_per_boe"),
        sum(when(col("high_water_cut_flag") == True, 1).otherwise(0)).alias("high_water_cut_days"),
        sum(when(col("low_producer_flag") == True, 1).otherwise(0)).alias("low_producer_days"),
        max("production_status").alias("production_status")
    ) \
    .withColumn("water_cut_trend", 
        when(col("avg_actual_water_cut") > 80, "Critical")
        .when(col("avg_actual_water_cut") > 60, "High")
        .when(col("avg_actual_water_cut") > 40, "Medium")
        .otherwise("Low"))

fct_monthly_production.write.format("delta").mode("overwrite") \
    .partitionBy("year_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.fct_monthly_production")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 2. fct_monthly_costs
# Partition: year_month
# Grain: year_month + Well_ID
# ========================================

df_costs = spark.read.format("delta").load(f"{slv_path}dst_costs")

fct_monthly_costs = df_costs \
    .withColumn("year_month", date_format(to_date(col("Date")), "yyyy-MM")) \
    .groupBy("year_month", "Well_ID") \
    .agg(
        count("*").alias("days_with_costs"),
        sum("Labor_Cost_USD").alias("total_labor_cost"),
        sum("Power_Cost_USD").alias("total_power_cost"),
        sum("Chemical_Cost_USD").alias("total_chemical_cost"),
        sum("Water_Disposal_Cost_USD").alias("total_water_disposal_cost"),
        sum("Maintenance_Cost_USD").alias("total_maintenance_cost"),
        sum("Workover_Cost_USD").alias("total_workover_cost"),
        sum("Total_OPEX_USD").alias("total_opex"),
        sum("BOE_Produced").alias("total_boe_produced"),
        avg("OPEX_Per_BOE").alias("avg_opex_per_boe"),
        sum("Operating_Netback_USD").alias("total_operating_netback"),
        avg("profit_margin_pct").alias("avg_profit_margin_pct"),
        sum(when(col("Is_Profitable") == "Yes", 1).otherwise(0)).alias("profitable_days"),
        max("cost_efficiency").alias("cost_efficiency_rating"),
        max("largest_cost_driver").alias("largest_cost_driver")
    ) \
    .withColumn("profitability_status",
        when(col("total_operating_netback") < 0, "Loss")
        .when(col("avg_profit_margin_pct") < 20, "Marginal")
        .when(col("avg_profit_margin_pct") < 50, "Profitable")
        .otherwise("Highly Profitable"))

fct_monthly_costs.write.format("delta").mode("overwrite") \
    .partitionBy("year_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.fct_monthly_costs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 3. fct_monthly_well_performance
# Partition: year_month
# Grain: year_month + Well_ID
# Join: Production + Costs
# ========================================

fct_monthly_well_performance = fct_monthly_production \
    .join(fct_monthly_costs, ["year_month", "Well_ID"], "left") \
    .withColumn("operating_margin", col("total_net_revenue") - col("total_opex")) \
    .withColumn("operating_margin_pct", 
        when(col("total_net_revenue") > 0, 
            round((col("operating_margin") / col("total_net_revenue")) * 100, 2))
        .otherwise(0)) \
    .withColumn("revenue_per_boe", 
        when(col("total_boe") > 0, round(col("total_net_revenue") / col("total_boe"), 2))
        .otherwise(0)) \
    .withColumn("cost_per_boe",
        when(col("total_boe") > 0, round(col("total_opex") / col("total_boe"), 2))
        .otherwise(0)) \
    .withColumn("well_tier",
        when((col("avg_daily_oil") > 100) & (col("operating_margin_pct") > 50), "Tier 1 - Strong")
        .when((col("avg_daily_oil") >= 50) & (col("operating_margin_pct") > 20), "Tier 2 - Average")
        .when((col("avg_daily_oil") >= 10) & (col("operating_margin_pct") >= 0), "Tier 3 - Weak")
        .otherwise("Tier 4 - Marginal"))

# Add decline rate (month-over-month)
window_spec = Window.partitionBy("Well_ID").orderBy("year_month")

fct_monthly_well_performance = fct_monthly_well_performance \
    .withColumn("prev_month_oil", lag("total_oil_bopd").over(window_spec)) \
    .withColumn("decline_rate_pct",
        when(col("prev_month_oil") > 0,
            round(((col("prev_month_oil") - col("total_oil_bopd")) / col("prev_month_oil")) * 100, 2))
        .otherwise(None)) \
    .drop("prev_month_oil")

fct_monthly_well_performance.write.format("delta").mode("overwrite") \
    .partitionBy("year_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.fct_monthly_well_performance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 4. fct_monthly_basin_summary
# Partition: year_month
# Grain: year_month + Basin
# ========================================

fct_monthly_basin_summary = fct_monthly_well_performance \
    .groupBy("year_month", "Basin") \
    .agg(
        countDistinct("Well_ID").alias("active_wells"),
        sum("total_oil_bopd").alias("basin_total_oil"),
        sum("total_gas_mcf").alias("basin_total_gas"),
        sum("total_boe").alias("basin_total_boe"),
        sum("total_net_revenue").alias("basin_total_revenue"),
        sum("total_opex").alias("basin_total_opex"),
        sum("operating_margin").alias("basin_operating_margin"),
        avg("avg_daily_oil").alias("avg_well_daily_oil"),
        avg("operating_margin_pct").alias("avg_operating_margin_pct"),
        avg("avg_water_cut").alias("basin_avg_water_cut"),
        sum(when(col("well_tier") == "Tier 1 - Strong", 1).otherwise(0)).alias("tier1_wells"),
        sum(when(col("well_tier") == "Tier 2 - Average", 1).otherwise(0)).alias("tier2_wells"),
        sum(when(col("well_tier") == "Tier 3 - Weak", 1).otherwise(0)).alias("tier3_wells"),
        sum(when(col("well_tier") == "Tier 4 - Marginal", 1).otherwise(0)).alias("tier4_wells")
    ) \
    .withColumn("basin_profitability",
        when(col("basin_operating_margin") < 0, "Loss Making")
        .when(col("avg_operating_margin_pct") < 30, "Low Margin")
        .when(col("avg_operating_margin_pct") < 50, "Profitable")
        .otherwise("High Margin"))

fct_monthly_basin_summary.write.format("delta").mode("overwrite") \
    .partitionBy("year_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.fct_monthly_basin_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 5. dim_well_current (No partition)
# Grain: Well_ID
# Current snapshot with lifetime totals
# ========================================

df_wells = spark.read.format("delta").load(f"{slv_path}dst_wells")

# Get latest production metrics
latest_production = fct_monthly_well_performance \
    .withColumn("row_num", row_number().over(Window.partitionBy("Well_ID").orderBy(desc("year_month")))) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Get lifetime totals
lifetime_totals = fct_monthly_well_performance \
    .groupBy("Well_ID") \
    .agg(
        sum("total_oil_bopd").alias("lifetime_oil"),
        sum("total_gas_mcf").alias("lifetime_gas"),
        sum("total_boe").alias("lifetime_boe"),
        sum("total_net_revenue").alias("lifetime_revenue"),
        sum("total_opex").alias("lifetime_opex"),
        sum("operating_margin").alias("lifetime_margin"),
        count("year_month").alias("months_produced")
    )

dim_well_current = df_wells \
    .join(latest_production.select("Well_ID", "year_month", "avg_daily_oil", "avg_water_cut", 
                                    "total_boe", "operating_margin_pct", "well_tier", "decline_rate_pct"), 
          "Well_ID", "left") \
    .join(lifetime_totals, "Well_ID", "left") \
    .withColumn("last_production_month", col("year_month")) \
    .drop("year_month") \
    .withColumn("lifetime_profitability",
        when(col("lifetime_margin") > 0, "Profitable").otherwise("Unprofitable"))

dim_well_current.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.dim_well_current")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 6. dim_equipment_current (No partition)
# Grain: Equipment_ID
# Current equipment snapshot
# ========================================

df_equip = spark.read.format("delta").load(f"{slv_path}dst_equipment")

dim_equipment_current = df_equip.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.dim_equipment_current")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 7. dim_employees_current (No partition)
# Grain: Employee_ID
# Current employee snapshot
# ========================================

df_emp = spark.read.format("delta").load(f"{slv_path}dst_employees")

dim_employees_current = df_emp.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dev_Gld_OnG_Lkh.dim_employees_current")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
