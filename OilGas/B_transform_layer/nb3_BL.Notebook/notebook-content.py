# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f67b15ec-0153-4344-a458-411122433c29",
# META       "default_lakehouse_name": "dev_Slv_OnG_Lkh",
# META       "default_lakehouse_workspace_id": "29b83063-d09f-4997-ab9d-c075a0f36775",
# META       "known_lakehouses": [
# META         {
# META           "id": "f67b15ec-0153-4344-a458-411122433c29"
# META         },
# META         {
# META           "id": "c2a56730-e48e-40d5-8eb7-15fb1fe397e2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bronze lakehouse path
dev_brnz_OnG_Lkh = "abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Tables/"

# ========================================
# 1. EMPLOYEES
# ========================================
# Read from: dbt_employees (Bronze)
# Write to: dst_employees (Silver)

df_emp = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_employees")

df_emp \
    .withColumn("Employee_ID", trim(col("Employee_ID"))) \
    .withColumn("First_Name", trim(col("First_Name"))) \
    .withColumn("Last_Name", trim(col("Last_Name"))) \
    .withColumn("Hire_Date", 
        when(col("Hire_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Hire_Date"))
        .when(col("Hire_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Hire_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Hire_Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("Safety_Training_Date",
        when(col("Safety_Training_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Safety_Training_Date"))
        .when(col("Safety_Training_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Safety_Training_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Safety_Training_Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("tenure_years", round(datediff(current_date(), to_date(col("Hire_Date"))) / 365, 1)) \
    .withColumn("training_status", when(datediff(current_date(), to_date(col("Safety_Training_Date"))) > 365, "Expired").otherwise("Current")) \
    .withColumn("salary_band", when(col("Annual_Salary_USD") < 70000, "Junior").when(col("Annual_Salary_USD") <= 120000, "Mid").otherwise("Senior")) \
    .withColumn("is_field_role", when(col("Job_Title").rlike("Operator|Technician"), True).otherwise(False)) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_Slv_OnG_Lkh.dbo.dst_employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 2. WELLS
# ========================================
# Read from: dbt_wells_master (Bronze)
# Write to: dst_wells (Silver)

df_wells = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_wells_master")

df_wells \
    .withColumn("Well_ID", trim(col("Well_ID"))) \
    .withColumn("API_Number", trim(col("API_Number"))) \
    .withColumn("Spud_Date", when(col("Spud_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Spud_Date")).when(col("Spud_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Spud_Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).otherwise(date_format(to_date(col("Spud_Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("First_Production_Date", when(col("First_Production_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("First_Production_Date")).when(col("First_Production_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("First_Production_Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).otherwise(date_format(to_date(col("First_Production_Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("is_horizontal", when(col("Lateral_Length_Feet") > 1000, True).otherwise(False)) \
    .withColumn("well_age_years", round(datediff(current_date(), to_date(col("First_Production_Date"))) / 365, 1)) \
    .withColumn("well_maturity", when(col("well_age_years") < 1, "New").when(col("well_age_years") <= 3, "Mature").otherwise("Declining")) \
    .withColumn("cost_per_foot", round(col("Drilling_Cost_USD") / col("Total_Depth_Feet"), 2)) \
    .withColumn("depth_category", when(col("Total_Depth_Feet") < 10000, "Shallow").when(col("Total_Depth_Feet") <= 15000, "Medium").otherwise("Deep")) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_Slv_OnG_Lkh.dbo.dst_wells")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 3. PRODUCTION
# ========================================
# Read from: dbt_production_daily (Bronze)
# Write to: dst_production (Silver)

df_prod = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_production_daily")

df_prod \
    .withColumn("Date", when(col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Date")).when(col("Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).otherwise(date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("BOE", round(col("Oil_Production_BOPD") + (col("Gas_Production_MCF") / 6), 2)) \
    .withColumn("revenue_per_BOE", when(col("BOE") > 0, round(col("Net_Revenue_USD") / col("BOE"), 2)).otherwise(0)) \
    .withColumn("actual_water_cut", round((col("Water_Production_BWPD") / (col("Oil_Production_BOPD") + col("Water_Production_BWPD"))) * 100, 1)) \
    .withColumn("high_water_cut_flag", when(col("actual_water_cut") > 80, True).otherwise(False)) \
    .withColumn("low_producer_flag", when(col("Oil_Production_BOPD") < 10, True).otherwise(False)) \
    .withColumn("production_status", when(col("Oil_Production_BOPD") > 100, "Strong").when(col("Oil_Production_BOPD") >= 50, "Average").when(col("Oil_Production_BOPD") >= 10, "Weak").otherwise("Marginal")) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_Slv_OnG_Lkh.dbo.dst_production")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 4. COSTS
# ========================================
# Read from: dbt_operating_costs (Bronze)
# Write to: dst_costs (Silver)

# Fix for COSTS table - largest_cost_driver

df_costs = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_operating_costs")

df_costs \
    .withColumn("Date", when(col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Date")).when(col("Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).otherwise(date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("profit_margin_pct", when((col("Operating_Netback_USD") + col("Total_OPEX_USD")) > 0, round((col("Operating_Netback_USD") / (col("Operating_Netback_USD") + col("Total_OPEX_USD"))) * 100, 2)).otherwise(0)) \
    .withColumn("cost_efficiency", when(col("OPEX_Per_BOE") < 30, "Efficient").when(col("OPEX_Per_BOE") <= 50, "Average").otherwise("High Cost")) \
    .withColumn("largest_cost_driver",
        when((col("Labor_Cost_USD") >= col("Power_Cost_USD")) & 
             (col("Labor_Cost_USD") >= col("Chemical_Cost_USD")) & 
             (col("Labor_Cost_USD") >= col("Maintenance_Cost_USD")) & 
             (col("Labor_Cost_USD") >= col("Water_Disposal_Cost_USD")) & 
             (col("Labor_Cost_USD") >= col("Workover_Cost_USD")), "Labor")
        .when((col("Power_Cost_USD") >= col("Chemical_Cost_USD")) & 
              (col("Power_Cost_USD") >= col("Maintenance_Cost_USD")) & 
              (col("Power_Cost_USD") >= col("Water_Disposal_Cost_USD")) & 
              (col("Power_Cost_USD") >= col("Workover_Cost_USD")), "Power")
        .when((col("Chemical_Cost_USD") >= col("Maintenance_Cost_USD")) & 
              (col("Chemical_Cost_USD") >= col("Water_Disposal_Cost_USD")) & 
              (col("Chemical_Cost_USD") >= col("Workover_Cost_USD")), "Chemical")
        .when((col("Maintenance_Cost_USD") >= col("Water_Disposal_Cost_USD")) & 
              (col("Maintenance_Cost_USD") >= col("Workover_Cost_USD")), "Maintenance")
        .when(col("Water_Disposal_Cost_USD") >= col("Workover_Cost_USD"), "Water_Disposal")
        .otherwise("Workover")) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_Slv_OnG_Lkh.dbo.dst_costs")# ========================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. EQUIPMENT
# ========================================
# Read from: dbt_equipment (Bronze)
# Write to: dst_equipment (Silver)

df_equip = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_equipment")

df_equip \
    .withColumn("Install_Date", when(col("Install_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Install_Date")).when(col("Install_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Install_Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).otherwise(date_format(to_date(col("Install_Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("Last_Maintenance_Date", when(col("Last_Maintenance_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Last_Maintenance_Date")).when(col("Last_Maintenance_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Last_Maintenance_Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).otherwise(date_format(to_date(col("Last_Maintenance_Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))) \
    .withColumn("equipment_age_years", round(datediff(current_date(), to_date(col("Install_Date"))) / 365, 1)) \
    .withColumn("utilization_pct", round((col("Runtime_Hours") / col("Expected_Life_Hours")) * 100, 1)) \
    .withColumn("remaining_life_hours", col("Expected_Life_Hours") - col("Runtime_Hours")) \
    .withColumn("maintenance_overdue", when(col("Days_Since_Maintenance") > 90, True).otherwise(False)) \
    .withColumn("critical_equipment", when((col("Health_Score") < 50) | (col("Failure_Probability_PCT") > 70), True).otherwise(False)) \
    .withColumn("replacement_priority", when((col("Health_Score") < 40) & (col("Failure_Probability_PCT") > 70), "Urgent").when((col("Health_Score") <= 60) | (col("Failure_Probability_PCT") >= 50), "High").when(col("Health_Score") <= 80, "Medium").otherwise("Low")) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_Slv_OnG_Lkh.dbo.dst_equipment")

print("✅ Silver transformation complete!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
