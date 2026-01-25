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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

## Brnz lakehouse path
dev_brnz_OnG_Lkh="abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Tables/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
# 1. EMPLOYEES - Fix trim and dates only
df = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_employees")
df.select("*", 
    trim(col("Employee_ID")).alias("Employee_ID_clean"),
    trim(col("First_Name")).alias("First_Name_clean"),
    trim(col("Last_Name")).alias("Last_Name_clean"),
    when(col("Hire_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Hire_Date"))
        .when(col("Hire_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Hire_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Hire_Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Hire_Date_clean"),
    when(col("Safety_Training_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Safety_Training_Date"))
        .when(col("Safety_Training_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Safety_Training_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Safety_Training_Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Safety_Training_Date_clean")
).drop("Employee_ID", "First_Name", "Last_Name", "Hire_Date", "Safety_Training_Date") \
 .withColumnRenamed("Employee_ID_clean", "Employee_ID") \
 .withColumnRenamed("First_Name_clean", "First_Name") \
 .withColumnRenamed("Last_Name_clean", "Last_Name") \
 .withColumnRenamed("Hire_Date_clean", "Hire_Date") \
 .withColumnRenamed("Safety_Training_Date_clean", "Safety_Training_Date")
df.write.format("delta").mode("overwrite").saveAsTable("dst_employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. WELLS
df = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_wells_master")
df.select("*",
    trim(col("Well_ID")).alias("Well_ID_clean"),
    trim(col("API_Number")).alias("API_Number_clean"),
    when(col("Spud_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Spud_Date"))
        .when(col("Spud_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Spud_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Spud_Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Spud_Date_clean"),
    when(col("First_Production_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("First_Production_Date"))
        .when(col("First_Production_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("First_Production_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("First_Production_Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("First_Production_Date_clean")
).drop("Well_ID", "API_Number", "Spud_Date", "First_Production_Date") \
 .withColumnRenamed("Well_ID_clean", "Well_ID") \
 .withColumnRenamed("API_Number_clean", "API_Number") \
 .withColumnRenamed("Spud_Date_clean", "Spud_Date") \
 .withColumnRenamed("First_Production_Date_clean", "First_Production_Date")
df.write.format("delta").mode("overwrite").saveAsTable("dst_wells")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. PRODUCTION
df = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_production_daily")
df.select("*",
    when(col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Date"))
        .when(col("Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Date_clean")
).drop("Date").withColumnRenamed("Date_clean", "Date")
df.write.format("delta").mode("overwrite").saveAsTable("dst_production")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. COSTS
df = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_operating_costs")
df.select("*",
    when(col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Date"))
        .when(col("Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Date_clean")
).drop("Date").withColumnRenamed("Date_clean", "Date")
df.write.format("delta").mode("overwrite").saveAsTable("dst_costs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. EQUIPMENT
df = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_equipment")
df.select("*",
    when(col("Install_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Install_Date"))
        .when(col("Install_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Install_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Install_Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Install_Date_clean"),
    when(col("Last_Maintenance_Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), col("Last_Maintenance_Date"))
        .when(col("Last_Maintenance_Date").rlike(r"^\d{2}/\d{2}/\d{4}$"), date_format(to_date(col("Last_Maintenance_Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
        .otherwise(date_format(to_date(col("Last_Maintenance_Date"), "dd/MM/yyyy"), "yyyy-MM-dd")).alias("Last_Maintenance_Date_clean")
).drop("Install_Date", "Last_Maintenance_Date") \
 .withColumnRenamed("Install_Date_clean", "Install_Date") \
 .withColumnRenamed("Last_Maintenance_Date_clean", "Last_Maintenance_Date")
df.write.format("delta").mode("overwrite").saveAsTable("dst_equipment")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
