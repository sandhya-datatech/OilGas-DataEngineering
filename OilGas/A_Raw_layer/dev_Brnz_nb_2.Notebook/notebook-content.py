# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c2a56730-e48e-40d5-8eb7-15fb1fe397e2",
# META       "default_lakehouse_name": "dev_brnz_OnG_Lkh",
# META       "default_lakehouse_workspace_id": "29b83063-d09f-4997-ab9d-c075a0f36775",
# META       "known_lakehouses": [
# META         {
# META           "id": "c2a56730-e48e-40d5-8eb7-15fb1fe397e2"
# META         },
# META         {
# META           "id": "f67b15ec-0153-4344-a458-411122433c29"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/bronze_raw/employees/part-00000-9c5796fa-a340-4861-90bd-82c482b61876-c000.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze_raw/employees/part-00000-9c5796fa-a340-4861-90bd-82c482b61876-c000.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

employees_schema = StructType([
    StructField("Employee_ID", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Job_Title", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Hire_Date", StringType(), True),
    StructField("Years_Of_Service", DecimalType(18,2), True),
    StructField("Annual_Salary_USD", DecimalType(18,2), True),
    StructField("Employment_Status", StringType(), True),
    StructField("Certification_Status", StringType(), True),
    StructField("Assigned_Wells_Count", IntegerType(), True),
    StructField("Assigned_Wells", StringType(), True),
    StructField("Safety_Training_Date", StringType(), True)
])

# Read CSV with schema
df = spark.read.format("csv").option("header", "true").schema(employees_schema).load("abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files/bronze_raw/employees/part-00000-9c5796fa-a340-4861-90bd-82c482b61876-c000.csv")
df.printSchema()
df.write.format("delta").mode("overwrite").saveAsTable("dbt_employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

from pyspark.sql.types import *
equipment_schema = StructType([
    StructField("Equipment_ID", StringType(), True),
    StructField("Well_ID", StringType(), True),
    StructField("Equipment_Type_Code", StringType(), True),
    StructField("Equipment_Type", StringType(), True),
    StructField("Manufacturer", StringType(), True),
    StructField("Model_Number", StringType(), True),
    StructField("Serial_Number", StringType(), True),
    StructField("Install_Date", StringType(), True),
    StructField("Days_In_Service", IntegerType(), True),
    StructField("Purchase_Cost_USD", DecimalType(18,2), True),
    StructField("Runtime_Hours", IntegerType(), True),
    StructField("Expected_Life_Hours", IntegerType(), True),
    StructField("Health_Score", DoubleType(), True),
    StructField("Last_Maintenance_Date", StringType(), True),
    StructField("Days_Since_Maintenance", IntegerType(), True),
    StructField("Maintenance_Status", StringType(), True),
    StructField("Failure_Probability_PCT", DoubleType(), True),
    StructField("Vibration_Level", DoubleType(), True),
    StructField("Temperature_F", DoubleType(), True),
    StructField("Pressure_PSI", DoubleType(), True)
])

# Read CSV with schema
df = spark.read.format("csv").option("header", "true").schema(equipment_schema).load("abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files/bronze_raw/equipment")
df.printSchema()
df.write.format("delta").mode("overwrite").saveAsTable("dbt_equipment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
costs_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Well_ID", StringType(), True),
    StructField("BOE_Produced", DoubleType(), True),
    StructField("Labor_Cost_USD", DecimalType(18,2), True),
    StructField("Power_Cost_USD", DecimalType(18,2), True),
    StructField("Chemical_Cost_USD", DecimalType(18,2), True),
    StructField("Water_Disposal_Cost_USD", DecimalType(18,2), True),
    StructField("Maintenance_Cost_USD", DecimalType(18,2), True),
    StructField("Workover_Cost_USD", DecimalType(18,2), True),
    StructField("Total_OPEX_USD",DecimalType(18,2), True),
    StructField("OPEX_Per_BOE", DoubleType(), True),
    StructField("Operating_Netback_USD", DecimalType(18,2), True),
    StructField("Is_Profitable", StringType(), True)
])
# Read CSV with schema
df = spark.read.format("csv").option("header", "true").schema(costs_schema).load("abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files/bronze_raw/operating_costs")
df.printSchema()
df.write.format("delta").mode("overwrite").saveAsTable("dbt_operating_costs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

production_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Well_ID", StringType(), True),
    StructField("Well_Name", StringType(), True),
    StructField("Basin", StringType(), True),
    StructField("Well_Type", StringType(), True),
    StructField("Days_On_Production", IntegerType(), True),
    StructField("Oil_Production_BOPD", DoubleType(), True),
    StructField("Gas_Production_MCF", DoubleType(), True),
    StructField("Water_Production_BWPD", DoubleType(), True),
    StructField("Gas_Oil_Ratio", IntegerType(), True),
    StructField("Water_Cut_PCT", DoubleType(), True),
    StructField("Oil_Price_USD", DecimalType(18,2), True),
    StructField("Gas_Price_USD", DecimalType(18,2), True),
    StructField("Gross_Revenue_USD", DecimalType(18,2), True),
    StructField("Royalty_USD", DecimalType(18,2), True),
    StructField("Severance_Tax_USD", DecimalType(18,2), True),
    StructField("Net_Revenue_USD", DecimalType(18,2), True),
    StructField("Well_Status", StringType(), True),
    StructField("Decline_Factor", DoubleType(), True)
])
# Read CSV with schema
df = spark.read.format("csv").option("header", "true").schema(production_schema).load("abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files/bronze_raw/production_daily")
df.printSchema()
df.write.format("delta").mode("overwrite").saveAsTable("dbt_production_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

wells_schema = StructType([
    StructField("Well_ID", StringType(), True),
    StructField("Well_Name", StringType(), True),
    StructField("API_Number", StringType(), True),
    StructField("Operator", StringType(), True),
    StructField("Basin", StringType(), True),
    StructField("State", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Well_Type", StringType(), True),
    StructField("Total_Depth_Feet", IntegerType(), True),
    StructField("Lateral_Length_Feet", IntegerType(), True),
    StructField("Spud_Date", StringType(), True),
    StructField("First_Production_Date", StringType(), True),
    StructField("Days_On_Production", IntegerType(), True),
    StructField("Drilling_Cost_USD", IntegerType(), True),
    StructField("Initial_Production_BOPD", DoubleType(), True),
    StructField("Well_Status", StringType(), True),
    StructField("Ownership_Type", StringType(), True),
    StructField("Royalty_Interest_PCT", DoubleType(), True)
])
# Read CSV with schema
df = spark.read.format("csv").option("header", "true").schema(wells_schema).load("abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files/bronze_raw/wells_master")
df.printSchema()
df.write.format("delta").mode("overwrite").saveAsTable("dbt_wells_master")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
