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

%pip install great-expectations==0.18.19 --break-system-packages
import importlib
importlib.invalidate_caches()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
from pyspark.sql.types import *
from datetime import datetime
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
import builtins
round = builtins.round

print("EMPLOYEES - VALIDATION WITH TRIM & DATE CHECKS")
print("=" * 80)

df = spark.read.format("delta").load(f"{dev_brnz_OnG_Lkh}dbt_employees")
gx_df = SparkDFDataset(df)

# Primary key checks
gx_df.expect_column_values_to_not_be_null("Employee_ID", result_format="SUMMARY")  # No nulls
gx_df.expect_column_values_to_be_unique("Employee_ID", result_format="SUMMARY")  # No duplicates
gx_df.expect_column_values_to_match_regex("Employee_ID", r"^EMP-\d{4}$", result_format="SUMMARY")  # Pattern match
gx_df.expect_column_values_to_not_match_regex("Employee_ID", r"^\s|\s$", result_format="SUMMARY")  # No spaces

# Name validations
gx_df.expect_column_values_to_not_be_null("First_Name", result_format="SUMMARY")  # No nulls
gx_df.expect_column_values_to_not_match_regex("First_Name", r"^\s|\s$", result_format="SUMMARY")  # No spaces
gx_df.expect_column_values_to_not_be_null("Last_Name", result_format="SUMMARY")  # No nulls
gx_df.expect_column_values_to_not_match_regex("Last_Name", r"^\s|\s$", result_format="SUMMARY")  # No spaces
gx_df.expect_column_value_lengths_to_be_between("First_Name", min_value=2, max_value=20, result_format="SUMMARY")  # Length check
gx_df.expect_column_value_lengths_to_be_between("Last_Name", min_value=2, max_value=20, result_format="SUMMARY")  # Length check

# Job validations
gx_df.expect_column_values_to_be_in_set("Job_Title",
    ["Drilling Engineer", "Production Engineer", "Petroleum Geologist",
     "Field Operator", "Maintenance Technician", "Safety Manager",
     "Reservoir Engineer", "Completion Engineer", "Operations Manager", "Lease Operator"],
    result_format="SUMMARY")  # Valid titles
gx_df.expect_column_values_to_be_in_set("Department",
    ["Engineering", "Geology", "Operations", "HSE"],
    result_format="SUMMARY")  # Valid departments
gx_df.expect_column_values_to_not_be_null("Job_Title", result_format="SUMMARY")  # No nulls
gx_df.expect_column_values_to_not_be_null("Department", result_format="SUMMARY")  # No nulls

# Salary validations
gx_df.expect_column_values_to_not_match_regex("Annual_Salary_USD", r"\$")  # No dollar
gx_df.expect_column_values_to_not_match_regex("Annual_Salary_USD", r",")  # No comma
gx_df.expect_column_values_to_be_between("Annual_Salary_USD", min_value=50000.0, max_value=200000.0, result_format="SUMMARY")  # Range check
gx_df.expect_column_values_to_not_be_null("Annual_Salary_USD", result_format="SUMMARY")  # No nulls

# Service validations
gx_df.expect_column_values_to_be_between("Years_Of_Service", min_value=0.0, max_value=40.0, result_format="SUMMARY")  # Range check
gx_df.expect_column_values_to_not_be_null("Years_Of_Service", result_format="SUMMARY")  # No nulls

# Status validations
gx_df.expect_column_values_to_be_in_set("Employment_Status",
    ["Full-Time", "Part-Time", "Contract", "Temporary"],
    result_format="SUMMARY")  # Valid status
gx_df.expect_column_values_to_be_in_set("Certification_Status",
    ["Current", "Renewal Needed", "Expired"],
    result_format="SUMMARY")  # Valid certification

# Assignment validations
gx_df.expect_column_values_to_be_between("Assigned_Wells_Count", min_value=0, max_value=10, result_format="SUMMARY")  # Range check
gx_df.expect_column_values_to_not_be_null("Assigned_Wells_Count", result_format="SUMMARY")  # No nulls

# Date validations
gx_df.expect_column_values_to_not_be_null("Hire_Date", result_format="SUMMARY")  # No nulls
gx_df.expect_column_values_to_match_regex("Hire_Date", r"^\d{4}-\d{2}-\d{2}$", result_format="SUMMARY")  # ISO format
gx_df.expect_column_values_to_not_be_null("Safety_Training_Date", result_format="SUMMARY")  # No nulls
gx_df.expect_column_values_to_match_regex("Safety_Training_Date", r"^\d{4}-\d{2}-\d{2}$", result_format="SUMMARY")  # ISO format

# Run validation
results = gx_df.validate()

# Format results
results_list = []
for r in results['results']:
    results_list.append({
        "check": r['expectation_config']['expectation_type'].replace('expect_column_values_', ''),
        "column": r['expectation_config']['kwargs'].get('column', 'TABLE'),
        "failed_count": r['result'].get('unexpected_count', 0) if 'result' in r else 0,
        "status": "Pass" if r['success'] else "Fail"
    })

results_df = spark.createDataFrame(results_list)
display(results_df.orderBy("status", "column"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get failed checks and show the bad records
failed_checks = results_df.filter(col("status") == "Fail")
# Get the columns that failed
failed_columns = [row['column'] for row in failed_checks.collect()]

bad_records = df.filter(
    col("Hire_Date").contains("/") |
    col("Safety_Training_Date").contains("/") |
    col("Employee_ID").rlike(r"^\s|\s$") |
    col("First_Name").rlike(r"^\s|\s$") |
    col("Last_Name").rlike(r"^\s|\s$")
)

display(bad_records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
