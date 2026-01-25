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

%run /Configuration_nb

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(type(validation_config))
print(len(validation_config))
print(list(validation_config.keys()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# VALIDATION FUNCTION - CLEAN OUTPUT
# ========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, sum, when, round 
from datetime import datetime
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
import builtins

def validate_table(table_path, validation_rules, table_display_name=None):
    """Validate table - returns results DataFrame only"""
    
    if table_display_name is None:
        table_display_name = table_path.split('/')[-1]
    
    # Read table
    df = spark.read.format("delta").load(table_path)
    gx_df = SparkDFDataset(df)
    
    # Apply validation rules
    for rule in validation_rules:
        rule_type = rule['type']
        column = rule.get('column')
        
        if rule_type == 'not_null':
            gx_df.expect_column_values_to_not_be_null(column)
        elif rule_type == 'unique':
            gx_df.expect_column_values_to_be_unique(column)
        elif rule_type == 'regex_match':
            gx_df.expect_column_values_to_match_regex(column, rule['pattern'])
        elif rule_type == 'regex_not_match':
            gx_df.expect_column_values_to_not_match_regex(column, rule['pattern'])
        elif rule_type == 'in_set':
            gx_df.expect_column_values_to_be_in_set(column, rule['values'])
        elif rule_type == 'between':
            gx_df.expect_column_values_to_be_between(column, min_value=rule['min'], max_value=rule['max'])
        elif rule_type == 'length_between':
            gx_df.expect_column_value_lengths_to_be_between(column, min_value=rule['min'], max_value=rule['max'])
    
    # Run validation
    results = gx_df.validate()
    
    # Format results
    results_list = []
    for r in results['results']:
        results_list.append({
            "table": table_display_name,
            "check_type": r['expectation_config']['expectation_type'].replace('expect_column_values_', '').replace('_', ' ').title(),
            "column": r['expectation_config']['kwargs'].get('column', 'TABLE'),
            "status": "Pass" if r['success'] else "Fail",
            "failed_count": r['result'].get('unexpected_count', 0) if 'result' in r else 0
        })
    
    return spark.createDataFrame(results_list)


# ========================================
# RUN VALIDATION - TABLE OUTPUT ONLY
# ========================================

all_results = []

# Validate each table
for table_path, rules in validation_config.items():
    table_name = table_path.split('/')[-1]
    result_df = validate_table(table_path, rules, table_name)
    all_results.append(result_df)

# Combine results
combined_results = all_results[0]
for result in all_results[1:]:
    combined_results = combined_results.union(result)

# Show only failed checks
print("❌ FAILED CHECKS:")
display(combined_results.filter(col("status") == "Fail").orderBy("table", "failed_count", ascending=False))

# Show summary by table
print("\n📊 SUMMARY BY TABLE:")
display(
    combined_results.groupBy("table")
    .agg(
        count("*").alias("total_checks"),
        sum(when(col("status") == "Pass", 1).otherwise(0)).alias("passed"),
        sum(when(col("status") == "Fail", 1).otherwise(0)).alias("failed"),
        sum("failed_count").alias("records_with_issues")
    )
    .withColumn("success_rate_pct", round((col("passed") / col("total_checks") * 100), 1))
    .orderBy("success_rate_pct")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_failed_rows_by_table_name(table_name, validation_config):
    """
    Get failed rows dynamically by just specifying the table name.
    Finds the table path and rules automatically from validation_config.
    """
    # 1️⃣ Find the table path in validation_config
    table_path = next((path for path in validation_config if path.split('/')[-1] == table_name), None)
    
    if table_path is None:
        print(f"Table {table_name} not found in validation_config.")
        return None
    
    rules = validation_config[table_path]
    
    # 2️⃣ Load the table and apply the rules
    df = spark.read.format("delta").load(table_path)
    
    conditions = []
    for rule in rules:
        col_name = rule.get('column')
        rule_type = rule['type']
        
        if rule_type == 'not_null':
            conditions.append(col(col_name).isNull())
        elif rule_type == 'regex_match':
            conditions.append(~col(col_name).rlike(rule['pattern']))
        elif rule_type == 'regex_not_match':
            conditions.append(col(col_name).rlike(rule['pattern']))
        elif rule_type == 'between':
            conditions.append((col(col_name) < rule['min']) | (col(col_name) > rule['max']))
        elif rule_type == 'length_between':
            conditions.append((length(col(col_name)) < rule['min']) | (length(col(col_name)) > rule['max']))
        elif rule_type == 'in_set':
            conditions.append(~col(col_name).isin(rule['values']))
    
    if not conditions:
        return df.limit(0)
    
    from functools import reduce
    df_failed = df.filter(reduce(lambda x, y: x | y, conditions))
    
    return df_failed


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


failed_equipment_df = get_failed_rows_by_table_name("dbt_production_daily", validation_config)
display(failed_equipment_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

failed_employees_df = get_failed_rows_by_table_name("dbt_employees", validation_config)
display(failed_employees_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM dev_brnz_OnG_Lkh.dbt_production_daily ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
