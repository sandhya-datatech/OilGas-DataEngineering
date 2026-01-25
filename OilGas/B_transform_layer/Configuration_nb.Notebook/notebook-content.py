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

## Brnz lakehouse path
dev_brnz_OnG_Lkh="abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Tables/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# VALIDATION CONFIGURATION
# Notebook: validation_config
# Purpose: Central config for all table validations
# ========================================

validation_config = {
    # ========================================
    # 1. EMPLOYEES TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_employees": [
        {'type': 'not_null', 'column': 'Employee_ID'},
        {'type': 'unique', 'column': 'Employee_ID'},
        {'type': 'regex_match', 'column': 'Employee_ID', 'pattern': r'^EMP-\d{4}$'},
        {'type': 'regex_not_match', 'column': 'Employee_ID', 'pattern': r'^\s|\s$'},
        {'type': 'not_null', 'column': 'First_Name'},
        {'type': 'not_null', 'column': 'Last_Name'},
        {'type': 'regex_not_match', 'column': 'First_Name', 'pattern': r'^\s|\s$'},
        {'type': 'regex_not_match', 'column': 'Last_Name', 'pattern': r'^\s|\s$'},
        {'type': 'length_between', 'column': 'First_Name', 'min': 2, 'max': 20},
        {'type': 'length_between', 'column': 'Last_Name', 'min': 2, 'max': 20},
        {'type': 'not_null', 'column': 'Job_Title'},
        {'type': 'not_null', 'column': 'Department'},
        {'type': 'in_set', 'column': 'Job_Title', 'values': [
            "Drilling Engineer", "Production Engineer", "Petroleum Geologist",
            "Field Operator", "Maintenance Technician", "Safety Manager",
            "Reservoir Engineer", "Completion Engineer", "Operations Manager", "Lease Operator"
        ]},
        {'type': 'in_set', 'column': 'Department', 'values': ["Engineering", "Geology", "Operations", "HSE"]},
        {'type': 'not_null', 'column': 'Annual_Salary_USD'},
        {'type': 'between', 'column': 'Annual_Salary_USD', 'min': 50000.0, 'max': 200000.0},
        {'type': 'not_null', 'column': 'Years_Of_Service'},
        {'type': 'between', 'column': 'Years_Of_Service', 'min': 0.0, 'max': 40.0},
        {'type': 'in_set', 'column': 'Employment_Status', 'values': ["Full-Time", "Part-Time", "Contract", "Temporary"]},
        {'type': 'in_set', 'column': 'Certification_Status', 'values': ["Current", "Renewal Needed", "Expired"]},
        {'type': 'not_null', 'column': 'Assigned_Wells_Count'},
        {'type': 'between', 'column': 'Assigned_Wells_Count', 'min': 0, 'max': 10},
        {'type': 'not_null', 'column': 'Hire_Date'},
        {'type': 'not_null', 'column': 'Safety_Training_Date'},
        {'type': 'regex_match', 'column': 'Hire_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'regex_match', 'column': 'Safety_Training_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
    ],
    
    # ========================================
    # 2. WELLS MASTER TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_wells_master": [
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'unique', 'column': 'Well_ID'},
        {'type': 'unique', 'column': 'API_Number'},
        {'type': 'regex_match', 'column': 'Well_ID', 'pattern': r'^WELL-\d{4}$'},
        {'type': 'regex_not_match', 'column': 'Well_ID', 'pattern': r'^\s|\s$'},
        {'type': 'not_null', 'column': 'Well_Name'},
        {'type': 'not_null', 'column': 'API_Number'},
        {'type': 'not_null', 'column': 'Basin'},
        {'type': 'not_null', 'column': 'Well_Type'},
        {'type': 'not_null', 'column': 'Well_Status'},
        {'type': 'in_set', 'column': 'Basin', 'values': [
            "Permian Basin", "Bakken Formation", "Eagle Ford", "Marcellus Shale",
            "STACK Play", "Delaware Basin", "Haynesville", "Niobrara"
        ]},
        {'type': 'in_set', 'column': 'Well_Type', 'values': ["Horizontal", "Vertical"]},
        {'type': 'in_set', 'column': 'Well_Status', 'values': ["Active", "Shut In"]},
        {'type': 'between', 'column': 'Total_Depth_Feet', 'min': 1000, 'max': 20000},
        {'type': 'between', 'column': 'Lateral_Length_Feet', 'min': 0, 'max': 15000},
        {'type': 'between', 'column': 'Initial_Production_BOPD', 'min': 0.0, 'max': 1000.0},
        {'type': 'between', 'column': 'Latitude', 'min': 25.0, 'max': 50.0},
        {'type': 'between', 'column': 'Longitude', 'min': -125.0, 'max': -65.0},
        {'type': 'not_null', 'column': 'Spud_Date'},
        {'type': 'regex_match', 'column': 'Spud_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'regex_match', 'column': 'First_Production_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
    ],
    
    # ========================================
    # 3. PRODUCTION DAILY TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_production_daily": [
        {'type': 'not_null', 'column': 'Date'},
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'regex_match', 'column': 'Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'not_null', 'column': 'Oil_Production_BOPD'},
        {'type': 'between', 'column': 'Oil_Production_BOPD', 'min': 0.0, 'max': 1000.0},
        {'type': 'between', 'column': 'Gas_Production_MCF', 'min': 0.0, 'max': 5000.0},
        {'type': 'between', 'column': 'Water_Production_BWPD', 'min': 0.0, 'max': 2000.0},
        {'type': 'between', 'column': 'Water_Cut_PCT', 'min': 0.0, 'max': 100.0},
        {'type': 'between', 'column': 'Oil_Price_USD', 'min': 30.0, 'max': 150.0},
        {'type': 'between', 'column': 'Gas_Price_USD', 'min': 1.0, 'max': 20.0},
        {'type': 'not_null', 'column': 'Net_Revenue_USD'},
        {'type': 'between', 'column': 'Net_Revenue_USD', 'min': 0.0, 'max': 100000.0},
        {'type': 'not_null', 'column': 'Well_Name'},
        {'type': 'not_null', 'column': 'Basin'},
        {'type': 'in_set', 'column': 'Well_Status', 'values': ["Active", "Shut In"]},
    ],
    
    # ========================================
    # 4. OPERATING COSTS TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_operating_costs": [
        {'type': 'not_null', 'column': 'Date'},
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'regex_match', 'column': 'Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'not_null', 'column': 'Total_OPEX_USD'},
        {'type': 'between', 'column': 'Labor_Cost_USD', 'min': 0.0, 'max': 10000.0},
        {'type': 'between', 'column': 'Power_Cost_USD', 'min': 0.0, 'max': 5000.0},
        {'type': 'between', 'column': 'Chemical_Cost_USD', 'min': 0.0, 'max': 5000.0},
        {'type': 'between', 'column': 'Water_Disposal_Cost_USD', 'min': 0.0, 'max': 5000.0},
        {'type': 'between', 'column': 'Maintenance_Cost_USD', 'min': 0.0, 'max': 10000.0},
        {'type': 'between', 'column': 'Workover_Cost_USD', 'min': 0.0, 'max': 50000.0},
        {'type': 'between', 'column': 'Total_OPEX_USD', 'min': 0.0, 'max': 100000.0},
        {'type': 'not_null', 'column': 'BOE_Produced'},
        {'type': 'between', 'column': 'BOE_Produced', 'min': 0.0, 'max': 2000.0},
        {'type': 'between', 'column': 'OPEX_Per_BOE', 'min': 0.0, 'max': 500.0},
        {'type': 'in_set', 'column': 'Is_Profitable', 'values': ["Yes", "No"]},
    ],
    
    # ========================================
    # 5. EQUIPMENT TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_equipment": [
        {'type': 'not_null', 'column': 'Equipment_ID'},
        {'type': 'unique', 'column': 'Equipment_ID'},
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'not_null', 'column': 'Equipment_Type'},
        {'type': 'not_null', 'column': 'Equipment_Type_Code'},
        {'type': 'not_null', 'column': 'Health_Score'},
        {'type': 'between', 'column': 'Health_Score', 'min': 0.0, 'max': 100.0},
        {'type': 'between', 'column': 'Failure_Probability_PCT', 'min': 0.0, 'max': 100.0},
        {'type': 'in_set', 'column': 'Maintenance_Status', 'values': ["Overdue", "Scheduled","Good","Unknown"]},
        {'type': 'not_null', 'column': 'Runtime_Hours'},
        {'type': 'between', 'column': 'Runtime_Hours', 'min': 0, 'max': 100000},
        {'type': 'between', 'column': 'Expected_Life_Hours', 'min': 1000, 'max': 100000},
        {'type': 'between', 'column': 'Vibration_Level', 'min': 0.0, 'max': 10.0},
        {'type': 'between', 'column': 'Temperature_F', 'min': 0.0, 'max': 300.0},
        {'type': 'between', 'column': 'Pressure_PSI', 'min': 0.0, 'max': 5000.0},
        {'type': 'not_null', 'column': 'Install_Date'},
        {'type': 'regex_match', 'column': 'Install_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'regex_match', 'column': 'Last_Maintenance_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
    ]
}

print("✅ Validation config loaded with ABFSS paths!")
print(f"📋 Tables configured: {len(validation_config)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# ========================================
# SILVER LAYER VALIDATION CONFIGURATION
# Notebook: silver_validation_config
# Purpose: Validate cleaned Silver tables
# ========================================


validation_config = {
    # ========================================
    # 1. SILVER EMPLOYEES TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_employees": [
        {'type': 'not_null', 'column': 'Employee_ID'},
        {'type': 'unique', 'column': 'Employee_ID'},
        {'type': 'regex_match', 'column': 'Employee_ID', 'pattern': r'^EMP-\d{4}$'},
        {'type': 'regex_not_match', 'column': 'Employee_ID', 'pattern': r'^\s|\s$'},
        {'type': 'not_null', 'column': 'First_Name'},
        {'type': 'not_null', 'column': 'Last_Name'},
        {'type': 'regex_not_match', 'column': 'First_Name', 'pattern': r'^\s|\s$'},
        {'type': 'regex_not_match', 'column': 'Last_Name', 'pattern': r'^\s|\s$'},
        {'type': 'not_null', 'column': 'Job_Title'},
        {'type': 'not_null', 'column': 'Department'},
        {'type': 'not_null', 'column': 'Assigned_Wells_Count'},
        {'type': 'not_null', 'column': 'Hire_Date'},
        {'type': 'not_null', 'column': 'Safety_Training_Date'},
        {'type': 'regex_match', 'column': 'Hire_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'regex_match', 'column': 'Safety_Training_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
    ],
    
    # ========================================
    # 2. SILVER WELLS TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_wells_master": [
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'unique', 'column': 'Well_ID'},
        {'type': 'unique', 'column': 'API_Number'},
        {'type': 'regex_match', 'column': 'Well_ID', 'pattern': r'^WELL-\d{4}$'},
        {'type': 'regex_not_match', 'column': 'Well_ID', 'pattern': r'^\s|\s$'},
        {'type': 'not_null', 'column': 'Well_Name'},
        {'type': 'not_null', 'column': 'Basin'},
        {'type': 'not_null', 'column': 'Well_Type'},
        {'type': 'not_null', 'column': 'Well_Status'},
        {'type': 'in_set', 'column': 'Basin', 'values': [
            "Permian Basin", "Bakken Formation", "Eagle Ford", "Marcellus Shale",
            "STACK Play", "Delaware Basin", "Haynesville", "Niobrara"
        ]},
        {'type': 'not_null', 'column': 'Spud_Date'},
        {'type': 'regex_match', 'column': 'Spud_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'regex_match', 'column': 'First_Production_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
    ],
    
    # ========================================
    # 3. SILVER PRODUCTION TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_production_daily": [
        {'type': 'not_null', 'column': 'Date'},
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'regex_match', 'column': 'Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'not_null', 'column': 'Well_Name'},
        {'type': 'not_null', 'column': 'Basin'},
    ],
    
    # ========================================
    # 4. SILVER COSTS TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_operating_costs": [
        {'type': 'not_null', 'column': 'Date'},
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'regex_match', 'column': 'Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'not_null', 'column': 'BOE_Produced'},
    ],
    
    # ========================================
    # 5. SILVER EQUIPMENT TABLE
    # ========================================
    f"{dev_brnz_OnG_Lkh}dbt_equipment": [
        {'type': 'not_null', 'column': 'Equipment_ID'},
        {'type': 'unique', 'column': 'Equipment_ID'},
        {'type': 'not_null', 'column': 'Well_ID'},
        {'type': 'not_null', 'column': 'Equipment_Type'},
        {'type': 'not_null', 'column': 'Equipment_Type_Code'},
        {'type': 'not_null', 'column': 'Health_Score'},
        {'type': 'in_set', 'column': 'Maintenance_Status', 'values': ["Overdue", "Scheduled", "Good", "Unknown"]},
        {'type': 'not_null', 'column': 'Runtime_Hours'},
        {'type': 'not_null', 'column': 'Install_Date'},
        {'type': 'regex_match', 'column': 'Install_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        {'type': 'regex_match', 'column': 'Last_Maintenance_Date', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
