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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# TEST CONFIGURATION
print("🧪 Testing Fabric setup...")

# YOUR CONFIGURATION (already filled in for you!)
ABFSS_ROOT = "abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files"

# Test write
test_path = f"{ABFSS_ROOT}/test_folder"
spark.range(1).write.mode("overwrite").csv(test_path)

# Test read
result = spark.read.csv(test_path).count()

if result == 1:
    print("✅ SUCCESS! Configuration is correct!")
    print(f"📂 Your Lakehouse path works!")
else:
    print("❌ ERROR: Something went wrong")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix for PySpark round() conflict
import builtins
py_round = builtins.round  # Save Python's round function

# Now use py_round() instead of round() throughout

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# ENHANCED OIL & GAS DATA GENERATOR
# WITH INTENTIONAL QUALITY ISSUES
# FINAL CORRECTED VERSION - NO ERRORS
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
from decimal import Decimal

# FIX PYSPARK CONFLICTS
import builtins
round = builtins.round
sum = builtins.sum
min = builtins.min
max = builtins.max
abs = builtins.abs

print("🛢️ ENHANCED OIL & GAS DATA GENERATOR WITH QUALITY ISSUES")
print("=" * 70)
print("This generator creates realistic data with intentional problems")
print("to demonstrate data quality handling in the pipeline.")
print("=" * 70)

# ==========================================
# CONFIGURATION
# ==========================================

ABFSS_ROOT = "abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/c2a56730-e48e-40d5-8eb7-15fb1fe397e2/Files"

# Scale: Generate 1000+ records per dataset
NUM_WELLS = 50
DAYS = 30
NUM_EMPLOYEES = 100
NUM_EQUIPMENT = 150

random.seed(42)

# ==========================================
# BUSINESS LOGIC PARAMETERS
# ==========================================

OIL_BASE_PRICE = 75.00
OIL_PRICE_VOLATILITY = 0.05
GAS_BASE_PRICE = 3.50
GAS_PRICE_VOLATILITY = 0.08

DECLINE_RATE_HORIZONTAL = 0.002
DECLINE_RATE_VERTICAL = 0.001
DECLINE_RATE_DIRECTIONAL = 0.0015

MIN_PROFITABLE_OIL_RATE = 15
SHUTDOWN_THRESHOLD = 5
MAINTENANCE_TRIGGER_SCORE = 75

SEVERANCE_TAX_RATE = 0.075
ROYALTY_RATE = 0.125

# ==========================================
# REFERENCE DATA
# ==========================================

companies = ["ExxonMobil", "Chevron", "Shell", "BP America", "ConocoPhillips", 
             "Devon Energy", "Pioneer Natural", "EOG Resources", "Occidental Petroleum"]

basins = [
    ("Permian Basin", "TX", "Midland", 8500, 150),
    ("Bakken Formation", "ND", "Williston", 10000, 120),
    ("Eagle Ford", "TX", "San Antonio", 9000, 140),
    ("Marcellus Shale", "PA", "Pittsburgh", 7000, 80),
    ("STACK Play", "OK", "Oklahoma City", 8000, 100),
    ("Delaware Basin", "NM", "Carlsbad", 9500, 145),
    ("Haynesville", "LA", "Shreveport", 11000, 90),
    ("Niobrara", "CO", "Denver", 8500, 110)
]

well_types = ["Horizontal", "Vertical", "Directional"]

job_roles = [
    ("Drilling Engineer", "Engineering", 120000, 150000),
    ("Production Engineer", "Engineering", 110000, 140000),
    ("Petroleum Geologist", "Geology", 100000, 135000),
    ("Field Operator", "Operations", 65000, 85000),
    ("Maintenance Technician", "Operations", 55000, 75000),
    ("Safety Manager", "HSE", 90000, 120000),
    ("Reservoir Engineer", "Engineering", 115000, 145000),
    ("Completion Engineer", "Engineering", 105000, 135000),
    ("Operations Manager", "Operations", 130000, 170000),
    ("Lease Operator", "Operations", 60000, 80000)
]

equipment_types = [
    ("ESP", "Electrical Submersible Pump", 50000, 8000),
    ("Pump Jack", "Sucker Rod Pump", 35000, 12000),
    ("Gas Lift System", "Artificial Lift", 45000, 10000),
    ("Separator", "3-Phase Separator", 60000, 15000),
    ("Compressor", "Gas Compressor", 80000, 6000),
    ("Tank Battery", "Storage Tank", 40000, 20000),
    ("SCADA System", "Automation", 25000, 18000),
    ("Wellhead", "Surface Equipment", 15000, 25000)
]

# ==========================================
# HELPER FUNCTION FOR DECIMAL CONVERSION
# ==========================================

def to_decimal(value, precision=4):
    """Convert to Decimal with specified precision"""
    if value is None:
        return None
    return float(round(value, precision))

# ==========================================
# EXPLICIT SCHEMAS WITH DECIMAL TYPES
# ==========================================

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
    StructField("Oil_Price_USD", DoubleType(), True),
    StructField("Gas_Price_USD", DoubleType(), True),
    StructField("Gross_Revenue_USD", DoubleType(), True),
    StructField("Royalty_USD", DoubleType(), True),
    StructField("Severance_Tax_USD", DoubleType(), True),
    StructField("Net_Revenue_USD", DoubleType(), True),
    StructField("Well_Status", StringType(), True),
    StructField("Decline_Factor", DoubleType(), True)
])

costs_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Well_ID", StringType(), True),
    StructField("BOE_Produced", DoubleType(), True),
    StructField("Labor_Cost_USD", DoubleType(), True),
    StructField("Power_Cost_USD", DoubleType(), True),
    StructField("Chemical_Cost_USD", DoubleType(), True),
    StructField("Water_Disposal_Cost_USD", DoubleType(), True),
    StructField("Maintenance_Cost_USD", DoubleType(), True),
    StructField("Workover_Cost_USD", DoubleType(), True),
    StructField("Total_OPEX_USD", DoubleType(), True),
    StructField("OPEX_Per_BOE", DoubleType(), True),
    StructField("Operating_Netback_USD", DoubleType(), True),
    StructField("Is_Profitable", StringType(), True)
])

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
    StructField("Purchase_Cost_USD", IntegerType(), True),
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

employees_schema = StructType([
    StructField("Employee_ID", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Job_Title", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Hire_Date", StringType(), True),
    StructField("Years_Of_Service", DoubleType(), True),
    StructField("Annual_Salary_USD", DoubleType(), True),
    StructField("Employment_Status", StringType(), True),
    StructField("Certification_Status", StringType(), True),
    StructField("Assigned_Wells_Count", IntegerType(), True),
    StructField("Assigned_Wells", StringType(), True),
    StructField("Safety_Training_Date", StringType(), True)
])

# ==========================================
# DATASET 1: WELLS MASTER DATA
# ==========================================

print("\n" + "=" * 70)
print("DATASET 1: WELLS MASTER DATA")
print("=" * 70)

wells_data = []
well_id = 1000

for i in range(NUM_WELLS):
    basin_name, state, county, avg_depth, avg_prod = random.choice(basins)
    well_type = random.choice(well_types)
    company = random.choice(companies)
    
    if well_type == "Horizontal":
        depth = avg_depth + random.randint(-500, 1000)
        drilling_cost = random.randint(5000000, 8000000)
        lateral_length = random.randint(5000, 10000)
        initial_production = avg_prod * random.uniform(1.2, 1.8)
    elif well_type == "Directional":
        depth = avg_depth + random.randint(-300, 500)
        drilling_cost = random.randint(3000000, 5000000)
        lateral_length = random.randint(2000, 5000)
        initial_production = avg_prod * random.uniform(0.9, 1.3)
    else:
        depth = avg_depth + random.randint(-1000, 0)
        drilling_cost = random.randint(1500000, 3000000)
        lateral_length = 0
        initial_production = avg_prod * random.uniform(0.5, 1.0)
    
    spud_date = datetime.now() - timedelta(days=random.randint(30, 1095))
    first_production_date = spud_date + timedelta(days=random.randint(45, 90))
    
    # QUALITY ISSUE 1: Add trailing spaces to Well_ID (5% of records)
    well_id_str = f"WELL-{well_id:04d}"
    if random.random() < 0.05:
        well_id_str = well_id_str + "  "
    
    # QUALITY ISSUE 2: Mixed case in status (10% of records)
    status = "Active"
    if random.random() < 0.10:
        status = random.choice(["active", "ACTIVE", "Active "])
    
    # QUALITY ISSUE 3: Some future dates (2% of records)
    if random.random() < 0.02:
        spud_date = datetime.now() + timedelta(days=random.randint(1, 30))
        first_production_date = spud_date + timedelta(days=random.randint(45, 90))
    
    # QUALITY ISSUE 4: Different date formats (10% of records)
    if random.random() < 0.10:
        spud_date_str = spud_date.strftime("%m/%d/%Y")
        first_prod_str = first_production_date.strftime("%m/%d/%Y")
    else:
        spud_date_str = spud_date.strftime("%Y-%m-%d")
        first_prod_str = first_production_date.strftime("%Y-%m-%d")
    
    well = {
        "Well_ID": well_id_str,
        "Well_Name": f"{basin_name.split()[0]}-{well_id}",
        "API_Number": f"{random.randint(40, 50)}-{random.randint(100, 999)}-{random.randint(10000, 99999)}",
        "Operator": company,
        "Basin": basin_name,
        "State": state,
        "County": county,
        "Latitude": to_decimal(random.uniform(28.0, 48.0), 6),
        "Longitude": to_decimal(random.uniform(-104.0, -75.0), 6),
        "Well_Type": well_type,
        "Total_Depth_Feet": depth,
        "Lateral_Length_Feet": lateral_length,
        "Spud_Date": spud_date_str,
        "First_Production_Date": first_prod_str,
        "Days_On_Production": (datetime.now() - first_production_date).days,
        "Drilling_Cost_USD": drilling_cost,
        "Initial_Production_BOPD": to_decimal(initial_production, 2),
        "Well_Status": status,
        "Ownership_Type": random.choice(["100% WI", "75% WI", "50% WI", "25% WI"]),
        "Royalty_Interest_PCT": to_decimal(ROYALTY_RATE * 100, 2)
    }
    
    wells_data.append(well)
    well_id += 1

# QUALITY ISSUE 5: Create 2-3 duplicate wells
num_duplicates = random.randint(2, 3)
for _ in range(num_duplicates):
    duplicate_well = random.choice(wells_data).copy()
    duplicate_well["Well_ID"] = f"WELL-{well_id:04d}"
    duplicate_well["Well_Name"] = f"Duplicate-{well_id}"
    wells_data.append(duplicate_well)
    well_id += 1
    print(f"   ⚠️  Created duplicate well with API: {duplicate_well['API_Number']}")

wells_df = spark.createDataFrame(wells_data, schema=wells_schema)
wells_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/wells_master")

print(f"✅ Created {len(wells_data)} wells (including {num_duplicates} duplicates)")
print(f"   Quality Issues Injected:")
print(f"   • {sum(1 for w in wells_data if w['Well_ID'].endswith('  '))} wells with trailing spaces")
print(f"   • {sum(1 for w in wells_data if w['Well_Status'] != 'Active')} wells with mixed case status")
print(f"   • {sum(1 for w in wells_data if '/' in w['Spud_Date'])} wells with US date format")

# ==========================================
# DATASET 2: DAILY PRODUCTION
# ==========================================

print("\n" + "=" * 70)
print("DATASET 2: DAILY PRODUCTION WITH QUALITY ISSUES")
print("=" * 70)

production_data = []
start_date = datetime.now() - timedelta(days=DAYS)

# Generate market prices
daily_oil_prices = []
daily_gas_prices = []
current_oil = OIL_BASE_PRICE
current_gas = GAS_BASE_PRICE

for day in range(DAYS):
    oil_change = current_oil * random.uniform(-OIL_PRICE_VOLATILITY, OIL_PRICE_VOLATILITY)
    gas_change = current_gas * random.uniform(-GAS_PRICE_VOLATILITY, GAS_PRICE_VOLATILITY)
    current_oil = max(50, min(100, current_oil + oil_change))
    current_gas = max(2, min(6, current_gas + gas_change))
    daily_oil_prices.append(to_decimal(current_oil, 2))
    daily_gas_prices.append(to_decimal(current_gas, 2))

for day_idx in range(DAYS):
    current_date = start_date + timedelta(days=day_idx)
    date_str = current_date.strftime("%Y-%m-%d")
    
    oil_price_today = daily_oil_prices[day_idx]
    gas_price_today = daily_gas_prices[day_idx]
    
    for well in wells_data:
        days_producing = (current_date - datetime.strptime(
            well['First_Production_Date'].replace('/', '-') if '/' in well['First_Production_Date'] 
            else well['First_Production_Date'], 
            "%m-%d-%Y" if '/' in well['First_Production_Date'] else "%Y-%m-%d"
        )).days
        
        if well['Well_Type'] == "Horizontal":
            decline_rate = DECLINE_RATE_HORIZONTAL
        elif well['Well_Type'] == "Vertical":
            decline_rate = DECLINE_RATE_VERTICAL
        else:
            decline_rate = DECLINE_RATE_DIRECTIONAL
        
        decline_factor = (1 - decline_rate) ** days_producing
        current_oil_rate = well['Initial_Production_BOPD'] * decline_factor
        current_oil_rate *= random.uniform(0.9, 1.05)
        current_oil_rate = max(0, current_oil_rate)
        
        # QUALITY ISSUE 6: Set oil rate to NULL for 5% of records
        if random.random() < 0.05:
            current_oil_rate = None
        
        # QUALITY ISSUE 7: Create outliers
        if current_oil_rate is not None and random.random() < 0.02:
            current_oil_rate = random.uniform(5000, 15000)
        
        gor = random.randint(1000, 3000)
        gas_rate = current_oil_rate * gor / 1000 if current_oil_rate is not None else None
        
        initial_water_cut = 0.20
        water_cut_increase_rate = 0.0005
        current_water_cut = min(0.80, initial_water_cut + (water_cut_increase_rate * days_producing))
        water_rate = (current_oil_rate * (current_water_cut / (1 - current_water_cut))) if current_oil_rate is not None else None
        
        # Revenue calculations
        if current_oil_rate is not None and gas_rate is not None:
            gross_oil_revenue = current_oil_rate * oil_price_today
            gross_gas_revenue = gas_rate * gas_price_today
            gross_revenue = gross_oil_revenue + gross_gas_revenue
            royalty_payment = gross_revenue * ROYALTY_RATE
            severance_tax = gross_revenue * SEVERANCE_TAX_RATE
            net_revenue = gross_revenue - royalty_payment - severance_tax
        else:
            gross_revenue = None
            royalty_payment = None
            severance_tax = None
            net_revenue = None
        
        # Well status
        if current_oil_rate is None:
            status = "Unknown"
        elif current_oil_rate < SHUTDOWN_THRESHOLD:
            status = "Shut In"
        elif current_oil_rate < MIN_PROFITABLE_OIL_RATE:
            status = "Marginal"
        else:
            status = "Producing"
        
        # QUALITY ISSUE 8: Mixed case in status
        if random.random() < 0.05:
            status = status.lower()
        
        production = {
            "Date": date_str,
            "Well_ID": well['Well_ID'],
            "Well_Name": well['Well_Name'],
            "Basin": well['Basin'],
            "Well_Type": well['Well_Type'],
            "Days_On_Production": days_producing,
            "Oil_Production_BOPD": to_decimal(current_oil_rate, 2) if current_oil_rate is not None else None,
            "Gas_Production_MCF": to_decimal(gas_rate, 2) if gas_rate is not None else None,
            "Water_Production_BWPD": to_decimal(water_rate, 2) if water_rate is not None else None,
            "Gas_Oil_Ratio": gor,
            "Water_Cut_PCT": to_decimal(current_water_cut * 100, 2),
            "Oil_Price_USD": oil_price_today,
            "Gas_Price_USD": gas_price_today,
            "Gross_Revenue_USD": to_decimal(gross_revenue, 2) if gross_revenue is not None else None,
            "Royalty_USD": to_decimal(royalty_payment, 2) if royalty_payment is not None else None,
            "Severance_Tax_USD": to_decimal(severance_tax, 2) if severance_tax is not None else None,
            "Net_Revenue_USD": to_decimal(net_revenue, 2) if net_revenue is not None else None,
            "Well_Status": status,
            "Decline_Factor": to_decimal(decline_factor, 4)
        }
        
        production_data.append(production)

# QUALITY ISSUE 9: Create duplicate production records
num_prod_duplicates = random.randint(5, 10)
for _ in range(num_prod_duplicates):
    duplicate_record = random.choice(production_data).copy()
    production_data.append(duplicate_record)

# QUALITY ISSUE 10: Create orphan records
for _ in range(random.randint(3, 5)):
    fake_well_record = random.choice(production_data).copy()
    fake_well_record["Well_ID"] = f"WELL-{random.randint(9000, 9999)}"
    fake_well_record["Well_Name"] = f"NonExistent-{random.randint(1000, 9999)}"
    production_data.append(fake_well_record)

production_df = spark.createDataFrame(production_data, schema=production_schema)
production_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/production_daily")

null_oil_count = sum(1 for p in production_data if p['Oil_Production_BOPD'] is None)
outlier_count = sum(1 for p in production_data if p['Oil_Production_BOPD'] is not None and p['Oil_Production_BOPD'] > 1000)

print(f"✅ Created {len(production_data)} production records")
print(f"   Quality Issues Injected:")
print(f"   • {null_oil_count} records with NULL oil production")
print(f"   • {outlier_count} records with outlier production values")
print(f"   • {num_prod_duplicates} duplicate records")
print(f"   • 3-5 records for non-existent wells")

# ==========================================
# DATASET 3: OPERATING COSTS
# ==========================================

print("\n" + "=" * 70)
print("DATASET 3: OPERATING COSTS WITH QUALITY ISSUES")
print("=" * 70)

costs_data = []

for day_idx in range(DAYS):
    current_date = start_date + timedelta(days=day_idx)
    date_str = current_date.strftime("%Y-%m-%d")
    
    for well in wells_data:
        prod_records = [p for p in production_data if p['Well_ID']==well['Well_ID'] and p['Date']==date_str]
        if not prod_records:
            continue
        prod_record = prod_records[0]
        
        if well['Well_Type'] == "Horizontal":
            base_opex = random.uniform(25, 35)
        elif well['Well_Type'] == "Directional":
            base_opex = random.uniform(18, 28)
        else:
            base_opex = random.uniform(12, 22)
        
        oil_prod = prod_record['Oil_Production_BOPD'] if prod_record['Oil_Production_BOPD'] is not None else 0.0
        gas_prod = prod_record['Gas_Production_MCF'] if prod_record['Gas_Production_MCF'] is not None else 0.0
        water_prod = prod_record['Water_Production_BWPD'] if prod_record['Water_Production_BWPD'] is not None else 0.0
        
        boe = oil_prod + (gas_prod / 6.0)
        
        labor_cost = random.uniform(300, 600)
        power_cost = boe * random.uniform(2, 4)
        chemical_cost = water_prod * random.uniform(0.50, 1.50)
        water_disposal = water_prod * random.uniform(1.00, 3.00)
        
        # QUALITY ISSUE 11: Negative costs
        if random.random() < 0.01:
            maintenance_cost = -random.uniform(100, 500)
        else:
            maintenance_cost = random.uniform(100, 500) if random.random() < 0.95 else random.uniform(2000, 8000)
        
        workover_cost = random.uniform(50000, 150000) if random.random() < 0.01 else 0.0
        
        total_opex = labor_cost + power_cost + chemical_cost + water_disposal + maintenance_cost + workover_cost
        
        net_revenue = prod_record['Net_Revenue_USD'] if prod_record['Net_Revenue_USD'] is not None else 0.0
        operating_netback = net_revenue - total_opex
        
        cost = {
            "Date": date_str,
            "Well_ID": well['Well_ID'],
            "BOE_Produced": to_decimal(boe, 4),
            "Labor_Cost_USD": to_decimal(labor_cost, 4),
            "Power_Cost_USD": to_decimal(power_cost, 4),
            "Chemical_Cost_USD": to_decimal(chemical_cost, 4),
            "Water_Disposal_Cost_USD": to_decimal(water_disposal, 4),
            "Maintenance_Cost_USD": to_decimal(maintenance_cost, 4),
            "Workover_Cost_USD": to_decimal(workover_cost, 4),
            "Total_OPEX_USD": to_decimal(total_opex, 4),
            "OPEX_Per_BOE": to_decimal(total_opex / boe if boe > 0 else 0.0, 4),
            "Operating_Netback_USD": to_decimal(operating_netback, 4),
            "Is_Profitable": "Yes" if operating_netback > 0 else "No"
        }
        
        costs_data.append(cost)

costs_df = spark.createDataFrame(costs_data, schema=costs_schema)
costs_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/operating_costs")

negative_costs = sum(1 for c in costs_data if c['Maintenance_Cost_USD'] < 0)

print(f"✅ Created {len(costs_data)} cost records")
print(f"   Quality Issues Injected:")
print(f"   • {negative_costs} records with negative costs")

# ==========================================
# DATASET 4: EQUIPMENT
# ==========================================

print("\n" + "=" * 70)
print("DATASET 4: EQUIPMENT WITH QUALITY ISSUES")
print("=" * 70)

equipment_data = []
equip_id = 5000

for i in range(NUM_EQUIPMENT):
    well = random.choice(wells_data)
    equip_type_code, equip_type_name, purchase_cost, avg_runtime = random.choice(equipment_types)
    
    first_prod = well['First_Production_Date']
    if '/' in first_prod:
        install_date = datetime.strptime(first_prod, "%m/%d/%Y") + timedelta(days=random.randint(-30, 30))
    else:
        install_date = datetime.strptime(first_prod, "%Y-%m-%d") + timedelta(days=random.randint(-30, 30))
    
    days_in_service = (datetime.now() - install_date).days
    
    runtime_hours = random.randint(int(avg_runtime * 0.3), int(avg_runtime * 1.5))
    
    expected_life_hours = avg_runtime * 1.2
    health_score = max(50, 100 - (runtime_hours / expected_life_hours * 50))
    health_score *= random.uniform(0.9, 1.05)
    health_score = min(100, max(0, health_score))
    
    # QUALITY ISSUE 13: NULL health scores
    if random.random() < 0.03:
        health_score = None
    
    # QUALITY ISSUE 14: Health score > 100
    if health_score is not None and random.random() < 0.01:
        health_score = random.uniform(100, 120)
    
    if health_score is None:
        maintenance_status = "Unknown"
        failure_probability = None
    elif health_score < MAINTENANCE_TRIGGER_SCORE:
        maintenance_status = "Overdue"
        failure_probability = max(0, min(100, (100 - health_score) * 1.5))
    elif health_score < 85:
        maintenance_status = "Scheduled"
        failure_probability = max(0, min(100, (100 - health_score) * 1.5))
    else:
        maintenance_status = "Good"
        failure_probability = max(0, min(100, (100 - health_score) * 1.5))
    
    last_maintenance = datetime.now() - timedelta(days=random.randint(30, 365))
    
    equipment = {
        "Equipment_ID": f"EQ-{equip_id:05d}",
        "Well_ID": well['Well_ID'],
        "Equipment_Type_Code": equip_type_code,
        "Equipment_Type": equip_type_name,
        "Manufacturer": random.choice(["Baker Hughes", "Schlumberger", "Halliburton", "Weatherford", "National Oilwell"]),
        "Model_Number": f"MDL-{random.randint(1000, 9999)}",
        "Serial_Number": f"SN-{random.randint(100000, 999999)}",
        "Install_Date": install_date.strftime("%Y-%m-%d"),
        "Days_In_Service": days_in_service,
        "Purchase_Cost_USD": purchase_cost,
        "Runtime_Hours": runtime_hours,
        "Expected_Life_Hours": int(expected_life_hours),
        "Health_Score": to_decimal(health_score, 2) if health_score is not None else None,
        "Last_Maintenance_Date": last_maintenance.strftime("%Y-%m-%d"),
        "Days_Since_Maintenance": (datetime.now() - last_maintenance).days,
        "Maintenance_Status": maintenance_status,
        "Failure_Probability_PCT": to_decimal(failure_probability, 2) if failure_probability is not None else None,
        "Vibration_Level": to_decimal(random.uniform(0, 10), 2),
        "Temperature_F": to_decimal(random.uniform(100, 200), 2),
        "Pressure_PSI": to_decimal(random.uniform(500, 3000), 2)
    }
    
    equipment_data.append(equipment)
    equip_id += 1

equipment_df = spark.createDataFrame(equipment_data, schema=equipment_schema)
equipment_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/equipment")

null_health = sum(1 for e in equipment_data if e['Health_Score'] is None)
invalid_health = sum(1 for e in equipment_data if e['Health_Score'] is not None and e['Health_Score'] > 100)

print(f"✅ Created {len(equipment_data)} equipment records")
print(f"   Quality Issues Injected:")
print(f"   • {null_health} records with NULL health score")
print(f"   • {invalid_health} records with health score > 100")

# ==========================================
# DATASET 5: EMPLOYEES
# ==========================================

print("\n" + "=" * 70)
print("DATASET 5: EMPLOYEES")
print("=" * 70)

employees_data = []
emp_id = 2000

first_names = ["John", "Mary", "Robert", "Patricia", "Michael", "Jennifer", "William", "Linda", 
               "David", "Barbara", "James", "Elizabeth", "Joseph", "Susan", "Thomas", "Jessica"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", 
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas"]

for i in range(NUM_EMPLOYEES):
    job_title, department, min_salary, max_salary = random.choice(job_roles)
    
    hire_date = datetime.now() - timedelta(days=random.randint(365, 3650))
    years_service = (datetime.now() - hire_date).days / 365.25
    
    base_salary = random.uniform(min_salary, max_salary)
    experience_bonus = base_salary * (years_service * 0.02)
    annual_salary = min(max_salary * 1.2, base_salary + experience_bonus)
    
    if department == "Operations":
        assigned_wells = random.sample([w for w in wells_data if not w['Well_ID'].endswith('  ')], 
                                      min(random.randint(3, 8), len(wells_data)))
    else:
        assigned_wells = []
    
    employee = {
        "Employee_ID": f"EMP-{emp_id:04d}",
        "First_Name": random.choice(first_names),
        "Last_Name": random.choice(last_names),
        "Job_Title": job_title,
        "Department": department,
        "Hire_Date": hire_date.strftime("%Y-%m-%d"),
        "Years_Of_Service": to_decimal(years_service, 1),
        "Annual_Salary_USD": to_decimal(annual_salary, 2),
        "Employment_Status": random.choice(["Full-Time", "Full-Time", "Full-Time", "Contract"]),
        "Certification_Status": random.choice(["Current", "Current", "Renewal Needed"]),
        "Assigned_Wells_Count": len(assigned_wells),
        "Assigned_Wells": ",".join([w['Well_ID'].strip() for w in assigned_wells[:3]]) if assigned_wells else "N/A",
        "Safety_Training_Date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    }
    
    employees_data.append(employee)
    emp_id += 1

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
employees_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/employees")

print(f"✅ Created {len(employees_data)} employee records")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name
from datetime import datetime

# Raw CSV root inside Lakehouse
RAW_ROOT = "Files/bronze_raw"

# Map raw folders -> your existing bronze table names (dbt_*)
datasets = {
    "wells_master": "dbt_wells_master",
    "production_daily": "dbt_production_daily",
    "operating_costs": "dbt_operating_costs",
    "equipment": "dbt_equipment",
    "employees": "dbt_employees"
}

# For now keep it simple (pipeline parameters later)
run_id = "manual_run"
run_date = datetime.now().strftime("%Y-%m-%d")

for folder, table in datasets.items():
    path = f"{RAW_ROOT}/{folder}"
    print(f"\nLoading: {folder}")
    print(f"  Source: {path}")
    print(f"  Target: {table}")

    df = (
        spark.read
             .option("header", "true")
             .csv(path)
             .withColumn("_ingest_ts", F.current_timestamp())
             .withColumn("_run_id", F.lit(run_id))
             .withColumn("_run_date", F.lit(run_date))
             .withColumn("_source_file", input_file_name())
    )

    row_count = df.count()
    print(f"  Rows read: {row_count}")

    # Overwrite keeps the run deterministic for now
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(table))

print("\n✅ Bronze load completed into dbt_* tables.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/bronze_raw/employees/part-00000-9c5796fa-a340-4861-90bd-82c482b61876-c000.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
