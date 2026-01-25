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

# MAGIC %%sql
# MAGIC                  ┌─────────────────────┐
# MAGIC                  │  Source Data (CSV)  │
# MAGIC                  │------------------- │
# MAGIC                  │ Well Production     │
# MAGIC                  │ Well Master         │
# MAGIC                  │ Operating Cost      │
# MAGIC                  │ Commodity Prices    │
# MAGIC                  └─────────┬──────────┘
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                  ┌─────────────────────┐
# MAGIC                  │       Bronze        │
# MAGIC                  │  Raw Data Landing   │
# MAGIC                  │ (Dataflows Gen2 →   │
# MAGIC                  │    OneLake)         │
# MAGIC                  └─────────┬──────────┘
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                  ┌─────────────────────┐
# MAGIC                  │       Silver        │
# MAGIC                  │ Business Logic      │
# MAGIC                  │ (Notebooks / SQL)  │
# MAGIC                  │ - Net Production   │
# MAGIC                  │ - Total OPEX       │
# MAGIC                  │ - Revenue & Profit │
# MAGIC                  └─────────┬──────────┘
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                  ┌─────────────────────┐
# MAGIC                  │        Gold         │
# MAGIC                  │ Analytics Layer     │
# MAGIC                  │ Fact_Daily_Well_    │
# MAGIC                  │ Performance Table   │
# MAGIC                  └─────────┬──────────┘
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                  ┌─────────────────────┐
# MAGIC                  │      Power BI       │
# MAGIC                  │ Dashboard / KPIs    │
# MAGIC                  │ - Net Production    │
# MAGIC                  │ - Cost per Barrel   │
# MAGIC                  │ - Revenue           │
# MAGIC                  │ - Operating Profit  │
# MAGIC                  └─────────────────────┘
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

https://app.fabric.microsoft.com/groups/29b83063-d09f-4997-ab9d-c075a0f36775/synapsenotebooks/e54edff6-ca4b-40ab-bef8-8b7d14568ef8?clientSideAuth=0&experience=fabric-developer$0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

https://app.fabric.microsoft.com/groups/29b83063-d09f-4997-ab9d-c075a0f36775/lakehouses/c2a56730-e48e-40d5-8eb7-15fb1fe397e2?clientSideAuth=0&experience=fabric-developer$0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# ENHANCED OIL & GAS DATA GENERATOR
# WITH INTENTIONAL QUALITY ISSUES
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

print("🛢️ ENHANCED OIL & GAS DATA GENERATOR WITH QUALITY ISSUES")
print("=" * 70)
print("This generator creates realistic data with intentional problems")
print("to demonstrate data quality handling in the pipeline.")
print("=" * 70)

# ==========================================
# CONFIGURATION
# ==========================================

https://app.fabric.microsoft.com/groups/29b83063-d09f-4997-ab9d-c075a0f36775/synapsenotebooks/e54edff6-ca4b-40ab-bef8-8b7d14568ef8?clientSideAuth=0&experience=fabric-developer$0
abfss://Oil_n_gas_demo@onelake.dfs.fabric.microsoft.com/dev_brnz_OnG_Lkh.Lakehouse/Files/OGORBcsv.csv
ABFSS_ROOT = "abfss://YOUR_WORKSPACE_ID@onelake.dfs.fabric.microsoft.com/YOUR_LAKEHOUSE_ID/Files"

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

print("\n📊 DATA QUALITY ISSUES TO BE INJECTED:")
print("=" * 70)
print("1. MISSING VALUES (NULL)")
print("   • 5% of production records will have missing oil rates")
print("   • 3% of equipment will have missing health scores")
print("")
print("2. DUPLICATE RECORDS")
print("   • 2-3 duplicate wells (same API number)")
print("   • 5-10 duplicate production records")
print("")
print("3. INVALID DATES")
print("   • Some dates in the future")
print("   • Some dates before well spud date")
print("")
print("4. OUTLIERS")
print("   • Unrealistic production values (10,000+ BOPD)")
print("   • Negative costs")
print("")
print("5. FORMAT INCONSISTENCIES")
print("   • Different date formats (YYYY-MM-DD vs MM/DD/YYYY)")
print("   • Trailing spaces in Well_ID")
print("   • Mixed case in status fields")
print("")
print("6. REFERENTIAL INTEGRITY ISSUES")
print("   • Production records for non-existent wells")
print("   • Equipment assigned to deleted wells")
print("")
print("7. BUSINESS RULE VIOLATIONS")
print("   • Production > well capacity")
print("   • Costs recorded when well is shut in")
print("   • Equipment health score > 100")

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
# DATASET 1: WELLS MASTER DATA (50 records)
# WITH INTENTIONAL QUALITY ISSUES
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
        well_id_str = well_id_str + "  "  # Trailing spaces
    
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
        spud_date_str = spud_date.strftime("%m/%d/%Y")  # US format
        first_prod_str = first_production_date.strftime("%m/%d/%Y")
    else:
        spud_date_str = spud_date.strftime("%Y-%m-%d")  # ISO format
        first_prod_str = first_production_date.strftime("%Y-%m-%d")
    
    well = {
        "Well_ID": well_id_str,
        "Well_Name": f"{basin_name.split()[0]}-{well_id}",
        "API_Number": f"{random.randint(40, 50)}-{random.randint(100, 999)}-{random.randint(10000, 99999)}",
        "Operator": company,
        "Basin": basin_name,
        "State": state,
        "County": county,
        "Latitude": round(random.uniform(28.0, 48.0), 6),
        "Longitude": round(random.uniform(-104.0, -75.0), 6),
        "Well_Type": well_type,
        "Total_Depth_Feet": depth,
        "Lateral_Length_Feet": lateral_length,
        "Spud_Date": spud_date_str,
        "First_Production_Date": first_prod_str,
        "Days_On_Production": (datetime.now() - first_production_date).days,
        "Drilling_Cost_USD": drilling_cost,
        "Initial_Production_BOPD": round(initial_production, 2),
        "Well_Status": status,
        "Ownership_Type": random.choice(["100% WI", "75% WI", "50% WI", "25% WI"]),
        "Royalty_Interest_PCT": round(ROYALTY_RATE * 100, 2)
    }
    
    wells_data.append(well)
    well_id += 1

# QUALITY ISSUE 5: Create 2-3 duplicate wells (same API number)
num_duplicates = random.randint(2, 3)
for _ in range(num_duplicates):
    duplicate_well = random.choice(wells_data).copy()
    duplicate_well["Well_ID"] = f"WELL-{well_id:04d}"
    duplicate_well["Well_Name"] = f"Duplicate-{well_id}"
    wells_data.append(duplicate_well)
    well_id += 1
    print(f"   ⚠️  Created duplicate well with API: {duplicate_well['API_Number']}")

wells_df = spark.createDataFrame(wells_data)
wells_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/wells_master")

print(f"✅ Created {len(wells_data)} wells (including {num_duplicates} duplicates)")
print(f"   Quality Issues Injected:")
print(f"   • {sum(1 for w in wells_data if w['Well_ID'].endswith('  '))} wells with trailing spaces")
print(f"   • {sum(1 for w in wells_data if w['Well_Status'] != 'Active')} wells with mixed case status")
print(f"   • {sum(1 for w in wells_data if '/' in w['Spud_Date'])} wells with US date format")

# ==========================================
# DATASET 2: DAILY PRODUCTION
# WITH INTENTIONAL QUALITY ISSUES
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
    daily_oil_prices.append(round(current_oil, 2))
    daily_gas_prices.append(round(current_gas, 2))

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
        
        # QUALITY ISSUE 7: Create outliers (unrealistic values) for 2% of records
        if current_oil_rate is not None and random.random() < 0.02:
            current_oil_rate = random.uniform(5000, 15000)  # Impossible production rate
        
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
            "Oil_Production_BOPD": round(current_oil_rate, 2) if current_oil_rate is not None else None,
            "Gas_Production_MCF": round(gas_rate, 2) if gas_rate is not None else None,
            "Water_Production_BWPD": round(water_rate, 2) if water_rate is not None else None,
            "Gas_Oil_Ratio": gor,
            "Water_Cut_PCT": round(current_water_cut * 100, 2),
            "Oil_Price_USD": oil_price_today,
            "Gas_Price_USD": gas_price_today,
            "Gross_Revenue_USD": round(gross_revenue, 2) if gross_revenue is not None else None,
            "Royalty_USD": round(royalty_payment, 2) if royalty_payment is not None else None,
            "Severance_Tax_USD": round(severance_tax, 2) if severance_tax is not None else None,
            "Net_Revenue_USD": round(net_revenue, 2) if net_revenue is not None else None,
            "Well_Status": status,
            "Decline_Factor": round(decline_factor, 4)
        }
        
        production_data.append(production)

# QUALITY ISSUE 9: Create 5-10 duplicate production records
num_prod_duplicates = random.randint(5, 10)
for _ in range(num_prod_duplicates):
    duplicate_record = random.choice(production_data).copy()
    production_data.append(duplicate_record)

# QUALITY ISSUE 10: Create 3-5 production records for non-existent wells
for _ in range(random.randint(3, 5)):
    fake_well_record = random.choice(production_data).copy()
    fake_well_record["Well_ID"] = f"WELL-{random.randint(9000, 9999)}"
    fake_well_record["Well_Name"] = f"NonExistent-{random.randint(1000, 9999)}"
    production_data.append(fake_well_record)

production_df = spark.createDataFrame(production_data)
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
        # Find matching production record
        prod_records = [p for p in production_data if p['Well_ID']==well['Well_ID'] and p['Date']==date_str]
        if not prod_records:
            continue
        prod_record = prod_records[0]
        
        # Calculate costs
        if well['Well_Type'] == "Horizontal":
            base_opex = random.uniform(25, 35)
        elif well['Well_Type'] == "Directional":
            base_opex = random.uniform(18, 28)
        else:
            base_opex = random.uniform(12, 22)
        
        oil_prod = prod_record['Oil_Production_BOPD'] if prod_record['Oil_Production_BOPD'] is not None else 0
        gas_prod = prod_record['Gas_Production_MCF'] if prod_record['Gas_Production_MCF'] is not None else 0
        water_prod = prod_record['Water_Production_BWPD'] if prod_record['Water_Production_BWPD'] is not None else 0
        
        boe = oil_prod + (gas_prod / 6)
        
        labor_cost = random.uniform(300, 600)
        power_cost = boe * random.uniform(2, 4)
        chemical_cost = water_prod * random.uniform(0.50, 1.50)
        water_disposal = water_prod * random.uniform(1.00, 3.00)
        
        # QUALITY ISSUE 11: Negative costs for 1% of records
        if random.random() < 0.01:
            maintenance_cost = -random.uniform(100, 500)
        else:
            maintenance_cost = random.uniform(100, 500) if random.random() < 0.95 else random.uniform(2000, 8000)
        
        workover_cost = random.uniform(50000, 150000) if random.random() < 0.01 else 0
        
        total_opex = labor_cost + power_cost + chemical_cost + water_disposal + maintenance_cost + workover_cost
        
        # QUALITY ISSUE 12: Business rule violation - costs when well is shut in
        # (Should be zero or minimal, but we'll record normal costs)
        
        net_revenue = prod_record['Net_Revenue_USD'] if prod_record['Net_Revenue_USD'] is not None else 0
        operating_netback = net_revenue - total_opex
        
        cost = {
            "Date": date_str,
            "Well_ID": well['Well_ID'],
            "BOE_Produced": round(boe, 2),
            "Labor_Cost_USD": round(labor_cost, 2),
            "Power_Cost_USD": round(power_cost, 2),
            "Chemical_Cost_USD": round(chemical_cost, 2),
            "Water_Disposal_Cost_USD": round(water_disposal, 2),
            "Maintenance_Cost_USD": round(maintenance_cost, 2),
            "Workover_Cost_USD": round(workover_cost, 2),
            "Total_OPEX_USD": round(total_opex, 2),
            "OPEX_Per_BOE": round(total_opex / boe if boe > 0 else 0, 2),
            "Operating_Netback_USD": round(operating_netback, 2),
            "Is_Profitable": "Yes" if operating_netback > 0 else "No"
        }
        
        costs_data.append(cost)

costs_df = spark.createDataFrame(costs_data)
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
    
    # Parse the date properly
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
    
    # QUALITY ISSUE 13: Set health score to NULL for 3% of records
    if random.random() < 0.03:
        health_score = None
    
    # QUALITY ISSUE 14: Health score > 100 for 1% of records
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
        "Health_Score": round(health_score, 2) if health_score is not None else None,
        "Last_Maintenance_Date": last_maintenance.strftime("%Y-%m-%d"),
        "Days_Since_Maintenance": (datetime.now() - last_maintenance).days,
        "Maintenance_Status": maintenance_status,
        "Failure_Probability_PCT": round(failure_probability, 2) if failure_probability is not None else None,
        "Vibration_Level": round(random.uniform(0, 10), 2),
        "Temperature_F": round(random.uniform(100, 200), 2),
        "Pressure_PSI": round(random.uniform(500, 3000), 2)
    }
    
    equipment_data.append(equipment)
    equip_id += 1

equipment_df = spark.createDataFrame(equipment_data)
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
        "Years_Of_Service": round(years_service, 1),
        "Annual_Salary_USD": round(annual_salary, 2),
        "Employment_Status": random.choice(["Full-Time", "Full-Time", "Full-Time", "Contract"]),
        "Certification_Status": random.choice(["Current", "Current", "Renewal Needed"]),
        "Assigned_Wells_Count": len(assigned_wells),
        "Assigned_Wells": ",".join([w['Well_ID'].strip() for w in assigned_wells[:3]]) if assigned_wells else "N/A",
        "Safety_Training_Date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    }
    
    employees_data.append(employee)
    emp_id += 1

employees_df = spark.createDataFrame(employees_data)
employees_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{ABFSS_ROOT}/bronze_raw/employees")

print(f"✅ Created {len(employees_data)} employee records")

# ==========================================
# SUMMARY
# ==========================================

print("\n" + "=" * 70)
print("✅ DATA GENERATION COMPLETE WITH QUALITY ISSUES!")
print("=" * 70)

total_records = (len(wells_data) + len(production_data) + len(costs_data) + 
                 len(equipment_data) + len(employees_data))

print(f"\n📊 TOTAL RECORDS GENERATED: {total_records:,}")
print("\n📁 DATASETS CREATED IN bronze_raw/:")
print(f"  1️⃣  Wells Master       → {len(wells_data):,} records")
print(f"  2️⃣  Daily Production   → {len(production_data):,} records")
print(f"  3️⃣  Operating Costs    → {len(costs_data):,} records")
print(f"  4️⃣  Equipment          → {len(equipment_data):,} records")
print(f"  5️⃣  Employees          → {len(employees_data):,} records")

print("\n⚠️  DATA QUALITY ISSUES SUMMARY:")
print("=" * 70)
print("Type                          | Count    | Impact")
print("-" * 70)
print(f"Trailing spaces in Well_ID    | {sum(1 for w in wells_data if w['Well_ID'].endswith('  ')):8} | Join failures")
print(f"Mixed case status values      | ~50      | Inconsistent grouping")
print(f"Different date formats        | ~50      | Parse errors")
print(f"Duplicate wells               | {num_duplicates:8} | Incorrect aggregations")
print(f"Duplicate production records  | {num_prod_duplicates:8} | Inflated totals")
print(f"NULL oil production values    | {null_oil_count:8} | Missing revenue")
print(f"Outlier production (>1000)    | {outlier_count:8} | Unrealistic KPIs")
print(f"Orphan production records     | ~5       | Referential integrity")
print(f"Negative costs                | {negative_costs:8} | Invalid calculations")
print(f"NULL equipment health scores  | {null_health:8} | Unable to prioritize")
print(f"Invalid health scores (>100)  | {invalid_health:8} | Business rule violation")

print("\n🎯 NEXT STEPS:")
print("=" * 70)
print("1. Run Bronze Layer ingestion (copy raw data as-is)")
print("2. Run Silver Layer cleaning (fix all quality issues)")
print("3. Run Gold Layer aggregation (business metrics)")
print("4. Monitor data quality metrics")
print("5. Build Power BI dashboards on Gold layer")

print("\n💡 These quality issues are INTENTIONAL to demonstrate:")
print("   • Data profiling and discovery")
print("   • Validation rule design")
print("   • Error handling strategies")
print("   • Data quality monitoring")
print("   • Impact on downstream analytics")

print("\n📍 FILES LOCATION:")
print(f"{ABFSS_ROOT}/bronze_raw/")
print("\n" + "=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM dev_brnz_OnG_Lkh.dbt_employees LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create test data with intentional quality issues
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read original data
df_original = spark.sql("SELECT * FROM dev_brnz_OnG_Lkh.dbt_employees LIMIT 100")

# Create BAD records to test Great Expectations
bad_records = spark.createDataFrame([
    # Issue 1: NULL Employee_ID
    (None, "John", "Doe", "Field Operator", "Operations", "2020-01-15", 4.5, 65000.0, "Full-Time", "Current", 3, "WELL-1001", "2025-01-10"),
    
    # Issue 2: Duplicate Employee_ID
    ("EMP-2000", "Jane", "Smith", "Drilling Engineer", "Engineering", "2019-05-20", 5.5, 95000.0, "Full-Time", "Current", 5, "WELL-1002", "2024-12-15"),
    
    # Issue 3: Invalid ID pattern (missing hyphen)
    ("EMP2999", "Mike", "Johnson", "Production Engineer", "Engineering", "2021-03-10", 3.2, 88000.0, "Full-Time", "Current", 4, "WELL-1003", "2025-01-05"),
    
    # Issue 4: NULL First_Name
    ("EMP-3001", None, "Williams", "Maintenance Technician", "Operations", "2018-07-25", 6.8, 72000.0, "Contract", "Current", 2, "WELL-1004", "2024-11-20"),
    
    # Issue 5: Name too short (1 character)
    ("EMP-3002", "A", "Brown", "Safety Manager", "HSE", "2022-02-14", 2.1, 78000.0, "Full-Time", "Current", 0, "N/A", "2025-01-08"),
    
    # Issue 6: Invalid Job_Title
    ("EMP-3003", "Sarah", "Davis", "CEO", "Engineering", "2020-09-05", 4.0, 120000.0, "Full-Time", "Current", 6, "WELL-1005", "2024-12-01"),
    
    # Issue 7: Invalid Department
    ("EMP-3004", "Tom", "Miller", "Field Operator", "Finance", "2019-11-30", 5.3, 68000.0, "Full-Time", "Expired", 3, "WELL-1006", "2024-10-15"),
        
    # Issue 10: Years of service negative
    ("EMP-3007", "Lisa", "Taylor", "Completion Engineer", "Engineering", "2025-01-01", -2.0, 92000.0, "Full-Time", "Current", 4, "WELL-1009", "2025-01-14"),
    
    # Issue 11: Years of service > 40
    ("EMP-3008", "Robert", "Anderson", "Reservoir Engineer", "Engineering", "1970-01-01", 55.0, 105000.0, "Full-Time", "Current", 5, "WELL-1010", "2024-08-10"),
    
    # Issue 12: Invalid Employment_Status
    ("EMP-3009", "Jennifer", "Thomas", "Operations Manager", "Operations", "2019-12-20", 5.1, 115000.0, "Freelance", "Current", 8, "WELL-1011", "2024-11-05"),
    
    # Issue 13: Invalid Certification_Status
    ("EMP-3010", "William", "Jackson", "Drilling Engineer", "Engineering", "2021-08-15", 3.4, 98000.0, "Full-Time", "Pending", 6, "WELL-1012", "2025-01-03"),
    
    # Issue 14: Assigned_Wells_Count > 10
    ("EMP-3011", "Mary", "White", "Field Operator", "Operations", "2020-05-22", 4.6, 70000.0, "Full-Time", "Current", 15, "WELL-1013", "2024-12-18"),
    
    # Issue 15: NULL Hire_Date
    ("EMP-3012", "James", "Harris", "Maintenance Technician", "Operations", None, 3.8, 66000.0, "Contract", "Renewal Needed", 3, "WELL-1014", "2025-01-06"),
    
], schema=df_original.schema)

# Combine original + bad records
df_with_issues = df_original.union(bad_records)

# Show the data
print(f"Total records: {df_with_issues.count()}")
print(f"Original clean records: {df_original.count()}")
print(f"Added bad records: {bad_records.count()}")

display(df_with_issues)

# Optional: Save to temp table for testing
df_with_issues.write.format("delta").mode("overwrite").saveAsTable("temp_employees_with_issues")

print("\n✅ Test data created with 15 quality issues!")
print("Run Great Expectations on: spark.table('temp_employees_with_issues')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix: Cast values to Decimal properly
from pyspark.sql.functions import *
from pyspark.sql.types import *
from decimal import Decimal

# Create records with proper Decimal casting
bad_records = spark.createDataFrame([
    # Issue 1: Trailing spaces in Employee_ID
    ("EMP-3001  ", "John", "Doe", "Field Operator", "Operations", "2020-01-15", Decimal("4.50"), Decimal("65000.00"), "Full-Time", "Current", 3, "WELL-1001", "2025-01-10"),
    
    # Issue 2: Leading spaces in First_Name
    ("EMP-3002", "  Sarah", "Smith", "Drilling Engineer", "Engineering", "2019-05-20", Decimal("5.50"), Decimal("95000.00"), "Full-Time", "Current", 5, "WELL-1002", "2024-12-15"),
    
    # Issue 3: Trailing spaces in Last_Name
    ("EMP-3003", "Mike", "Johnson  ", "Production Engineer", "Engineering", "2021-03-10", Decimal("3.20"), Decimal("88000.00"), "Full-Time", "Current", 4, "WELL-1003", "2025-01-05"),
    
    # Issue 4: US date format (MM/DD/YYYY)
    ("EMP-3004", "Emily", "Williams", "Maintenance Technician", "Operations", "07/25/2018", Decimal("6.80"), Decimal("72000.00"), "Contract", "Current", 2, "WELL-1004", "11/20/2024"),
    
    # Issue 5: Date with slashes (DD/MM/YYYY)
    ("EMP-3005", "David", "Brown", "Safety Manager", "HSE", "14/02/2022", Decimal("2.10"), Decimal("78000.00"), "Full-Time", "Current", 0, "N/A", "08/01/2025"),
    
], schema=StructType([
    StructField("Employee_ID", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Job_Title", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Hire_Date", StringType(), True),
    StructField("Years_Of_Service", DecimalType(18, 2), True),
    StructField("Annual_Salary_USD", DecimalType(18, 2), True),
    StructField("Employment_Status", StringType(), True),
    StructField("Certification_Status", StringType(), True),
    StructField("Assigned_Wells_Count", IntegerType(), True),
    StructField("Assigned_Wells", StringType(), True),
    StructField("Safety_Training_Date", StringType(), True)
]))

bad_records.write.format("delta").mode("append").saveAsTable("dev_brnz_OnG_Lkh.dbt_employees")

print("✅ Inserted 5 bad records with quality issues!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM dev_brnz_OnG_Lkh.dbt_employees LIMIT 3")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dbt_employees
root
 |-- Employee_ID: string (nullable = true)
 |-- First_Name: string (nullable = true)
 |-- Last_Name: string (nullable = true)
 |-- Job_Title: string (nullable = true)
 |-- Department: string (nullable = true)
 |-- Hire_Date: string (nullable = true)
 |-- Years_Of_Service: decimal(18,2) (nullable = true)
 |-- Annual_Salary_USD: decimal(18,2) (nullable = true)
 |-- Employment_Status: string (nullable = true)
 |-- Certification_Status: string (nullable = true)
 |-- Assigned_Wells_Count: integer (nullable = true)
 |-- Assigned_Wells: string (nullable = true)
 |-- Safety_Training_Date: string (nullable = true)




dbt_equipment
root
 |-- Equipment_ID: string (nullable = true)
 |-- Well_ID: string (nullable = true)
 |-- Equipment_Type_Code: string (nullable = true)
 |-- Equipment_Type: string (nullable = true)
 |-- Manufacturer: string (nullable = true)
 |-- Model_Number: string (nullable = true)
 |-- Serial_Number: string (nullable = true)
 |-- Install_Date: string (nullable = true)
 |-- Days_In_Service: integer (nullable = true)
 |-- Purchase_Cost_USD: decimal(18,2) (nullable = true)
 |-- Runtime_Hours: integer (nullable = true)
 |-- Expected_Life_Hours: integer (nullable = true)
 |-- Health_Score: double (nullable = true)
 |-- Last_Maintenance_Date: string (nullable = true)
 |-- Days_Since_Maintenance: integer (nullable = true)
 |-- Maintenance_Status: string (nullable = true)
 |-- Failure_Probability_PCT: double (nullable = true)
 |-- Vibration_Level: double (nullable = true)
 |-- Temperature_F: double (nullable = true)
 |-- Pressure_PSI: double (nullable = true)



dbt_operating_costs
root
 |-- Date: string (nullable = true)
 |-- Well_ID: string (nullable = true)
 |-- BOE_Produced: double (nullable = true)
 |-- Labor_Cost_USD: decimal(18,2) (nullable = true)
 |-- Power_Cost_USD: decimal(18,2) (nullable = true)
 |-- Chemical_Cost_USD: decimal(18,2) (nullable = true)
 |-- Water_Disposal_Cost_USD: decimal(18,2) (nullable = true)
 |-- Maintenance_Cost_USD: decimal(18,2) (nullable = true)
 |-- Workover_Cost_USD: decimal(18,2) (nullable = true)
 |-- Total_OPEX_USD: decimal(18,2) (nullable = true)
 |-- OPEX_Per_BOE: double (nullable = true)
 |-- Operating_Netback_USD: decimal(18,2) (nullable = true)
 |-- Is_Profitable: string (nullable = true)



 dbt_production_daily
 root
 |-- Date: string (nullable = true)
 |-- Well_ID: string (nullable = true)
 |-- Well_Name: string (nullable = true)
 |-- Basin: string (nullable = true)
 |-- Well_Type: string (nullable = true)
 |-- Days_On_Production: integer (nullable = true)
 |-- Oil_Production_BOPD: double (nullable = true)
 |-- Gas_Production_MCF: double (nullable = true)
 |-- Water_Production_BWPD: double (nullable = true)
 |-- Gas_Oil_Ratio: integer (nullable = true)
 |-- Water_Cut_PCT: double (nullable = true)
 |-- Oil_Price_USD: decimal(18,2) (nullable = true)
 |-- Gas_Price_USD: decimal(18,2) (nullable = true)
 |-- Gross_Revenue_USD: decimal(18,2) (nullable = true)
 |-- Royalty_USD: decimal(18,2) (nullable = true)
 |-- Severance_Tax_USD: decimal(18,2) (nullable = true)
 |-- Net_Revenue_USD: decimal(18,2) (nullable = true)
 |-- Well_Status: string (nullable = true)
 |-- Decline_Factor: double (nullable = true)


dbt_wells_master
 root
 |-- Well_ID: string (nullable = true)
 |-- Well_Name: string (nullable = true)
 |-- API_Number: string (nullable = true)
 |-- Operator: string (nullable = true)
 |-- Basin: string (nullable = true)
 |-- State: string (nullable = true)
 |-- County: string (nullable = true)
 |-- Latitude: double (nullable = true)
 |-- Longitude: double (nullable = true)
 |-- Well_Type: string (nullable = true)
 |-- Total_Depth_Feet: integer (nullable = true)
 |-- Lateral_Length_Feet: integer (nullable = true)
 |-- Spud_Date: string (nullable = true)
 |-- First_Production_Date: string (nullable = true)
 |-- Days_On_Production: integer (nullable = true)
 |-- Drilling_Cost_USD: integer (nullable = true)
 |-- Initial_Production_BOPD: double (nullable = true)
 |-- Well_Status: string (nullable = true)
 |-- Ownership_Type: string (nullable = true)
 |-- Royalty_Interest_PCT: double (nullable = true)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create a Delta table
# MAGIC CREATE TABLE sales_delta USING DELTA
# MAGIC AS SELECT 1 AS id, 'A' AS product UNION ALL SELECT 2, 'B' AS product;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.sql("select * from sales_delta")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Update a row
# MAGIC UPDATE sales_delta SET product = 'C' WHERE id = 2;
# MAGIC 
# MAGIC -- Delete a row
# MAGIC DELETE FROM sales_delta WHERE id = 1;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.sql("select * from sales_delta")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parquet_df = spark.read.format("parquet") \
.load("abfss://29b83063-d09f-4997-ab9d-c075a0f36775@onelake.dfs.fabric.microsoft.com/f67b15ec-0153-4344-a458-411122433c29/Tables/dbo/sales_delta")
parquet_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
