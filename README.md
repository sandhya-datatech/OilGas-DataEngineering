# Oil & Gas-DataEngineering

**Project Overview**
End-to-end data engineering project simulating an oil & gas analytics platform. Transforms raw production, pricing, and operational data into curated, business-ready insights with Power BI dashboards.

**Business problem**
1) Oil & gas companies receive production, pricing, and operational data from multiple sources
2) Data quality issues (nulls, inconsistent dates, inconsistent units) lead to poor reporting
3) Business needs trusted, curated metrics for leadership decisions

**Outcome**
1) Built a medallion architecture in Microsoft Fabric
2) Automated validation, transformation, and reporting
3) Delivered executive-ready Power BI dashboards

**Basic Architecture of the project**
1) Source (CSV) 
2) Bronze Layer (Raw Delta Tables in MS Fabric)
3) Data Quality & Validation (Great Expectations)
4) Silver Layer (Cleaned + Business Logic Applied)
5) Gold Layer (Aggregated, Reporting-Ready Tables)
6) Power BI (Executive Dashboards & KPIs)

**Tech Stack**
Lakehouse & ETL: Microsoft Fabric
Data Format: Delta Tables
Data Quality: Great Expectations
Reporting: Power BI
CI/CD & Version Control: Azure DevOps + GitHub
