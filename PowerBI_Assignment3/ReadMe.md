# Enterprise Data Engineering Project

**Architecture Pattern:** Medallion Architecture (Bronze–Silver–Gold)  
**Technologies:** Azure Databricks, Apache Spark, Delta Lake, Power BI

---

## 1. Project Objective

This project implements an enterprise-grade data engineering pipeline using the **Medallion Architecture (Bronze–Silver–Gold)** on Azure Databricks. The objective is to demonstrate how raw data is progressively refined into analytics-ready datasets while enforcing data quality, supporting incremental processing, maintaining audit logs, and enabling cost-efficient business intelligence using Power BI.

Only the **Gold layer** is exposed to Power BI. Bronze and Silver layers remain internal to the data platform and are used strictly for processing and validation.

---

## 2. Medallion Architecture Overview

The Medallion Architecture is the core design principle of this project.

- **Bronze Layer:** Raw, immutable data as received from the source
- **Silver Layer:** Cleaned, validated, and trustworthy data
- **Gold Layer:** Aggregated, business-ready data optimized for analytics

Data flows sequentially through Bronze, Silver, and Gold, with each layer having a clearly defined responsibility and boundary.

---

## 3. Bronze Layer – Raw Data Ingestion

The Bronze layer ingests source CSV files without modification. The original schema is preserved, and ingestion metadata such as ingestion timestamp and source system is added. Data is stored as append-only Delta tables to ensure immutability and traceability.

The Bronze layer is not used for analytics or reporting.

---

## 4. Silver Layer – Cleansing and Validation

The Silver layer transforms raw Bronze data into reliable datasets by enforcing data quality rules.

Key responsibilities include:
- Removing duplicate records
- Validating business rules (positive quantity, valid prices, valid store identifiers)
- Recalculating incorrect totals
- Separating invalid records into a quarantine table
- Supporting incremental processing using Delta Lake MERGE operations

The Silver layer represents trusted, row-level data suitable for downstream aggregation.

---

## 5. Gold Layer – Business and Analytics Data

The Gold layer contains curated, analytics-ready datasets derived from Silver data. Business-level aggregations are precomputed to optimize reporting performance.

Gold tables are written as **managed Delta tables in the Hive Metastore**, making them directly accessible to Power BI.

Gold tables include:
- Daily sales summary
- Monthly revenue by region
- Product performance metrics

Only Gold tables are exposed for analytics consumption.

---

## 6. Source Data

The project uses three simulated datasets:
- Sales transactions containing transactional data with intentional data quality issues
- Product master data providing product and category information
- Store data mapping stores to regions

These datasets are uploaded to Databricks and processed through the Medallion pipeline.

---

## 7. Databricks Notebook Structure

Each Databricks notebook corresponds directly to a Medallion layer. The execution order reflects the architecture:

1. Bronze ingestion notebook  
2. Silver processing notebook  
3. Silver UPSERT (MERGE) notebook  
4. Gold aggregation notebook  

Each notebook is independently executable and includes embedded logging logic.

---

## 8. Incremental Processing and Delta Lake

Delta Lake is used to support reliable incremental processing. MERGE (UPSERT) operations ensure idempotent pipeline runs and prevent duplicate records during reruns. ACID transactions and schema enforcement provide consistency and reliability across executions.

---

## 9. Logging and Monitoring

Centralized logging is implemented across all pipeline stages using a Delta audit table.

The logging framework captures:
- Pipeline start and end events
- Record counts per layer
- Rejected (quarantined) record counts
- Error messages
- Event timestamps

This provides operational visibility, debugging capability, and rerun traceability across the Medallion architecture.

---

## 10. Power BI Integration

Power BI Desktop connects to Azure Databricks using the Azure Databricks connector in **Import mode**.

Key characteristics:
- Only Gold tables from the Hive Metastore are imported
- Import mode minimizes Databricks compute usage
- Databricks compute is required only during refresh
- Clusters can be shut down after import to save cost

Authentication is performed using a Databricks Personal Access Token.

---

## 11. Power BI Report Design

The Power BI report consists of two pages.

The Executive Sales Dashboard provides high-level metrics such as total revenue, daily revenue trends, and monthly revenue by region with filtering capabilities.

The Product Performance Dashboard focuses on top products by revenue, revenue by category, and detailed product-level revenue information.

All visuals are built exclusively on Gold tables.

---

## 12. Cost Optimization Strategy

Cost efficiency is achieved through the following decisions:
- Aggregations performed in the Gold layer
- Import mode used instead of DirectQuery
- Small, auto-terminating Databricks compute
- Databricks cluster stopped immediately after Power BI import

---

## 13. Validation

Gold layer aggregates are validated against Databricks queries. Power BI visuals are cross-checked with Gold table values. Logging tables are queried to confirm successful pipeline execution.

---

## 14. Deliverables

The project deliverables include:
- Databricks notebooks for Bronze, Silver, MERGE, and Gold layers
- Source CSV datasets
- Power BI Desktop report file (.pbix)
- This README.md file

---

## 15. How to Run the Project

1. Upload source CSV files to Databricks  
2. Execute notebooks in Medallion order: Bronze, Silver, MERGE, Gold  
3. Connect Power BI Desktop to Databricks  
4. Import Gold tables  
5. Validate report metrics  

---

End of README
