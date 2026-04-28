# Data Engineer assignment – final version

This repository contains the final submission for the **Unified Marketing -> Sales -> Project Dashboard** assignment.

The final implementation uses:

- **Microsoft Fabric Notebook** for ETL using **PySpark**
- **Delta tables in a Fabric Lakehouse** for Bronze, Silver, and Gold layers
- **Power BI** for reporting, connected directly to the Fabric Lakehouse

The earlier local Python/CSV/Streamlit pipeline has been removed because the final submission is now fully aligned to the requirement: **ETL in Microsoft Fabric and reporting in Power BI**.

---

## Final project structure

```text
data-engineer-assignment/
  docs/
    architecture.md
    data_model.md
    fabric_setup.md
  fabric/
    notebook_etl.py
  sql/
    bronze_to_silver.sql
    silver_to_gold.sql
    fabric_warehouse.sql
  requirements.txt          (kept for reference only – not used in Fabric)

**Final solution summary**

**ETL**

ETL runs inside a Fabric Notebook using PySpark. The notebook:

Generates realistic mock data directly inside the notebook (simulating API ingestion from Meta Ads, Google Ads, YouTube, Zoho CRM, and Zoho Projects) – no external CSV files are read.

Cleans and normalizes the data into Silver (in-memory transformations).

Builds KPI-ready business tables in Gold.

Writes the final outputs as Delta tables into the Fabric Lakehouse (bronze_*, silver_*, and gold_* tables).

**Reporting**

The dashboard is built in Power BI and connected directly to the Fabric Lakehouse Delta tables (Gold layer). The report includes:

Marketing performance (spend, leads, CPL, CTR, campaign breakdown)

Sales funnel and conversion KPIs (stage funnel, conversion rate, deal velocity)

Delivery / project progress (task completion gauge, project‑wise completion bar chart, count of active projects)

Sales‑to‑project KPIs (deals converted to projects, revenue delivered, average project value, and a detailed table)

Power BI dashboard screenshots are attached in the submission email (or a live report link is provided separately).

**Deliverables mapping
**

Requirement	File / Artifact
Architecture diagram	docs/architecture.md
ETL implementation (Fabric Notebook)	fabric/notebook_etl.py
Data model documentation	docs/data_model.md
SQL reference scripts	sql/ (reference only, not executed)
Fabric setup notes	docs/fabric_setup.md
Power BI dashboard	Screenshots in email / live link from workspace

**KPI definitions
**
Cost per Lead = Total Spend / Leads Generated

Conversion Rate = Deals Won / Total Deals (simplified proxy)

Deal Velocity = Deals Won / Total Deals (simplified proxy – days not tracked in mock data)

Deals → Projects Conversion = Projects Created / Deals Won

Project Completion % = Completed Tasks / Total Tasks

Revenue Delivered = Sum of Won Deal values that are linked to a project

Avg Project Value = Revenue Delivered / Projects Created

**Data modeling and mapping
**

Marketing Campaign → CRM Lead using Lead Source

CRM Lead → Deal using lead_id

Deal → Project using deal_id

Project → Task using project_id

Attribution is handled via Lead Source in this assignment version. In production, campaign IDs or UTM parameters would be used.

**Reliability and scale notes
**

Pipeline supports incremental ingestion using timestamp columns (created_date, close_date, start_date).

Basic retry and logging patterns are expected at the API ingestion layer.

Silver layer applies deduplication, null handling, and schema normalization.

Data lineage is maintained from Bronze → Silver → Gold for traceability.

The architecture is compatible with Microsoft Fabric Lakehouse, Fabric Pipelines, and Warehouse SQL endpoints.

The design can be extended to event‑driven or near‑real‑time ingestion.

**Notes**

Zoho CRM and Zoho Projects data were generated using trial‑account‑compatible mock data inside the notebook. The notebook is ready to be swapped with real API calls when credentials are available.

requirements.txt is kept only as a reference from the original local prototype. It is not used in the final Fabric execution path (the notebook runs entirely within Fabric’s Spark environment).

The Fabric notebook writes Bronze, Silver, and Gold Delta tables in the Lakehouse. The Power BI report reads only the Gold tables for performance and clarity.

**Submission proof
**

GitHub repository: https://github.com/MohammedRayees25/unified-marketing-sales-dashboard

Fabric notebook: fabric/notebook_etl.py

Power BI dashboard: Screenshots attached in the submission email + report published in Fabric workspace (link provided separately).
