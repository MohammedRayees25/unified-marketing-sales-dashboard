# Data Engineer assignment – final version

This repository contains the final submission for the **Unified Marketing -> Sales -> Project Dashboard** assignment.

The final implementation uses:

- **Microsoft Fabric Notebook** for ETL using **PySpark**
- **Delta tables in a Fabric Lakehouse** for Gold tables
- **Power BI** for reporting, connected directly to the Fabric Lakehouse

The earlier local Python/CSV/Streamlit pipeline has been removed. The submission now fully meets the requirement: **ETL in Microsoft Fabric and reporting in Power BI**.

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
  requirements.txt


###Final solution summary

###ETL

ETL runs inside a Fabric Notebook using PySpark. The notebook:

Fetches live Zoho CRM deals via OAuth 2.0 (refresh token, pagination) – this is the only dynamic source.

Transforms deals into Silver (clean, rename, cast) and then into Gold tables for Sales KPIs.

Writes the following Gold Delta tables:

gold_sales_pipeline – deals count and total value per stage.

gold_executive_kpis – deals_won and revenue_delivered.

Also creates static mock tables for Marketing (gold_marketing_performance) and Delivery (gold_project_delivery), plus a static mapping table gold_sales_to_projects (won deal → dummy project).

###Why are Marketing and Projects static?

Marketing APIs (Meta, Google, YouTube) were not implemented – the assignment allows sample datasets for non‑critical sources.

Zoho Projects API was attempted but could not be completed due to technical limitations of the trial account (multi‑scope OAuth not supported, endpoint configuration errors). A static placeholder is used instead, documented as a known limitation.

###Reporting
The dashboard is built in Power BI and connected directly to the Fabric Lakehouse Delta tables. The layout is:

Side	Sections
Left	Sales (top‑left) + Sales→Project (bottom‑left)
Right	Marketing (top‑right) + Delivery (bottom‑right)

The report includes all required KPIs:

Marketing: spend, leads, CPL, CTR, campaign breakdown.

Sales: funnel, conversion rate, deal velocity, deals won, revenue delivered.

Delivery: task completion gauge, project‑wise bar chart, active projects.

Sales→Project: deals converted, revenue delivered, average project value, mapping table.

Power BI dashboard screenshots are attached in the submission email (a live report link is also provided separately).

###Deliverables mapping

Requirement	File / Artifact
Architecture diagram	docs/architecture.md
ETL implementation (Fabric Notebook)	fabric/notebook_etl.py
Data model documentation	docs/data_model.md
SQL reference scripts	sql/ (reference only, not executed)
Fabric setup notes	docs/fabric_setup.md
Power BI dashboard	Screenshots in email / live link from workspace

###KPI definitions

Cost per Lead = Total Spend / Leads Generated

Conversion Rate = Deals Won / Total Deals (simplified proxy)

Deal Velocity = Deals Won / Total Deals (simplified proxy)

Deals → Projects Conversion = Projects Created / Deals Won

Project Completion % = Completed Tasks / Total Tasks

Revenue Delivered = Sum of Won Deal values linked to a project

Avg Project Value = Revenue Delivered / Projects Created

###Data modelling and mapping

Marketing Campaign → CRM Lead (attribution by Lead Source)

CRM Lead → Deal via lead_id

Won Deal → Project via deal_id

Project → Task via project_id

In a production environment, campaign IDs or UTM parameters would replace the simple Lead Source attribution.

###Reliability and scale notes

The dynamic CRM pipeline supports incremental ingestion using timestamp columns (created_date, close_date).

Basic retry and logging patterns are expected at the API ingestion layer.

Silver layer applies deduplication, null handling, and schema normalization.

Data lineage is maintained from Bronze → Silver → Gold for traceability.

The architecture is compatible with Microsoft Fabric Lakehouse, Fabric Pipelines, and Warehouse SQL endpoints.

The design can be extended to event‑driven or near‑real‑time ingestion.

###Notes

Zoho CRM is fully dynamic – OAuth 2.0, pagination, live deals, won deals, revenue.

Zoho Projects API integration was attempted but could not be completed due to trial limitations (multi‑scope OAuth not supported, endpoint errors). A static placeholder is used.

Marketing sources (Meta, Google, YouTube) use static mock data – the assignment explicitly allows sample datasets.

requirements.txt is kept only as a reference from the original local prototype and is not used in the final Fabric execution path.

The notebook writes only the necessary Gold tables; Bronze and Silver layers exist logically but are not persisted (acceptable for the assignment).

