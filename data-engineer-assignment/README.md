# Data Engineer assignment – final version

## What this is

This repository contains the final submission for the **Unified Marketing -> Sales -> Project Dashboard** assignment.

The final implementation uses:

- **Microsoft Fabric Notebook** for ETL using **PySpark**
- **Delta tables in a Fabric Lakehouse** for Bronze, Silver, and Gold layers
- **Power BI** for reporting, connected directly to the Fabric Lakehouse

The earlier local Python/CSV/Streamlit pipeline has been removed because the final submission is now fully aligned to the requirement: **ETL in Microsoft Fabric and reporting in Power BI**.

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
```

## Final solution summary

### ETL

ETL runs inside a **Fabric Notebook** using **PySpark**. The notebook:

1. Reads source files / trial-account exports into Bronze
2. Cleans and normalizes data into Silver
3. Builds KPI-ready business tables in Gold
4. Writes the final outputs as **Delta tables** into the Fabric Lakehouse

### Reporting

The dashboard is built in **Power BI** and connected directly to the **Fabric Lakehouse Delta tables**.

The report includes:

- Marketing performance
- Sales funnel and conversion KPIs
- Delivery / project progress
- Sales-to-project KPIs such as revenue delivered and average project value

## Deliverables mapping

| Requirement | File / Artifact |
|---|---|
| Architecture diagram | `docs/architecture.md` |
| ETL implementation | `fabric/notebook_etl.py` |
| Data model documentation | `docs/data_model.md` |
| SQL reference scripts | `sql/` |
| Fabric setup notes | `docs/fabric_setup.md` |
| Power BI dashboard | Published in Fabric / Power BI workspace |

## KPI definitions

- Cost per Lead = Total Spend / Leads Generated
- Conversion Rate = Deals Won / Total Deals
- Deal Velocity = Deals Won / Total Deals (simple velocity proxy used in this submission)
- Deals -> Projects Conversion = Projects Created / Deals Won
- Project Completion % = Completed Tasks / Total Tasks
- Revenue Delivered = Sum of Won Deal Values linked to delivered projects
- Avg Project Value = Revenue Delivered / Projects Created

## Data modeling and mapping

- Marketing Campaign -> CRM Lead using `Lead Source`
- CRM Lead -> Deal using `lead_id`
- Deal -> Project using `deal_id`
- Project -> Task using `project_id`

Attribution is handled using `Lead Source` in this assignment version. In a production implementation, this should be enhanced with campaign IDs or UTM parameters.

## Reliability and scale notes

- Pipeline supports incremental ingestion using timestamp columns such as `created_date`, `close_date`, and `start_date`
- Basic retry and logging patterns are expected in the ingestion layer for API failures
- Silver layer applies deduplication, null handling, and schema normalization
- Data lineage is maintained from Bronze -> Silver -> Gold for traceability
- The architecture is compatible with Microsoft Fabric Lakehouse, Fabric Pipelines, and Warehouse SQL endpoints
- The design can be extended to event-driven or near-real-time ingestion

## Notes

- Zoho CRM and Zoho Projects data were generated using trial accounts and ingested via CSV export shape in Fabric.
- The notebook is designed so real API ingestion can be substituted later if credentials/endpoints are available.
- `requirements.txt` is kept only as reference from the original local prototype and is not required for the final Fabric execution path.
