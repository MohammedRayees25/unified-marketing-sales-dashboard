# Microsoft Fabric Setup Notes

This project is executed directly in Microsoft Fabric.

## What is included

- Fabric Notebook ETL script:
  - `fabric/notebook_etl.py`
- Fabric Warehouse / SQL reference:
  - `sql/fabric_warehouse.sql`
- Power BI report connected to Fabric Lakehouse Gold tables

## How to use with Fabric

1. Create or open a Fabric workspace.
2. Create a Lakehouse.
3. Add the source files / trial-account exports into the Lakehouse Bronze area.
4. Create a Fabric Notebook and use the code from `fabric/notebook_etl.py`.
5. Run the notebook to build Bronze -> Silver -> Gold Delta tables.
6. Optionally use `sql/fabric_warehouse.sql` in a Fabric Warehouse or SQL endpoint.
7. Build the Power BI report on top of the Gold Delta tables.

## Assignment note

Zoho CRM and Zoho Projects data were generated using trial accounts and ingested via CSV export.  
The pipeline is designed to support API ingestion.
