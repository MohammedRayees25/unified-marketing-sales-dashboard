-- Microsoft Fabric Warehouse SQL (reference)
-- Use after loading CSV/Parquet files into Fabric Lakehouse tables.

-- Bronze to Silver
CREATE OR ALTER VIEW silver_marketing_leads AS
SELECT [Date] AS [date], 'Meta Ads' AS [channel], [Campaign Name] AS [campaign], [Impressions] AS [impressions],
       CAST([Impressions] * ([Ctr] / 100.0) AS INT) AS [clicks], [Spend] AS [cost], [Leads] AS [leads]
FROM bronze_meta_ads
UNION ALL
SELECT [Date], 'Google Ads', [Campaign Name], [Impressions], [Clicks], [Cost], [Conversions]
FROM bronze_google_ads
UNION ALL
SELECT [Date], 'YouTube', [Video Name], [Views], CAST([Views] * ([Engagement Rate] / 100.0) AS INT), 0, CAST([Subscribers] * 1.5 AS INT)
FROM bronze_youtube_analytics;

CREATE OR ALTER VIEW silver_sales_leads AS
SELECT DISTINCT [Record Id] AS lead_id, [Lead Name] AS lead_name, [Company] AS company,
       [Email] AS contact_email, [Phone] AS phone, [Lead Source] AS source, [Lead Status] AS status
FROM bronze_crm_leads;

CREATE OR ALTER VIEW silver_accounts AS
SELECT DISTINCT [Account Id] AS account_id, [Account Name] AS account_name, [Industry] AS industry, [Revenue] AS revenue
FROM bronze_crm_accounts;

-- Gold example
CREATE OR ALTER VIEW gold_sales_pipeline AS
SELECT
    stage,
    COUNT(*) AS deals_count,
    SUM(value) AS total_value
FROM silver_deals
GROUP BY stage;
