-- Bronze -> Silver transformations (reference SQL)
-- SQL scripts may require schema alignment based on source format.

CREATE OR REPLACE VIEW silver_marketing_leads AS
SELECT
    date,
    'Meta Ads' AS channel,
    campaign_name AS campaign,
    impressions,
    CAST(impressions * (ctr / 100.0) AS INTEGER) AS clicks,
    spend AS cost,
    leads
FROM bronze_meta_ads
UNION ALL
SELECT
    date,
    'Google Ads' AS channel,
    campaign_name AS campaign,
    impressions,
    clicks,
    cost,
    conversions AS leads
FROM bronze_google_ads
UNION ALL
SELECT
    date,
    'YouTube' AS channel,
    video_name AS campaign,
    views AS impressions,
    CAST(views * (engagement_rate / 100.0) AS INTEGER) AS clicks,
    0 AS cost,
    CAST(subscribers * 1.5 AS INTEGER) AS leads
FROM bronze_youtube_analytics;

CREATE OR REPLACE VIEW silver_sales_leads AS
SELECT DISTINCT lead_id, source, status, contact_email, created_date
FROM bronze_crm_leads;

CREATE OR REPLACE VIEW silver_deals AS
SELECT DISTINCT deal_id, lead_id, stage, value, close_date, account_name
FROM bronze_crm_deals;

CREATE OR REPLACE VIEW silver_accounts AS
SELECT DISTINCT account_id, account_name, industry, revenue
FROM bronze_crm_accounts;

CREATE OR REPLACE VIEW silver_projects AS
SELECT DISTINCT project_id, deal_id, project_name, account_name, status, start_date, end_date
FROM bronze_projects;

CREATE OR REPLACE VIEW silver_tasks AS
SELECT DISTINCT task_id, project_id, assigned_to, status, time_logs_hours
FROM bronze_tasks;
