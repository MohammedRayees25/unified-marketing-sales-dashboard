-- Silver -> Gold transformations (reference SQL)
-- SQL scripts may require schema alignment based on source format.

CREATE OR REPLACE VIEW gold_marketing_performance AS
SELECT
    channel,
    campaign,
    SUM(cost) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(leads) AS leads_generated,
    SUM(cost) / NULLIF(SUM(leads), 0) AS cost_per_lead
FROM silver_marketing_leads
GROUP BY channel, campaign;

CREATE OR REPLACE VIEW gold_sales_pipeline AS
SELECT
    stage,
    COUNT(*) AS deals_count,
    SUM(value) AS total_value,
    SUM(CASE WHEN LOWER(stage) = 'won' THEN 1 ELSE 0 END) * 1.0
        / NULLIF((SELECT SUM(leads_generated) FROM gold_marketing_performance), 0) AS conversion_rate,
    SUM(CASE WHEN LOWER(stage) = 'won' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0) AS deal_velocity
FROM silver_deals
GROUP BY stage;

CREATE OR REPLACE VIEW gold_sales_to_projects AS
SELECT
    d.deal_id,
    d.lead_id,
    d.account_name,
    d.value,
    p.project_id,
    p.project_name,
    p.status AS project_status,
    CASE WHEN p.project_id IS NOT NULL THEN 1 ELSE 0 END AS project_created
FROM silver_deals d
LEFT JOIN silver_projects p ON d.deal_id = p.deal_id
WHERE LOWER(d.stage) = 'won';

CREATE OR REPLACE VIEW gold_project_delivery AS
WITH task_summary AS (
    SELECT
        project_id,
        COUNT(*) AS total_tasks,
        SUM(CASE WHEN status IN ('Closed', 'Completed') THEN 1 ELSE 0 END) AS completed_tasks,
        SUM(time_logs_hours) AS hours_logged
    FROM silver_tasks
    GROUP BY project_id
)
SELECT
    p.project_id,
    p.project_name,
    p.status,
    COALESCE(t.total_tasks, 0) AS total_tasks,
    COALESCE(t.completed_tasks, 0) AS completed_tasks,
    COALESCE(100.0 * t.completed_tasks / NULLIF(t.total_tasks, 0), 0) AS task_completion_pct,
    COALESCE(t.hours_logged, 0) AS hours_logged
FROM silver_projects p
LEFT JOIN task_summary t ON p.project_id = t.project_id;
