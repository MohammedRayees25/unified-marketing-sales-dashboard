# Data Model Documentation

## Bronze Delta Tables (Raw)

- `bronze_meta_ads`: campaign-level spend, impressions, ctr, cpc, leads
- `bronze_google_ads`: campaign clicks, impressions, cost, conversions
- `bronze_youtube_analytics`: video-level views, watch time, engagement, subscribers
- `bronze_crm_leads`: lead id, source, status, contact details
- `bronze_crm_accounts`: account id, account name, industry, revenue
- `bronze_crm_deals`: deal id, stage, value, close date, account
- `bronze_projects`: project id, mapped deal id, status, timeline
- `bronze_tasks`: task id, project id, assignee, status, time logs

## Silver Delta Tables (Cleaned / Normalized)

- `silver_marketing_leads`
  - grain: date + channel + campaign
  - fields: date, channel, campaign, impressions, clicks, cost, leads
- `silver_sales_leads`
  - grain: lead_id
  - fields: lead_id, source, status, contact_email, created_date
- `silver_deals`
  - grain: deal_id
  - fields: deal_id, lead_id, stage, value, close_date, account_name
- `silver_accounts`
  - grain: account_id
  - fields: account_id, account_name, industry, revenue
- `silver_projects`
  - grain: project_id
  - fields: project_id, deal_id, project_name, account_name, status, start_date, end_date
- `silver_tasks`
  - grain: task_id
  - fields: task_id, project_id, assigned_to, status, time_logs_hours

## Gold Delta Tables (Business KPIs)

- `gold_marketing_performance`
  - metrics: total_spend, leads_generated, cost_per_lead, ctr_pct, top campaigns
- `gold_sales_pipeline`
  - metrics: deals_count, total_value, conversion_rate, deal_velocity, revenue_forecast by stage
- `gold_sales_to_projects`
  - metrics: won deals converted to projects
- `gold_project_delivery`
  - metrics: task completion %, active/completed tasks, hours logged
- `gold_executive_kpis`
  - metrics: leads generated, leads qualified, deals won, conversion rate, deal velocity, deals->projects, average project value, completion %, revenue delivered

## Relationship Flow

- Marketing lead source (`silver_marketing_leads.channel`) -> CRM lead source (`silver_sales_leads.source`)
- CRM lead (`silver_sales_leads.lead_id`) -> deal (`silver_deals.lead_id`)
- Won deal (`silver_deals.deal_id`) -> project (`silver_projects.deal_id`)
- Project (`silver_projects.project_id`) -> tasks (`silver_tasks.project_id`)

## Execution Context

- ETL is executed in a Microsoft Fabric Notebook using PySpark.
- Final reporting is built in Power BI on top of Fabric Lakehouse Delta tables.
