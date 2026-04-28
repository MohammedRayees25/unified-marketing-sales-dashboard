from pyspark.sql import functions as F


# Fabric Notebook - PySpark
# Unified Marketing -> Sales -> Project ETL (Bronze -> Silver -> Gold)

bronze_path = "Files/bronze"


# Bronze reads
meta = spark.read.option("header", True).csv(f"{bronze_path}/bronze_meta_ads.csv")
google = spark.read.option("header", True).csv(f"{bronze_path}/bronze_google_ads.csv")
youtube = spark.read.option("header", True).csv(f"{bronze_path}/bronze_youtube_analytics.csv")
crm_leads = spark.read.option("header", True).csv(f"{bronze_path}/bronze_crm_leads.csv")
crm_accounts = spark.read.option("header", True).csv(f"{bronze_path}/bronze_crm_accounts.csv")
crm_deals = spark.read.option("header", True).csv(f"{bronze_path}/bronze_crm_deals.csv")
projects = spark.read.option("header", True).csv(f"{bronze_path}/bronze_projects.csv")
tasks = spark.read.option("header", True).csv(f"{bronze_path}/bronze_tasks.csv")


# Silver layer
silver_meta = (
    meta.withColumnRenamed("Date", "date")
    .withColumnRenamed("Campaign Name", "campaign")
    .withColumnRenamed("Impressions", "impressions")
    .withColumnRenamed("Ctr", "ctr")
    .withColumnRenamed("Spend", "cost")
    .withColumnRenamed("Leads", "leads")
    .withColumn("channel", F.lit("Meta Ads"))
    .withColumn("impressions", F.col("impressions").cast("double"))
    .withColumn("ctr", F.col("ctr").cast("double"))
    .withColumn("cost", F.col("cost").cast("double"))
    .withColumn("leads", F.col("leads").cast("double"))
    .withColumn("clicks", F.round(F.col("impressions") * (F.col("ctr") / 100.0)).cast("double"))
    .select("date", "channel", "campaign", "impressions", "clicks", "cost", "leads")
)

silver_google = (
    google.withColumnRenamed("Date", "date")
    .withColumnRenamed("Campaign Name", "campaign")
    .withColumnRenamed("Impressions", "impressions")
    .withColumnRenamed("Clicks", "clicks")
    .withColumnRenamed("Cost", "cost")
    .withColumnRenamed("Conversions", "leads")
    .withColumn("channel", F.lit("Google Ads"))
    .withColumn("impressions", F.col("impressions").cast("double"))
    .withColumn("clicks", F.col("clicks").cast("double"))
    .withColumn("cost", F.col("cost").cast("double"))
    .withColumn("leads", F.col("leads").cast("double"))
    .select("date", "channel", "campaign", "impressions", "clicks", "cost", "leads")
)

silver_youtube = (
    youtube.withColumnRenamed("Date", "date")
    .withColumnRenamed("Video Name", "campaign")
    .withColumnRenamed("Views", "impressions")
    .withColumnRenamed("Engagement Rate", "engagement_rate")
    .withColumnRenamed("Subscribers", "subscribers")
    .withColumn("channel", F.lit("YouTube"))
    .withColumn("impressions", F.col("impressions").cast("double"))
    .withColumn("engagement_rate", F.col("engagement_rate").cast("double"))
    .withColumn("subscribers", F.col("subscribers").cast("double"))
    .withColumn("clicks", F.round(F.col("impressions") * (F.col("engagement_rate") / 100.0)).cast("double"))
    .withColumn("cost", F.lit(0.0))
    .withColumn("leads", F.round(F.col("subscribers") * F.lit(1.5)).cast("double"))
    .select("date", "channel", "campaign", "impressions", "clicks", "cost", "leads")
)

silver_marketing_leads = silver_meta.unionByName(silver_google).unionByName(silver_youtube).dropDuplicates()

silver_sales_leads = (
    crm_leads.withColumnRenamed("Record Id", "lead_id")
    .withColumnRenamed("Lead Name", "lead_name")
    .withColumnRenamed("Company", "company")
    .withColumnRenamed("Email", "contact_email")
    .withColumnRenamed("Phone", "phone")
    .withColumnRenamed("Lead Source", "source")
    .withColumnRenamed("Lead Owner Id", "lead_owner_id")
    .withColumnRenamed("Lead Owner Name", "lead_owner_name")
    .withColumnRenamed("Lead Status", "status")
    .withColumnRenamed("Created Time", "created_date")
    .dropDuplicates(["lead_id"])
)

silver_accounts = (
    crm_accounts.withColumnRenamed("Account Id", "account_id")
    .withColumnRenamed("Account Name", "account_name")
    .withColumnRenamed("Industry", "industry")
    .withColumnRenamed("Revenue", "revenue")
    .withColumn("revenue", F.col("revenue").cast("double"))
    .dropDuplicates(["account_id"])
)

silver_deals = (
    crm_deals.withColumnRenamed("Record Id", "deal_id")
    .withColumnRenamed("Deal Name", "deal_name")
    .withColumnRenamed("Lead Record Id", "lead_id")
    .withColumnRenamed("Stage", "stage")
    .withColumnRenamed("Amount", "value")
    .withColumnRenamed("Closing Date", "close_date")
    .withColumnRenamed("Account Name", "account_name")
    .withColumnRenamed("Deal Owner Id", "deal_owner_id")
    .withColumnRenamed("Deal Owner Name", "deal_owner_name")
    .withColumn("value", F.col("value").cast("double"))
    .dropDuplicates(["deal_id"])
)

silver_projects = (
    projects.withColumnRenamed("Record Id", "project_id")
    .withColumnRenamed("Deal Record Id", "deal_id")
    .withColumnRenamed("Project Name", "project_name")
    .withColumnRenamed("Account Name", "account_name")
    .withColumnRenamed("Project Status", "status")
    .withColumnRenamed("Project Owner Id", "project_owner_id")
    .withColumnRenamed("Project Owner Name", "project_owner_name")
    .withColumnRenamed("Start Date", "start_date")
    .withColumnRenamed("End Date", "end_date")
    .dropDuplicates(["project_id"])
)

silver_tasks = (
    tasks.withColumnRenamed("Record Id", "task_id")
    .withColumnRenamed("Project Record Id", "project_id")
    .withColumnRenamed("Assigned To", "assigned_to")
    .withColumnRenamed("Task Status", "status")
    .withColumnRenamed("Time Logs (Hours)", "time_logs_hours")
    .withColumnRenamed("Task Owner Id", "task_owner_id")
    .withColumnRenamed("Task Owner Name", "task_owner_name")
    .withColumn("time_logs_hours", F.col("time_logs_hours").cast("double"))
    .withColumn("status", F.when(F.col("status") == "Completed", F.lit("Closed")).otherwise(F.col("status")))
    .dropDuplicates(["task_id"])
)


# Gold layer
gold_marketing_performance = (
    silver_marketing_leads.groupBy("channel", "campaign")
    .agg(
        F.sum("cost").alias("total_spend"),
        F.sum("impressions").alias("total_impressions"),
        F.sum("clicks").alias("total_clicks"),
        F.sum("leads").alias("leads_generated"),
    )
    .withColumn(
        "cost_per_lead",
        F.col("total_spend")
        / F.when(F.col("leads_generated") == 0, F.lit(1)).otherwise(F.col("leads_generated")),
    )
    .withColumn(
        "ctr_pct",
        (F.col("total_clicks") / F.when(F.col("total_impressions") == 0, F.lit(1)).otherwise(F.col("total_impressions")))
        * 100,
    )
)

leads_generated_total = silver_marketing_leads.agg(F.sum("leads").alias("x")).collect()[0]["x"] or 0
total_deals = silver_deals.count()
deals_won = silver_deals.filter(F.lower(F.col("stage")).contains("won")).count()
conversion_rate = (deals_won / total_deals) if total_deals else 0.0
deal_velocity = (deals_won / total_deals) if total_deals else 0.0

gold_sales_pipeline = (
    silver_deals.groupBy("stage")
    .agg(F.count("*").alias("deals_count"), F.sum("value").alias("total_value"))
    .withColumn("revenue_forecast", F.round(F.col("total_value") * F.lit(1.1), 2))
    .withColumn("conversion_rate", F.lit(float(conversion_rate)))
    .withColumn("deal_velocity", F.lit(float(deal_velocity)))
)

won_deals = silver_deals.filter(F.lower(F.col("stage")).contains("won"))
gold_sales_to_projects = (
    won_deals.alias("d")
    .join(silver_projects.alias("p"), F.col("d.deal_id") == F.col("p.deal_id"), "left")
    .select(
        F.col("d.deal_id").alias("deal_id"),
        F.col("d.lead_id").alias("lead_id"),
        F.col("d.account_name").alias("account_name"),
        F.col("d.value").alias("value"),
        F.col("p.project_id").alias("project_id"),
        F.col("p.project_name").alias("project_name"),
        F.col("p.status").alias("project_status"),
        F.when(F.col("p.project_id").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("project_created"),
    )
)

tasks_summary = (
    silver_tasks.groupBy("project_id")
    .agg(
        F.count("*").alias("total_tasks"),
        F.sum(F.when(F.col("status") == "Closed", 1).otherwise(0)).alias("completed_tasks"),
        F.sum("time_logs_hours").alias("hours_logged"),
    )
    .withColumn(
        "task_completion_pct",
        (F.col("completed_tasks") / F.when(F.col("total_tasks") == 0, F.lit(1)).otherwise(F.col("total_tasks")))
        * 100,
    )
)

gold_project_delivery = (
    silver_projects.alias("p")
    .join(tasks_summary.alias("t"), F.col("p.project_id") == F.col("t.project_id"), "left")
    .select(
        F.col("p.project_id"),
        F.col("p.project_name"),
        F.col("p.status"),
        F.coalesce(F.col("t.total_tasks"), F.lit(0)).alias("total_tasks"),
        F.coalesce(F.col("t.completed_tasks"), F.lit(0)).alias("completed_tasks"),
        F.coalesce(F.col("t.task_completion_pct"), F.lit(0.0)).alias("task_completion_pct"),
        F.coalesce(F.col("t.hours_logged"), F.lit(0.0)).alias("hours_logged"),
    )
)

deals_to_projects = gold_sales_to_projects.filter(F.col("project_created") == True).count()
avg_project_value_row = (
    gold_sales_to_projects.filter(F.col("project_created") == True).agg(F.avg("value").alias("avg_project_value")).collect()[0]
)
avg_project_value = avg_project_value_row["avg_project_value"] if avg_project_value_row["avg_project_value"] is not None else 0.0
project_completion_avg = gold_project_delivery.agg(F.avg("task_completion_pct").alias("x")).collect()[0]["x"] or 0.0
revenue_delivered = gold_sales_to_projects.agg(F.sum("value").alias("x")).collect()[0]["x"] or 0.0
leads_qualified = silver_sales_leads.filter(F.lower(F.col("status")) == "qualified").count()

gold_executive_kpis = spark.createDataFrame(
    [
        (
            float(leads_generated_total),
            float(leads_qualified),
            float(deals_won),
            float(conversion_rate),
            float(deal_velocity),
            float(deals_to_projects),
            float(avg_project_value),
            float(project_completion_avg),
            float(revenue_delivered),
        )
    ],
    schema="""
        leads_generated double,
        leads_qualified double,
        deals_won double,
        conversion_rate double,
        deal_velocity double,
        deals_to_projects double,
        average_project_value double,
        project_completion_pct_avg double,
        revenue_delivered double
    """,
)


# Write Delta tables
silver_marketing_leads.write.mode("overwrite").format("delta").saveAsTable("silver_marketing_leads")
silver_sales_leads.write.mode("overwrite").format("delta").saveAsTable("silver_sales_leads")
silver_accounts.write.mode("overwrite").format("delta").saveAsTable("silver_accounts")
silver_deals.write.mode("overwrite").format("delta").saveAsTable("silver_deals")
silver_projects.write.mode("overwrite").format("delta").saveAsTable("silver_projects")
silver_tasks.write.mode("overwrite").format("delta").saveAsTable("silver_tasks")

gold_marketing_performance.write.mode("overwrite").format("delta").saveAsTable("gold_marketing_performance")
gold_sales_pipeline.write.mode("overwrite").format("delta").saveAsTable("gold_sales_pipeline")
gold_sales_to_projects.write.mode("overwrite").format("delta").saveAsTable("gold_sales_to_projects")
gold_project_delivery.write.mode("overwrite").format("delta").saveAsTable("gold_project_delivery")
gold_executive_kpis.write.mode("overwrite").format("delta").saveAsTable("gold_executive_kpis")

print("ETL completed successfully: Bronze -> Silver -> Gold Delta tables created.")
