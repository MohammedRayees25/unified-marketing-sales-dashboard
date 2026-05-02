import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# ---------- CREDENTIALS ----------
CLIENT_ID = "1000.1NMWQK8NP6P9AQ1FDDUI1ZDYVWMWNQ"
CLIENT_SECRET = "82f1e141e76f546aea7e68eae52c24ebc16788bc1f"
REFRESH_TOKEN_CRM = "1000.7eee9c2989f7d647938a7409d09ed893.4f2aaee833b2020af35a290ee7e4add5"

def get_crm_token():
    url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {
        "refresh_token": REFRESH_TOKEN_CRM,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

access_token = get_crm_token()
headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

# Fetch deals
deals_url = "https://www.zohoapis.in/crm/v5/Deals?fields=id,Deal_Name,Stage,Amount,Closing_Date,Account_Name&page=1&per_page=200"
resp = requests.get(deals_url, headers=headers)
deals_data = resp.json().get("data", [])
print(f"Fetched {len(deals_data)} deals")

# Bronze
schema = StructType([
    StructField("id", StringType(), True),
    StructField("Deal_Name", StringType(), True),
    StructField("Stage", StringType(), True),
    StructField("Amount", LongType(), True),
    StructField("Closing_Date", StringType(), True),
    StructField("Account_Name", StringType(), True)
])
bronze_crm_deals = spark.createDataFrame(deals_data, schema=schema)

# Silver
silver_deals = bronze_crm_deals.select(
    F.col("id").alias("deal_id"),
    F.col("Deal_Name").alias("deal_name"),
    F.col("Stage").alias("stage"),
    F.col("Amount").cast("double").alias("value"),
    F.col("Closing_Date").alias("close_date"),
    F.col("Account_Name").alias("account_name")
).dropDuplicates(["deal_id"])

# Gold
won_condition = F.lower(F.trim(F.col("stage"))).contains("won")
deals_won = silver_deals.filter(won_condition).count()
revenue_delivered = silver_deals.filter(won_condition).agg(F.sum("value")).collect()[0][0] or 0.0

gold_sales_pipeline = silver_deals.groupBy("stage").agg(
    F.count("*").alias("deals_count"),
    F.sum("value").alias("total_value")
)
gold_executive_kpis = spark.createDataFrame([(deals_won, revenue_delivered)], schema="deals_won long, revenue_delivered double")

# Write Delta tables
gold_sales_pipeline.write.mode("overwrite").format("delta").saveAsTable("gold_sales_pipeline")
gold_executive_kpis.write.mode("overwrite").format("delta").saveAsTable("gold_executive_kpis")

print(f"✅ Updated: deals_won = {deals_won}, revenue_delivered = {revenue_delivered}")


from pyspark.sql import Row

# Static Marketing mock
marketing_rows = [
    Row(channel="Meta Ads", campaign="Campaign A", total_spend=4980.00, leads_generated=673, cost_per_lead=7.40, ctr_pct=2.92),
    Row(channel="Google Ads", campaign="Campaign B", total_spend=1930.00, leads_generated=205, cost_per_lead=9.41, ctr_pct=4.52),
    Row(channel="YouTube", campaign="Video X", total_spend=0.00, leads_generated=93, cost_per_lead=0.00, ctr_pct=1.50),
]
gold_marketing_performance = spark.createDataFrame(marketing_rows)
gold_marketing_performance.write.mode("overwrite").format("delta").saveAsTable("gold_marketing_performance")
print("✅ gold_marketing_performance (static mock) created.")

# Static Project Delivery mock
project_rows = [
    Row(project_id="proj_001", project_name="Project Gamma", project_status="Active",
        total_tasks=5, completed_tasks=3, task_completion_pct=60.0, hours_logged=120),
    Row(project_id="proj_002", project_name="Project Alpha", project_status="Completed",
        total_tasks=3, completed_tasks=3, task_completion_pct=100.0, hours_logged=45),
]
gold_project_delivery = spark.createDataFrame(project_rows)
gold_project_delivery.write.mode("overwrite").format("delta").saveAsTable("gold_project_delivery")
print("✅ gold_project_delivery (static placeholder) created.")

# Static mapping for gold_sales_to_projects
spark.sql("DROP TABLE IF EXISTS gold_sales_to_projects")
dummy_mapping = spark.createDataFrame([("deal_003", "Project Gamma", 35000.0, "Active")],
                                      ["deal_id", "project_name", "value", "project_status"])
dummy_mapping.write.mode("overwrite").format("delta").saveAsTable("gold_sales_to_projects")
print("✅ gold_sales_to_projects (static mapping) created.")
