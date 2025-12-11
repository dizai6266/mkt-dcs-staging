# Databricks notebook source
# MAGIC %md
# MAGIC # Google Ads Asset Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Google Ads API 获取广告素材消耗数据。
# MAGIC
# MAGIC - 支持多客户层级账号
# MAGIC - 获取 ad_group_ad_asset_view 数据
# MAGIC - 包含 Campaign、Asset 等维度信息

# COMMAND ----------

# MAGIC %pip install google-ads

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta
import sys
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# 动态添加当前目录到 sys.path 以加载 utils
current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

from utils import helper
from utils.config_manager import get_env_mode, setup_feishu_notify
import importlib
importlib.reload(helper)

# 设置 feishu-notify
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'google_ads_asset'
_AD_TYPE = 'spend'
_DATE_RANGE = 7

# 重试配置
BACKOFF_FACTOR = 5
MAX_RETRIES = 0

# 获取 Widget 参数
try:
    dbutils.widgets.text("ds", "", "Execution Date (YYYY-MM-DD)")
    ds_param = dbutils.widgets.get("ds")
except:
    ds_param = ""

if not ds_param:
    ds_param = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"📅 Execution Date: {ds_param}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core Functions

# COMMAND ----------

def _get_managed_customer_ids():
    """
    获取所有管理的客户 ID
    
    Returns:
        客户 ID 列表，格式为 (seed_customer_id, customer_id, client_customer_id)
    """
    cfg = helper.get_cfg('google_spend')
    client = GoogleAdsClient.load_from_dict(cfg)

    googleads_service = client.get_service("GoogleAdsService")
    customer_service = client.get_service("CustomerService")

    seed_customer_ids = []
    customer_resource_names = (
        customer_service.list_accessible_customers().resource_names
    )
    for customer_resource_name in customer_resource_names:
        customer_id = googleads_service.parse_customer_path(
            customer_resource_name
        )["customer_id"]
        seed_customer_ids.append(customer_id)

    query = """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
    """

    customer_ids_hierarchy = dict()

    for seed_customer_id in seed_customer_ids:
        cfg['login_customer_id'] = seed_customer_id
        login_client = GoogleAdsClient.load_from_dict(cfg)
        googleads_service = login_client.get_service("GoogleAdsService")
       
        unprocessed_customer_ids = [seed_customer_id]
        root_customer_client = None
        customer_ids_to_child_accounts = dict()

        while unprocessed_customer_ids:
            customer_id = int(unprocessed_customer_ids.pop(0))
            response = googleads_service.search(
                customer_id=str(customer_id), query=query
            )

            for googleads_row in response:
                customer_client = googleads_row.customer_client

                if customer_client.level == 0:
                    if root_customer_client is None:
                        root_customer_client = customer_client
                    continue

                if customer_id not in customer_ids_to_child_accounts:
                    customer_ids_to_child_accounts[customer_id] = []

                if customer_client.manager:
                    if (
                        customer_client.id not in customer_ids_to_child_accounts
                        and customer_client.level == 1
                    ):
                        unprocessed_customer_ids.append(customer_client.id)
                        continue
 
                customer_ids_to_child_accounts[customer_id].append(customer_client)

        customer_ids_hierarchy[seed_customer_id] = customer_ids_to_child_accounts

    result_ids = list()
    for seed_customer_id, customer_ids_to_child_accounts in customer_ids_hierarchy.items():
        for customer_id, client_customer_ids in customer_ids_to_child_accounts.items():
            for item in client_customer_ids:
                result_ids.append((str(seed_customer_id), str(customer_id), str(item.id)))
         
    return result_ids


def _issue_search_request(client, customer_id, query):
    """
    执行搜索请求
    
    Args:
        client: GoogleAdsClient 实例
        customer_id: 客户 ID
        query: GAQL 查询语句
    
    Returns:
        (success, result_dict)
    """
    ga_service = client.get_service("GoogleAdsService")
    retry_count = 0
    
    while True:
        try:
            stream = ga_service.search_stream(
                customer_id=customer_id, query=query
            )
            result_strings = []
            result_strings.append('Day,Campaign ID,Campaign,Campaign state,Impressions,Clicks,Cost,Avg. Cost,Conversions,Currency,Account,Time zone,Client name,Customer ID,Asset ID,Asset Name,Asset Type')
            
            for batch in stream:
                for row in batch.results:
                    result_string = f"{row.segments.date},{row.campaign.id},{row.campaign.name},{row.campaign.status},{row.metrics.impressions},{row.metrics.clicks},{row.metrics.cost_micros},{row.metrics.conversions},{row.customer.currency_code},{row.customer.descriptive_name},{row.customer.time_zone},{row.customer.descriptive_name},{row.customer.id},{row.asset.id},{row.asset.name},{row.asset.type}"
                    result_strings.append(result_string)
            return (True, {"results": result_strings})
            
        except GoogleAdsException as ex:
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                return (
                    False,
                    {
                        "exception": ex,
                        "customer_id": customer_id,
                        "query": query,
                    },
                )


def _fetch_spend_report(ds: str, customer_id: tuple):
    """
    获取单个客户的消耗报告
    
    Args:
        ds: 执行日期
        customer_id: (login_customer_id, parent_id, client_customer_id)
    
    Returns:
        文件路径或 None
    """
    login_customer_id, client_customer_id = customer_id[0], customer_id[2]

    cfg = helper.get_cfg('google_spend')
    cfg['login_customer_id'] = login_customer_id

    client = GoogleAdsClient.load_from_dict(cfg)

    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(-1 * _DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')

    campaign_query = """
        SELECT 
            segments.date
            , campaign.id, campaign.name, campaign.status
            , metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.ctr
            , customer.currency_code, customer.descriptive_name, customer.time_zone, customer.descriptive_name, customer.id
            , segments.month, segments.quarter, segments.week, segments.year
            , asset.id, asset.name, asset.type
        FROM ad_group_ad_asset_view 
        WHERE segments.date DURING {0}
    """.format(f'LAST_{_DATE_RANGE}_DAYS')

    results = _issue_search_request(client, client_customer_id, campaign_query)
        
    if not results[0]:
        failure = results[1]
        ex = failure["exception"]
        print(
            f'   ⚠️ Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" for customer_id {failure["customer_id"]}'
        )
        for error in ex.failure.errors:
            print(f'\t   Error: "{error.message}"')
        return None

    if results[0]:
        success = results[1]
        result_str = "\n".join(success["results"])
        return helper.save_report(
            ad_network=_AD_NETWORK, 
            ad_type=_AD_TYPE, 
            exc_ds=ds, 
            start_ds=start_ds, 
            end_ds=end_ds, 
            report=result_str, 
            custom=client_customer_id
        )
    
    return None


def fetch_spend_report_task(ds: str):
    """
    获取 Google Ads Asset 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} spend report for {ds}")
    
    # Step 1: 获取所有客户 ID
    print("   🔍 Fetching managed customer IDs...")
    managed_customer_ids = _get_managed_customer_ids()
    print(f"   📱 Found {len(managed_customer_ids)} customer(s)")
    
    # Step 2: 逐个获取报告
    file_paths = []
    for i, customer_id in enumerate(managed_customer_ids, 1):
        print(f"\n   [{i}/{len(managed_customer_ids)}] Processing customer: {customer_id[2]}")
        file_path = _fetch_spend_report(ds=ds, customer_id=customer_id)
        if file_path:
            file_paths.append(file_path)
            print(f"      ✅ Saved report")
        else:
            print(f"      ⚠️ No data or failed")
    
    print(f"\n✅ Completed {_AD_NETWORK} spend report: {len(file_paths)} file(s) saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_spend_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_spend_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
