# Databricks notebook source
# MAGIC %md
# MAGIC # Galaxy Store Sales Report
# MAGIC
# MAGIC 该 Notebook 从 Samsung Galaxy Store API 获取 IAP 销售数据。
# MAGIC
# MAGIC - 获取所有内容信息
# MAGIC - 按内容 ID 获取每日销售收入
# MAGIC - 支持按国家/地区维度

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
import sys
import os

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
_AD_NETWORK = 'galaxy_store'
_AD_TYPE = 'iap'
_DATE_RANGE = 7

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

def _get_api_config():
    """获取 Galaxy Store API 配置"""
    cfg = helper.get_cfg(_AD_NETWORK)
    
    access_token = cfg.get('access-token')
    service_account_id = cfg.get('service-account-id')
    api_server_url = cfg.get('api_server_url')
    auth_server_url = cfg.get('auth_server_url')
    gss_auth_token = cfg.get('gss_auth_token')
    seller_locale = cfg.get('sellerLocale')
    seller_id = cfg.get('sellerId')
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "service-account-id": service_account_id,
    }
    
    cookies = {
        "sellerLocale": seller_locale,
        "gss_auth_token": gss_auth_token,
        "auth_server_url": auth_server_url,
        "api_server_url": api_server_url
    }
    
    return headers, cookies, seller_id


def _fetch_content_info(start_ds: str, end_ds: str, headers: dict, cookies: dict, seller_id: str) -> list:
    """
    获取 Galaxy Store 内容信息列表
    
    Args:
        start_ds: 开始日期
        end_ds: 结束日期
        headers: 请求头
        cookies: Cookie
        seller_id: 卖家 ID
    
    Returns:
        内容信息列表
    """
    content_infos = []
    
    post_url = "https://devapi.samsungapps.com/gss/query/sellerMetric"
    payload = json.dumps({
        "sellerId": seller_id,
        "periods": [{
            "startDate": start_ds,
            "endDate": end_ds
        }],
        "noContentMetadata": False,
    })
    
    resp = requests.post(post_url, cookies=cookies, headers=headers, data=payload, timeout=60)
    
    if resp.status_code == 200:
        data = resp.json().get('data', {})
        contents = data.get('contents', {})
        for _, v in contents.items():
            content_infos.append(v)
        print(f"   📱 Found {len(content_infos)} content(s)")
    else:
        print(f"   ⚠️ Failed to fetch content info: {resp.status_code}")
    
    return content_infos


def _fetch_sales_report(start_ds: str, end_ds: str, content_infos: list, headers: dict, cookies: dict) -> list:
    """
    获取销售报告
    
    Args:
        start_ds: 开始日期
        end_ds: 结束日期
        content_infos: 内容信息列表
        headers: 请求头
        cookies: Cookie
    
    Returns:
        销售报告列表
    """
    report = []
    post_url = "https://devapi.samsungapps.com/gss/query/contentMetric"
    metric_ids = ["revenue_iap_order_count"]
    
    for content_info in content_infos:
        content_id = content_info.get('content')
        content_name = content_info.get('content_name')
        
        payload = json.dumps({
            "contentId": content_id,
            "periods": [{
                "startDate": start_ds,
                "endDate": end_ds
            }],
            "noBreakdown": False,
            "metricIds": metric_ids,
            "filters": {},
            "trendAggregation": "day"
        })
        
        resp = requests.post(post_url, cookies=cookies, headers=headers, data=payload, timeout=60)
        
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            periods = data.get('periods', [])
            
            for period in periods:
                content_sales = period.get(content_id, {})
                for metric in metric_ids:
                    if metric != 'revenue_iap_order_count':
                        continue
                    metric_info = content_sales.get(metric, {}).get('dailyTrend', {})
                    for date, daily_v in metric_info.items():
                        for country, revenue in daily_v.get('subValuesMap', {}).get('country', {}).items():
                            report.append({
                                'date': date,
                                'contentId': content_id,
                                'contentName': content_name,
                                'countryCode': country,
                                'revenue': revenue
                            })
    
    print(f"   📊 Collected {len(report)} sales record(s)")
    return report


def fetch_sales_report_task(ds: str):
    """
    获取 Galaxy Store 销售报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} sales report for {ds}")
    
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = end_dt.strftime('%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"   📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取 API 配置
    headers, cookies, seller_id = _get_api_config()
    
    # Step 1: 获取内容信息
    content_infos = _fetch_content_info(start_ds, end_ds, headers, cookies, seller_id)
    
    if not content_infos:
        print("   ⚠️ No content info found, skipping...")
        return
    
    # Step 2: 获取销售报告
    report = _fetch_sales_report(start_ds, end_ds, content_infos, headers, cookies)
    
    if not report:
        print("   ⚠️ No sales data found")
        return
    
    # Step 3: 保存报告
    report_str = json.dumps(report)
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=report_str,
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds
    )
    
    print(f"\n✅ Completed {_AD_NETWORK} sales report for {ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_sales_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_sales_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
