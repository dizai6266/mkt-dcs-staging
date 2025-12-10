# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin MAX Ad Revenue Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin MAX API 获取广告收入数据。
# MAGIC
# MAGIC 注意：此报告需要两步请求（先获取报告 URL，再下载报告），无法直接使用 fetch_report。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
import io
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# 动态添加当前目录到 sys.path 以加载 utils
current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

from utils import helper
from utils.config_manager import get_env_mode, setup_feishu_notify
import importlib
importlib.reload(helper)

# 设置 feishu-notify（路径已在 config_manager 中配置）
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'applovin_max_ad_revenue'
_AD_TYPE = 'mediation'

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
# MAGIC ## 3. Task Logic

# COMMAND ----------

def fetch_max_ad_revenue_report_task(ds: str):
    """
    获取 AppLovin MAX 广告收入报告
    
    注意：此 API 需要两步请求：
    1. 获取报告下载 URL
    2. 从 URL 下载实际报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    try:
        cfg = helper.get_cfg('applovin')
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        raise

    report_key = cfg.get('report_key')
    apps = cfg.get('apps')

    if not report_key:
        print("⚠️ No report_key found in config.")
        return

    if not apps:
        print("⚠️ No apps found in config.")
        return

    # 计算日期范围：从 ds-3 到 ds-1（共3天）
    end_dt = datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=-1)
    start_dt = end_dt + timedelta(days=-2)
    delta = timedelta(days=1)

    print(f"📆 Date Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    print(f"📋 Processing {len(apps)} app(s)")

    base_url = "https://r.applovin.com/max/userAdRevenueReport"

    current_date = start_dt
    while current_date <= end_dt:
        report_day = current_date.strftime("%Y-%m-%d")
        print(f"\n--- Processing Date: {report_day} ---")

        for app in apps:
            platform = app.get('platform')
            store_id = app.get('store_id')
            app_name = app.get('app_name')
            custom = f"{platform}_{app_name}"

            print(f"   📱 Processing App: {custom}")

            # Step 1: 获取报告下载 URL
            params = {
                "api_key": report_key,
                "date": report_day,
                "platform": platform,
                "store_id": store_id,
                "aggregated": 'true'
            }

            response = requests.get(base_url, params=params, timeout=300)
            print(f"      Status Code: {response.status_code}")

            if response.status_code not in [200, 204, 422]:
                raise RuntimeError(
                    f'Failed to fetch report URL for {custom} on {report_day}: {response.status_code} {response.text[:200]}'
                )

            result = response.json()
            ad_revenue_report_url = result.get('ad_revenue_report_url', '').replace('\\', '')

            if not ad_revenue_report_url:
                print(f"      ⚠️ No report URL returned for {custom} on {report_day}")
                current_date += delta
                continue

            # Step 2: 从 URL 下载实际报告
            print(f"      📥 Downloading report from URL...")
            
            req_opt = dict(url=ad_revenue_report_url)
            
            # 使用 helper.fetch_report 下载报告
            helper.fetch_report(
                ad_network=_AD_NETWORK,
                ad_type=_AD_TYPE,
                exc_ds=ds,
                start_ds=report_day,
                end_ds=report_day,
                custom=custom,
                **req_opt
            )
            print(f"      ✅ Saved report for {custom} on {report_day}")

        current_date += delta

    print(f"\n✅ Saved {_AD_NETWORK} report for {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_max_ad_revenue_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
