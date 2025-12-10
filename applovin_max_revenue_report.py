# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin MAX Revenue Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin MAX API 获取收入数据。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
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
_AD_NETWORK = 'max'
_AD_TYPE = 'mediation'
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
# MAGIC ## 3. Task Logic

# COMMAND ----------

def fetch_max_revenue_report_task(ds: str):
    """
    获取 AppLovin MAX 收入报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    try:
        cfg = helper.get_cfg('applovin')
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        raise

    api_key = cfg.get('api_key')
    if not api_key:
        print("⚠️ No api_key found in config.")
        return

    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')

    print(f"📆 Date Range: {start_ds} to {end_ds}")

    req_opt = dict(
        url='http://r.applovin.com/maxReport',
        params={
            'api_key': api_key,
            'start': start_ds,
            'end': end_ds,
            'columns': 'day,application,package_name,store_id,platform,network,network_placement,max_placement,max_ad_unit,max_ad_unit_id,max_ad_unit_test,ad_unit_waterfall_name,device_type,ad_format,country,impressions,estimated_revenue,ecpm',
            'format': 'json'
        }
    )

    print(f"📡 Fetching report from: {req_opt['url']}...")

    # 使用 helper.fetch_report 获取报告
    helper.fetch_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds,
        **req_opt
    )

    print(f"✅ Saved {_AD_NETWORK} report for {start_ds} to {end_ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_max_revenue_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_revenue_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
