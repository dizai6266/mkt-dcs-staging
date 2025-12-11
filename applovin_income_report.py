# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin Income Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin API 获取发布者收入数据。

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
_AD_NETWORK = 'applovin'
_AD_TYPE = 'income'
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

def fetch_income_report_task(ds: str):
    """
    获取 AppLovin 收入报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    try:
        cfg = helper.get_cfg(_AD_NETWORK)
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        raise

    if not cfg.get('income'):
        print("⚠️ No income config found.")
        return

    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')

    print(f"📆 Date Range: {start_ds} to {end_ds}")
    print(f"📋 Processing {len(cfg.get('income'))} account(s)")

    # API Key 映射：使用 API key 的前4位作为标识符
    # 创建映射表，account_index -> api_key 前4位
    API_KEY_MAP = {}
    for item in cfg.get('income'):
        api_key = item.get('api_key')
        account_index = item.get('index')
        if api_key and account_index:
            key_identifier = api_key.strip()[:4] if len(api_key.strip()) >= 4 else f"key{account_index}"
            API_KEY_MAP[account_index] = key_identifier

    for item in cfg.get('income'):
        api_key = item.get('api_key')
        account_index = item.get('index')
        
        # 优先使用配置中的 account_id，如果没有则使用 API key 前4位
        account_id = item.get('account_id') or API_KEY_MAP.get(account_index)
        
        if not account_id:
            print(f"⚠️ Skipping account with index {account_index} (no account_id found)")
            continue
        
        print(f"\n--- Processing Account: index={account_index}, account_id={account_id} ---")
        
        req_opt = dict(
            url='https://r.applovin.com/report',
            params={
                'api_key': api_key,
                'start': start_ds,
                'end': end_ds,
                'columns': 'day,package_name,impressions,clicks,ctr,revenue,ecpm,country,ad_type,size,zone_id,platform',
                'format': 'json',
                'report_type': 'publisher',
            }
        )

        print(f"   📡 Fetching report from: {req_opt['url']}...")

        # 使用 helper.fetch_report 获取报告
        helper.fetch_report(
            ad_network=_AD_NETWORK,
            ad_type=_AD_TYPE,
            exc_ds=ds,
            start_ds=start_ds,
            end_ds=end_ds,
            custom=account_id,
            **req_opt
        )
        print(f"   ✅ Processed account {account_id}")

    print(f"\n✅ Saved {_AD_NETWORK} report for {start_ds} to {end_ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_income_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_income_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
