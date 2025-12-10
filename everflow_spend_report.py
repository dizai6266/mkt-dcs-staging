# Databricks notebook source
# MAGIC %md
# MAGIC # Everflow Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Everflow API 获取广告消耗数据。
# MAGIC 支持多个 API Key，返回 JSON 格式数据。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
import logging
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
_AD_NETWORK = 'everflow'
_AD_TYPE = 'spend'
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

def fetch_spend_report_task(ds: str):
    """
    获取 Everflow 消耗报告
    
    使用 Everflow Reporting API，支持多个 API Key。
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取配置
    cfg = helper.get_cfg(_AD_NETWORK)
    cfg_api_keys = cfg.get('api-key', '')
    api_keys = cfg_api_keys.split(',') if cfg_api_keys else []
    
    if not api_keys:
        print("⚠️ No API keys found in config.")
        return
    
    print(f"📋 Processing {len(api_keys)} API key(s)")
    
    # API 请求配置
    url = 'https://api.eflow.team/v1/advertisers/reporting/entity'
    json_data = {
        "from": start_ds,
        "to": end_ds,
        "timezone_id": 67,
        "currency_id": "USD",
        "columns": [
            {"column": "unix_timestamp"},
            {"column": "country_code"},
            {"column": "app_identifier"},
            {"column": "offer_id"},
            {"column": "advertiser_campaign_name"}
        ]
    }
    
    all_reports = []
    
    for key_index, api_key in enumerate(api_keys, start=1):
        print(f"\n--- Processing API Key {key_index}/{len(api_keys)} ---")
        
        headers = {
            'X-Eflow-API-Key': api_key,
            'content-type': 'application/json'
        }
        
        response = requests.post(url, headers=headers, json=json_data)
        
        if response.status_code != 200:
            logging.error(f"Failed to fetch report: {response.status_code}, {response.text}")
            continue
        
        # 解析响应数据
        response_data = response.json()
        tables = response_data.get('table', [])
        
        for table in tables:
            # 提取维度信息
            dimensions = {}
            for column in table.get('columns', []):
                dimensions[column.get('column_type')] = column.get('label')
            
            # 合并维度和报告数据
            reporting = table.get('reporting', {})
            item = dict(dimensions, **reporting)
            all_reports.append(item)
        
        print(f"   ✅ Fetched {len(tables)} table(s)")
    
    # 保存报告
    if all_reports:
        helper.save_report(
            ad_network=_AD_NETWORK,
            ad_type=_AD_TYPE,
            report=json.dumps(all_reports),
            exc_ds=ds,
            start_ds=start_ds,
            end_ds=end_ds
        )
        print(f"\n✅ Saved {_AD_NETWORK} report with {len(all_reports)} record(s)")
    else:
        print(f"\n⚠️ No data to save for {_AD_NETWORK}")

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
