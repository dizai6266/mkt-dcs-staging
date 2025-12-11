# Databricks notebook source
# MAGIC %md
# MAGIC # Blind Ferret Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Blind Ferret (Influence Mobile) API 获取广告消耗数据。
# MAGIC 支持多账号，按月获取报告。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import time
import calendar
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
_AD_NETWORK = 'blind_ferret'
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
    获取 Blind Ferret 消耗报告
    
    按月获取数据，支持多个 API Key
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    
    # 获取涉及的月份（可能跨月）
    dts = list(set([
        datetime(start_dt.year, start_dt.month, 1), 
        datetime(end_dt.year, end_dt.month, 1)
    ]))
    
    cfg = helper.get_cfg(_AD_NETWORK)
    api_key = cfg.get('api_key')
    keys = api_key.split(',') if api_key else []
    
    if not keys:
        print("⚠️ No API keys found in config.")
        return
    
    # API Key 映射：使用 API key 的前4位作为标识符
    # 创建映射表，key_index -> api_key 前4位
    API_KEY_MAP = {}
    for key_index, key in enumerate(keys, start=1):
        key_identifier = key.strip()[:4] if len(key.strip()) >= 4 else f"key{key_index}"
        API_KEY_MAP[key_index] = key_identifier
    
    print(f"📋 Processing {len(keys)} API key(s) for {len(dts)} month(s)")
    
    for key_index, key in enumerate(keys, start=1):
        # 获取对应的标识符（API key 前4位）
        key_identifier = API_KEY_MAP.get(key_index, f"key{key_index}")
        
        print(f"\n--- Processing API Key {key_index}/{len(keys)} ({key_identifier}) ---")
        
        for dt in dts:
            # 获取月份的最后一天
            end_day = calendar.monthrange(dt.year, dt.month)[1]
            month_end_ds = datetime(dt.year, dt.month, end_day).strftime('%Y-%m-%d')
            month_start_ds = dt.strftime('%Y-%m-%d')
            
            print(f"   📆 Month: {dt.strftime('%Y-%m')} (end date: {month_end_ds})")
            
            req_opt = dict(
                url="https://engage-network.influencemobile.com/reports/v2/offers.csv",
                params={
                    'api_key': key,
                    'date': month_end_ds
                }
            )
            
            print(f"   📡 Fetching report...")
            
            # 使用 helper.fetch_report 进行流式下载
            helper.fetch_report(
                ad_network=_AD_NETWORK,
                ad_type=_AD_TYPE,
                exc_ds=ds,
                start_ds=month_start_ds,
                end_ds=month_end_ds,
                custom=key_identifier,
                **req_opt
            )
            
            print(f"   ✅ Saved report for {dt.strftime('%Y-%m')}")
            
            # 避免 API 限流
            time.sleep(1.0)
    
    print(f"\n✅ Saved all {_AD_NETWORK} reports for {ds}")

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
