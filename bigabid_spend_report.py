# Databricks notebook source
# MAGIC %md
# MAGIC # BigaBid Spend Report
# MAGIC
# MAGIC 该 Notebook 从 BigaBid API 获取广告消耗数据。

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

# 设置 feishu-notify
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'bigabid'
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
# MAGIC ## 3. Core Functions

# COMMAND ----------

def fetch_spend_report_task(ds: str):
    """
    获取 BigaBid 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} spend report for {ds}")
    
    # 计算日期范围（从执行日期前一天开始往前推）
    end_dt = datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=-1)
    end_ds = end_dt.strftime('%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')

    print(f"   📆 Date Range: {start_ds} to {end_ds}")

    cfg = helper.get_cfg(_AD_NETWORK)
    api_token = cfg.get('api_token')

    req_opt = dict(
        url=f'https://rep.bigabidserv.com/reporting/{api_token}',
        params={
            'start_date': start_ds,
            'end_date': end_ds
        },
        headers={
            "Content-Type": "application/json"
        }
    )

    print(f"   📡 Fetching from BigaBid API...")
    
    # 使用 helper.fetch_report 进行请求和保存
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
    fetch_spend_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_{_AD_TYPE}_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
