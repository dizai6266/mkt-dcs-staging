# Databricks notebook source
# MAGIC %md
# MAGIC # Edge Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Edge (Peak226) API 获取广告消耗数据。

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
_AD_NETWORK = 'edge'
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
    获取 Edge (Peak226) 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = end_dt.strftime('%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取配置
    cfg = helper.get_cfg(_AD_NETWORK)
    api_token = cfg.get('api_token')
    
    # 构建请求
    report_name = 'agency_report'
    fields = [
        'advertiser_bundle',
        'campaign_group_id',
        'campaign_group_name',
        'country',
        'date_localized',
        'mmp_cost_prioritized',
        'mmp_installs'
    ]
    field_str = '&fields='.join(fields)
    
    report_url = f'https://api.peak226.com/v2/joycastlebingo/report/{report_name}?dateRange={start_ds}-{end_ds}&fields={field_str}&format=csv&auth={api_token}'
    
    headers = {"Content-Type": "application/json"}
    
    print(f"📡 Fetching report from: {report_url[:80]}...")
    
    response = requests.get(report_url, headers=headers)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch report: {response.status_code} {response.text}")
    
    # 替换 <nil> 为 0
    report_str = response.text.replace('<nil>', '0')
    
    # 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=report_str,
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds
    )
    
    print(f"\n✅ Saved {_AD_NETWORK} report for {start_ds} to {end_ds}")

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
