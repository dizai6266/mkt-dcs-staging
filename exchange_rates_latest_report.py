# Databricks notebook source
# MAGIC %md
# MAGIC # Exchange Rates Latest Report (Exchange Rates API)
# MAGIC
# MAGIC 该 Notebook 从 Exchange Rates API (apilayer) 获取最新汇率数据。

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
_AD_NETWORK = 'exchange_rates_api'
_AD_TYPE = 'exchange_rates'
_XR_BASE = 'USD'

# API URLs
EXCHANGE_RATES_API_LATEST_URL = 'https://api.apilayer.com/exchangerates_data/latest'

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

def _check_response(resp):
    """检查 API 响应状态"""
    if resp.status_code != 200 and resp.status_code != 204:
        raise RuntimeError(
            f'Failed to download latest exchange rates report: {resp.status_code} {resp.text}'
        )


def fetch_exchange_rates_task(ds: str):
    """
    从 Exchange Rates API 获取最新汇率
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📡 Fetching latest exchange rates from Exchange Rates API...")
    
    # 获取配置
    cfg = helper.get_cfg('exchangerate')
    secret_key = cfg.get('secretkey')
    
    # 请求最新汇率
    params = {'base': _XR_BASE}
    headers = {'apikey': secret_key}
    
    response = requests.get(
        url=EXCHANGE_RATES_API_LATEST_URL,
        params=params,
        headers=headers
    )
    
    _check_response(response)
    
    # 解析响应
    response_data = response.json()
    latest_rates = response_data.get('rates', {})
    latest_rates_date = response_data.get('date', ds)
    
    print(f"   📅 Rates Date: {latest_rates_date}")
    print(f"   💱 Found {len(latest_rates)} currency rate(s)")
    
    # 转换为列表格式
    latest_rates_list = [
        {
            'currency': currency,
            'exchange_rate': rate,
            'date': latest_rates_date
        }
        for currency, rate in latest_rates.items()
    ]
    
    # 保存报告
    import json
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=json.dumps(latest_rates_list),
        exc_ds=ds,
        report_ds=latest_rates_date
    )
    
    print(f"\n✅ Saved exchange rates for {latest_rates_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_exchange_rates_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_TYPE}_{_AD_NETWORK}_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
