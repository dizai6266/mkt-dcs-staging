# Databricks notebook source
# MAGIC %md
# MAGIC # Exchange Rates Latest Report (Currency Layer)
# MAGIC
# MAGIC 该 Notebook 从 Currency Layer API 获取历史汇率数据（7天范围）。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
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
_AD_NETWORK = 'exchange_rates'
_AD_TYPE = 'exchange_rate'
_DATE_RANGE = 7
_XR_BASE = 'USD'

# API URLs
CL_HISTORY_URL = 'http://api.currencylayer.com/timeframe'

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
            f'Failed to download exchange rates report: {resp.status_code} {resp.text}'
        )


def fetch_currency_layer_task(ds: str):
    """
    从 Currency Layer API 获取历史汇率
    
    获取过去 7 天的汇率数据。
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取配置
    cfg = helper.get_cfg('exchangerate')
    access_key = cfg.get('currencylayer_access_key')
    
    # 请求历史汇率
    url = f'{CL_HISTORY_URL}?start_date={start_ds}&end_date={end_ds}'
    params = {'access_key': access_key}
    
    print(f"📡 Fetching exchange rates from Currency Layer...")
    
    response = requests.get(url=url, params=params)
    _check_response(response)
    
    # 解析响应
    response_data = response.json()
    
    if not response_data.get('success', False):
        error_info = response_data.get('error', {})
        raise RuntimeError(f"Currency Layer API error: {error_info.get('info', 'Unknown error')}")
    
    rates = response_data.get('quotes', {})
    
    # 转换为列表格式
    latest_rates_list = []
    for rate_date, rate_info in rates.items():
        for currency, value in rate_info.items():
            # 移除 USD 前缀（如 USDEUR -> EUR）
            currency_code = currency.replace('USD', '')
            latest_rates_list.append({
                'currency': currency_code,
                'exchange_rate': value,
                'date': rate_date
            })
    
    print(f"   💱 Processed {len(latest_rates_list)} rate record(s)")
    
    # 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=json.dumps(latest_rates_list),
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds,
        custom='currency_layer'
    )
    
    print(f"\n✅ Saved exchange rates for {start_ds} to {end_ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_currency_layer_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_currency_layer_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
