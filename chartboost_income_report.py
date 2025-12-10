# Databricks notebook source
# MAGIC %md
# MAGIC # Chartboost Income Report
# MAGIC
# MAGIC 该 Notebook 从 Chartboost API 获取收入数据。
# MAGIC 支持 v3 和 v5 两种 API 版本。

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
from time import sleep
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
_AD_NETWORK = 'chartboost'
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

def _get_oauth_token(client_id: str, client_secret: str) -> str:
    """
    获取 Chartboost v5 OAuth Token
    
    Args:
        client_id: 客户端 ID
        client_secret: 客户端密钥
        
    Returns:
        access_token: OAuth 访问令牌
    """
    response = requests.post(
        url='https://api.chartboost.com/v5/oauth/token',
        headers={'Content-Type': 'application/json'},
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": "https://public.api.gateway.chartboost.com",
            "grant_type": "client_credentials"
        }
    )
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to get OAuth token: {response.text}")
    
    return response.json()['access_token']


def fetch_income_report_task(ds: str):
    """
    获取 Chartboost App Country Income 报告
    
    支持 v3 和 v5 两种 API 版本
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    cfg = helper.get_cfg(_AD_NETWORK)
    income_cfg = cfg.get('income')
    
    if not income_cfg:
        print("⚠️ No income config found.")
        return
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    print(f"📋 Processing {len(income_cfg)} account(s)")
    
    for index, item in enumerate(income_cfg, start=1):
        try:
            if item.get('ver') == 'v5':
                # v5 API (OAuth)
                print(f"\n--- Processing Account {index} (v5 API) ---")
                
                token = _get_oauth_token(
                    item.get('clientId'), 
                    item.get('clientSecret')
                )
                
                url = 'https://api.chartboost.com/v5/analytics/appcountry'
                headers = {
                    'Authorization': f"Bearer {token}",
                    'Content-Type': 'application/json;charset=UTF-8'
                }
                params = {
                    'dateMin': start_ds,
                    'dateMax': end_ds,
                    'adLocation': 'all'
                }
                
                response = requests.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                    logging.error(f"Failed to fetch v5 report: {response.text}")
                    continue
                
                custom_id = item.get('clientId')
                
            else:
                # v3 API (userId/userSignature)
                print(f"\n--- Processing Account {index} (v3 API) ---")
                
                url = 'https://analytics.chartboost.com/v3/metrics/appcountry'
                params = {
                    'dateMin': start_ds,
                    'dateMax': end_ds,
                    'userId': item.get('userId'),
                    'userSignature': item.get('userSignature'),
                    'adLocation': 'all'
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    logging.error(f"Failed to fetch v3 report: {response.text}")
                    continue
                
                custom_id = item.get('userId')
            
            print(f"   📡 Fetching report...")
            
            # 保存报告
            helper.save_report(
                ad_network=_AD_NETWORK,
                ad_type=_AD_TYPE,
                report=response.text,
                exc_ds=ds,
                start_ds=start_ds,
                end_ds=end_ds,
                custom=custom_id
            )
            
            print(f"   ✅ Processed account {index}")
            
        except Exception as e:
            logging.error(f"Failed to process account {index}: {e}")
            continue
    
    print(f"\n✅ Saved {_AD_NETWORK} income report for {start_ds} to {end_ds}")

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
