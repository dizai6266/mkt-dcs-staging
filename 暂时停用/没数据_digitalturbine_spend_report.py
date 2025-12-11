# Databricks notebook source
# MAGIC %md
# MAGIC # Digital Turbine Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Digital Turbine API 获取广告消耗数据。
# MAGIC 使用 OAuth 认证获取 Access Token。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import time
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
_AD_NETWORK = 'digitalturbine'
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

def _get_access_token(client_id: str, client_secret: str) -> str:
    """
    获取 Digital Turbine Access Token
    
    Args:
        client_id: 客户端 ID
        client_secret: 客户端密钥
        
    Returns:
        access_token: 访问令牌
    """
    url = "https://reporting.digitalturbine.com/auth/v1/token"
    headers = {"Content-Type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to get access token: {response.text}")
    
    return response.json()["accessToken"]


def _request_report_and_wait(access_token: str, start_ds: str, end_ds: str, 
                              max_wait_time: int = 3600, poll_interval: int = 60) -> str:
    """
    请求报告并等待 CSV 数据准备就绪
    
    Args:
        access_token: 访问令牌
        start_ds: 开始日期
        end_ds: 结束日期
        max_wait_time: 最大等待时间（秒）
        poll_interval: 轮询间隔（秒）
        
    Returns:
        report_content: 报告内容
    """
    url = "https://reporting.digitalturbine.com/api/v1/report"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    data = {
        "product": "dtgrowth",
        "query": {
            "dateRange": {
                "start": start_ds,
                "end": end_ds
            },
            "metrics": [
                "Impressions",
                "Clicks",
                "Installs",
                "Spend"
            ],
            "splits": [
                "Campaign ID",
                "Campaign Name",
                "Country",
                "Date"
            ],
            "filters": [],
            "reportFormat": "csv"
        }
    }
    
    # 请求报告
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to request report: {response.text}")
    
    csv_url = response.json()["signed_url"]
    print(f"   📥 Report URL obtained, waiting for data...")
    
    # 轮询等待数据准备就绪
    start_time = time.time()
    
    while True:
        report = requests.get(csv_url)
        
        if report.text.strip():
            print(f"   ✅ Report data ready")
            return report.text
        
        elapsed_time = time.time() - start_time
        
        if elapsed_time >= max_wait_time:
            raise RuntimeError(f"Report not ready after {max_wait_time} seconds")
        
        print(f"   ⏳ Waiting... ({int(elapsed_time)}s elapsed)")
        time.sleep(poll_interval)


def fetch_spend_report_task(ds: str):
    """
    获取 Digital Turbine 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    # 日期范围：结束日期为昨天
    end_dt = datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=-1)
    end_ds = end_dt.strftime('%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取配置
    cfg = helper.get_cfg(_AD_NETWORK)
    client_id = cfg.get('client_id')
    client_secret = cfg.get('client_secret')
    
    # Step 1: 获取 Access Token
    print(f"🔐 Getting access token...")
    access_token = _get_access_token(client_id, client_secret)
    print(f"   ✅ Token obtained")
    
    # Step 2: 请求并等待报告
    print(f"📡 Requesting report...")
    report_content = _request_report_and_wait(access_token, start_ds, end_ds)
    
    # Step 3: 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=report_content,
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
