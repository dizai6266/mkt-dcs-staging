# Databricks notebook source
# MAGIC %md
# MAGIC # Fyber Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Fyber Reporting API 获取 Offerwall 广告消耗数据。
# MAGIC
# MAGIC - 使用 OAuth 认证
# MAGIC - 异步报告生成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import json
import requests
from datetime import datetime, timedelta
from time import sleep
import sys
import os

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
_AD_NETWORK = 'fyber'
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

def _get_access_token(cfg: dict) -> str:
    """
    获取 Fyber OAuth Access Token
    
    Args:
        cfg: 配置信息
        
    Returns:
        access_token
    """
    resp = requests.post(
        'https://reporting.fyber.com/auth/v1/token?format=csv',
        headers={'Content-Type': 'application/json'},
        json={
            "grant_type": "client_credentials",
            "client_id": cfg.get('client_id'),
            "client_secret": cfg.get('client_secret'),
        }
    )
    
    result = resp.json()
    if result.get('error'):
        raise RuntimeError(result.get('error'))
    
    return result.get('accessToken')


def fetch_spend_report_task(ds: str):
    """
    获取 Fyber Spend 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} spend report for {ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = end_dt.strftime('%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取 Access Token
    print("   🔑 Getting access token...")
    access_token = _get_access_token(cfg)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    data = {
        "source": "event",
        "dateRange": {
            "start": start_ds,
            "end": end_ds
        },
        "metrics": [
            "Offer Impressions",
            "Offer Clicks",
            "Offer Installs",
            "Advertiser Spend",
            "Offer Conversions",
            "Advertiser ARPDEU",
            "Advertiser Offer eCPM"
        ],
        "splits": [
            "Date", "Country", "Campaign ID", "Campaign Name",
            "Publisher App Bundle", "Provider Custom Id"
        ],
        "filters": []
    }
    
    # 提交报告请求
    print("   📡 Submitting report request...")
    post_url = 'https://reporting.fyber.com/api/v1/report/offerwall?format=csv'
    resp_post = requests.post(post_url, json=data, headers=headers)
    
    post_result = resp_post.json()
    if post_result.get('error'):
        raise RuntimeError(post_result.get('error'))
    
    # 下载报告
    print("   📥 Downloading report...")
    report_url = post_result.get('url')
    
    for retry in range(3):
        try:
            resp_data = requests.get(report_url)
            if resp_data.text:
                # 保存报告
                helper.save_report(
                    ad_network=_AD_NETWORK,
                    ad_type=_AD_TYPE,
                    report=resp_data.text,
                    exc_ds=ds,
                    start_ds=start_ds,
                    end_ds=end_ds
                )
                print(f"\n✅ Saved {_AD_NETWORK} spend report for {start_ds} to {end_ds}")
                return
        except Exception as e:
            print(f'   ⚠️ Retry {retry + 1} downloading report: {e}')
            sleep(10)
    
    raise RuntimeError('Failed to download Fyber spend report')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK} Spend")

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
