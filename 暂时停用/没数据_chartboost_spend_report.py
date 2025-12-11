# Databricks notebook source
# MAGIC %md
# MAGIC # Chartboost Spend Report (Market Report)
# MAGIC
# MAGIC 该 Notebook 从 Chartboost API 获取广告消耗数据（Campaign Report）。
# MAGIC 使用异步 Job 机制获取报告。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
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

def _request_report_job(user_id: str, user_signature: str, start_ds: str, end_ds: str) -> str:
    """
    请求 Chartboost 创建报告 Job
    
    Args:
        user_id: 用户 ID
        user_signature: 用户签名
        start_ds: 开始日期
        end_ds: 结束日期
        
    Returns:
        job_id: 报告 Job ID
    """
    url = 'https://analytics.chartboost.com/v3/metrics/campaign'
    params = {
        'dateMin': start_ds,
        'dateMax': end_ds,
        'userId': user_id,
        'userSignature': user_signature,
        'groupBy': 'country'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to request report job: {response.text}")
    
    return eval(response.text)['jobId']


def _fetch_report_by_job_id(job_id: str, max_retries: int = 10) -> str:
    """
    根据 Job ID 获取报告数据
    
    Args:
        job_id: 报告 Job ID
        max_retries: 最大重试次数
        
    Returns:
        report_text: 报告内容
    """
    status_url = f'http://analytics.chartboost.com/v3/metrics/jobs/{job_id}?status=true'
    data_url = f'http://analytics.chartboost.com/v3/metrics/jobs/{job_id}'
    
    for attempt in range(1, max_retries + 1):
        print(f"   ⏳ Checking job status (attempt {attempt}/{max_retries})...")
        
        response = requests.get(status_url)
        status = eval(response.text).get('status')
        
        if status == 'created':
            print(f"   ✅ Job completed, fetching data...")
            result = requests.get(data_url)
            return result.text
        
        sleep(3)
    
    raise RuntimeError(f'Report job {job_id} is not ready after {max_retries} attempts.')


def fetch_spend_report_task(ds: str):
    """
    获取 Chartboost Campaign 消耗报告（Market Report）
    
    使用异步 Job 机制：
    1. 请求创建报告 Job
    2. 轮询 Job 状态直到完成
    3. 获取报告数据
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    cfg = helper.get_cfg(_AD_NETWORK)
    spend_cfg = cfg.get('spend')
    
    if not spend_cfg:
        print("⚠️ No spend config found.")
        return
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 获取配置
    user_id = spend_cfg.get('userId')
    user_signature = spend_cfg.get('userSignature')
    
    # Step 1: 请求报告 Job
    print(f"📡 Requesting report job...")
    job_id = _request_report_job(user_id, user_signature, start_ds, end_ds)
    print(f"   Job ID: {job_id}")
    
    # Step 2: 获取报告数据
    report_text = _fetch_report_by_job_id(job_id)
    
    # Step 3: 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=report_text,
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds
    )
    
    print(f"\n✅ Saved {_AD_NETWORK} spend report for {start_ds} to {end_ds}")

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
