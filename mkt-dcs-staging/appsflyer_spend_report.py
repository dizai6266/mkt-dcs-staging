# Databricks notebook source
# MAGIC %md
# MAGIC # AppsFlyer Spend Report
# MAGIC
# MAGIC 该 Notebook 从 AppsFlyer Master API 获取聚合消耗/安装数据。

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
from utils.config_manager import get_env_mode
import importlib
importlib.reload(helper)

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'appsflyer_spend'
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
    获取 AppsFlyer 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = end_dt.strftime('%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")

    cfg = helper.get_cfg(_AD_NETWORK)
    token = cfg.get('token')

    url = f'https://hq1.appsflyer.com/api/master-agg-data/v4/app/all'

    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {token}'
    }
    params = {
        'from': start_ds,
        'to': end_ds,
        'groupings': 'app_id,pid,af_prt,af_c_id,c,is_primary,install_time,attributed_touch_type,geo',
        'kpis': 'impressions,clicks,installs,cost'
    }

    print("📡 Requesting Master API...")
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise RuntimeError(f"API Error: {response.status_code} {response.text[:200]}")

    helper.save_report(
        ad_network=_AD_NETWORK, 
        ad_type=helper._AD_TYPE_SPEND, 
        report=response.text, 
        exc_ds=ds, 
        start_ds=start_ds, 
        end_ds=end_ds
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
    helper.failure_callback(str(e), f"{_AD_NETWORK}_spend_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

if env_mode != 'staging':
    print("⚠️ 非 staging 模式，跳过本地 preview。")
else:
    try:
        base_root = getattr(helper, "_DATA_BASE_PATH", None) or os.path.join(os.getcwd(), "data_output")
        preview_root = os.path.join(base_root, helper._AD_TYPE_SPEND, _AD_NETWORK)
        print(f"🔎 Scanning preview files under: {preview_root}")

        if not os.path.exists(preview_root):
            print(f"⚠️ Preview directory does not exist: {preview_root}")
        else:
            preview_files = []
            for root, dirs, files in os.walk(preview_root):
                for name in files:
                    if name.endswith('.preview'):
                        preview_files.append(os.path.join(root, name))

            print(f"✅ Found {len(preview_files)} preview file(s)")

            for sample_file in preview_files:
                print(f"\n   Previewing: {sample_file}")
                try:
                    df = pd.read_json(sample_file, lines=True)
                    try:
                        display(df.head(5))
                    except NameError:
                        print(df.head(5).to_string())
                    print(f"   Total rows: {len(df)}\n")
                except Exception as e:
                    print(f"   ❌ Failed to read preview file: {e}")
    except Exception as e:
        print(f"❌ Preview scan error: {e}")
