# Databricks notebook source
# MAGIC %md
# MAGIC # Aarki Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Aarki API 获取广告消耗数据。

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
from utils.config_manager import get_env_mode
import importlib
importlib.reload(helper)

# 添加 feishu-notify 路径（根据环境自动切换）
_feishu_notify_path = '/Workspace/Repos/Shared/feishu-notify' if get_env_mode() == 'prod' else '/Workspace/Users/dizai@joycastle.mobi/feishu-notify'
sys.path.append(_feishu_notify_path)
from notifier import Notifier

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'aarki'
_DATE_RANGE = 2

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
    获取 Aarki 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')

    cfg = helper.get_cfg(_AD_NETWORK)
    token = cfg.get('token')

    req_opt = dict(
        url='http://encore.aarki.com/dsp/api/v2/account_summary.csv?by_campaign=y&by_campaign_tag=y&by_country=y&by_store_identifier=y&by_platform=y&by_creative=y&by_size=y&by_placement=y',
        params={
            'token': token,
            'start_date': start_ds,
            'end_date': end_ds
        }
    )

    print(f"📡 Fetching report from: {req_opt['url']}")
    print(f"📆 Date Range: {start_ds} to {end_ds}")

    # 使用 helper.fetch_report 进行流式下载，避免 OOM
    helper.fetch_report(
        ad_network=_AD_NETWORK,
        ad_type=helper._AD_TYPE_SPEND,
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
