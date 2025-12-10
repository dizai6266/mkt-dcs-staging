# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin MAX Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin MAX API 获取广告单元配置数据。

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
from utils.data_parser import convert_applovin_max_config
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
_AD_NETWORK = 'max'
_AD_TYPE = 'mediation_config'
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

def get_ad_unit_ids(management_key: str):
    """获取所有广告单元 ID"""
    url = 'https://o.applovin.com/mediation/v1/ad_units'
    headers = {'Api-Key': management_key}

    response = requests.get(url, headers=headers, timeout=300)
    if response.status_code != 200:
        raise RuntimeError(f'Failed to fetch ad units: {response.status_code} {response.text[:200]}')
    
    results = [item['id'] for item in response.json()]
    return results


def fetch_max_report_task(ds: str):
    """
    获取 AppLovin MAX 配置报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    try:
        cfg = helper.get_cfg('applovin')
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        raise

    management_key = cfg.get('management_key')
    if not management_key:
        print("⚠️ No management_key found in config.")
        return

    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')

    print(f"📆 Date Range: {start_ds} to {end_ds}")

    # 获取所有广告单元 ID
    print("📡 Fetching ad unit IDs...")
    ad_units = get_ad_unit_ids(management_key)
    print(f"📋 Found {len(ad_units)} ad unit(s)")

    # 收集所有广告单元的数据
    all_records = []
    
    for ad_unit in ad_units:
        print(f"   📦 Processing ad unit: {ad_unit}")
        
        try:
            url = f'https://o.applovin.com/mediation/v1/ad_unit/{ad_unit}'
            headers = {'Api-Key': management_key}
            params = {'fields': 'ad_network_settings'}
            
            response = requests.get(url, headers=headers, params=params, timeout=300)
            
            if response.status_code != 200:
                print(f"      ⚠️ Failed to fetch ad unit {ad_unit}: {response.status_code}")
                continue
            
            # 使用专用转换器展开 ad_network_settings
            jsonl_content, row_count = convert_applovin_max_config(response.text)
            
            if jsonl_content:
                # 收集记录
                for line in jsonl_content.split('\n'):
                    if line.strip():
                        all_records.append(line)
                print(f"      ✅ Extracted {row_count} network records")
            else:
                print(f"      ⚠️ No data extracted for ad unit {ad_unit}")
                
        except Exception as e:
            print(f"      ❌ Error processing ad unit {ad_unit}: {e}")
            continue

    print(f"\n📊 Total records collected: {len(all_records)}")
    
    # 合并所有记录并保存
    if all_records:
        combined_content = '\n'.join(all_records)
        
        helper.save_report(
            ad_network=_AD_NETWORK,
            ad_type=_AD_TYPE,
            report_content=combined_content,
            exc_ds=ds,
            start_ds=start_ds,
            end_ds=end_ds,
            data_format='jsonl'  # 已经是 JSONL 格式
        )
        print(f"✅ Saved {_AD_NETWORK} report for {start_ds} to {end_ds}")
    else:
        print(f"⚠️ No data to save for {_AD_NETWORK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_max_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
