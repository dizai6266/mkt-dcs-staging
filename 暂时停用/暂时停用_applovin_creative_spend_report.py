# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin Creative Spend Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin API 获取广告创意维度的消耗数据。

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
_AD_NETWORK = 'applovin_creative'
_AD_TYPE = 'spend'

########################################################
########################################################
# 这里存在数据重复获取的问题，如果需要重新启用再改
_DATE_RANGE = 15
########################################################
########################################################

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
    获取 AppLovin Creative 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    try:
        cfg = helper.get_cfg('applovin')
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        raise

    if not cfg.get('spend'):
        print("⚠️ No spend config found.")
        return

    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    initial_start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    
    print(f"📆 Date Range: {initial_start_dt.strftime('%Y-%m-%d')} to {ds}")
    print(f"📋 Processing {len(cfg.get('spend'))} account(s)")

    # 账号 ID 映射：index 1 -> 53127, index 2 -> 1385759904
    ACCOUNT_ID_MAP = {
        1: '53127',
        2: '1385759904'
    }

    for item in cfg.get('spend'):
        api_key = item.get('api_key')
        account_index = item.get('index')
        
        # 优先使用配置中的 account_id，如果没有则使用映射
        account_id = item.get('account_id') or ACCOUNT_ID_MAP.get(account_index)
        
        if not account_id:
            print(f"⚠️ Skipping account with index {account_index} (no account_id found)")
            continue
        
        print(f"\n--- Processing Account: index={account_index}, account_id={account_id} ---")
        
        start_dt = initial_start_dt
        day_count = 0
        
        while start_dt <= end_dt:
            start_ds = start_dt.strftime('%Y-%m-%d')
            end_ds = start_ds
            
            print(f'   📡 Fetching report for {start_ds}...')

            req_opt = dict(
                url='https://r.applovin.com/probabilisticReport',
                params={
                    'api_key': api_key,
                    'start': start_ds,
                    'end': end_ds,
                    'columns': 'day,impressions,clicks,ctr,conversions,conversion_rate,average_cpa,average_cpc,country,campaign,traffic_source,ad_type,cost,sales,first_purchase,size,device_type,platform,campaign_package_name,campaign_id_external,campaign_ad_type,ad,ad_id,creative_set,creative_set_id',
                    'format': 'csv',
                    'report_type': 'advertiser',
                }
            )
            
            try:
                resp = requests.get(**req_opt)
                if resp.status_code not in [200, 204, 422]:
                    raise RuntimeError(
                        f'Failed to download {_AD_NETWORK} report for {end_ds} (execute_date={ds}): {resp.status_code} {resp.text[:200]}'
                    )
                
                if resp.text:
                    resp.encoding = 'utf-8'
                    report_str = resp.text
                    
                    helper.save_report(
                        ad_network=_AD_NETWORK,
                        ad_type=_AD_TYPE,
                        report=report_str,
                        exc_ds=ds,
                        start_ds=start_ds,
                        end_ds=end_ds,
                        custom=account_id  # 使用 account_id 而不是 index
                    )
                    day_count += 1
                    print(f"     ✅ Saved report for {start_ds}")
                else:
                    print(f"     ⚠️ No data returned for {start_ds}")
                    
            except Exception as e:
                print(f"     ❌ Error processing {start_ds}: {e}")
                raise e

            start_dt += timedelta(days=1)
        
        print(f"   ✅ Processed {day_count} day(s) for account {account_id}")

    print(f"\n✅ Saved {_AD_NETWORK} report for {initial_start_dt.strftime('%Y-%m-%d')} to {ds}")

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
