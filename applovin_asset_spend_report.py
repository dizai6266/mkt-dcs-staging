# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin Asset Spend Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin API 获取广告素材维度的消耗数据。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
from datetime import datetime, timedelta, timezone
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
_AD_NETWORK = 'applovin_asset'
_AD_TYPE = 'spend'

# AppLovin Asset 报告使用 UTC 时间的昨天作为目标日期
utc_now = datetime.now(timezone.utc)
ds_param = (utc_now - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"📅 Job Run Date (UTC): {utc_now}")
print(f"🎯 Target Report Date: {ds_param}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Task Logic

# COMMAND ----------

def fetch_spend_report_task(ds: str):
    """
    获取 AppLovin Asset 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)，即报告的目标日期
    """
    try:
        cfg = helper.get_cfg('applovin')
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        raise

    if not cfg.get('spend'):
        print("⚠️ No spend config found.")
        return
    
    # 内容 account_id 映射：用于 CSV 内容中的 account_id 字段
    # index 1 -> '53127', index 2 -> '1385759904'
    ACCOUNT_ID_MAP = {
        1: '53127',
        2: '1385759904'
    }

    # 只处理 index 为 1 和 2 的账号
    target_accounts = [item for item in cfg.get('spend') if item.get('index') in [1, 2]]
    
    if not target_accounts:
        print("⚠️ No target accounts found (index 1 or 2).")
        return
    
    # API Key 映射：使用 API key 的前4位作为文件名标识符
    # 创建映射表，account_index -> api_key 前4位
    API_KEY_MAP = {}
    for item in target_accounts:
        api_key = item.get('api_key')
        account_index = item.get('index')
        if api_key and account_index:
            key_identifier = api_key.strip()[:4] if len(api_key.strip()) >= 4 else f"key{account_index}"
            API_KEY_MAP[account_index] = key_identifier
    
    print(f"📋 Found {len(target_accounts)} target account(s) to process")

    for item in target_accounts:
        api_key = item.get('api_key')
        account_index = item.get('index')
        
        # 文件名标识：使用 api_key 前4位
        file_identifier = API_KEY_MAP.get(account_index)
        
        # 内容 account_id：使用真实的 account_id
        account_id = item.get('account_id') or ACCOUNT_ID_MAP.get(account_index)
        
        if not file_identifier or not account_id:
            print(f"⚠️ Skipping account with index {account_index} (missing mapping)")
            continue
        
        print(f"\n--- Processing Account: index={account_index}, file_id={file_identifier}, account_id={account_id} ---")
        
        for range_val in ['yesterday', 'last_7d']:
            print(f'   📡 Fetching report for {ds} (range={range_val})...')
            
            req_opt = dict(
                url='https://r.applovin.com/assetReport',
                params={
                    'api_key': api_key,
                    'range': range_val,
                    'columns': 'asset_id,asset_name,impressions,clicks,ctr,cost',
                    'format': 'csv'
                }
            )
            
            try:
                resp = requests.get(**req_opt)
                if resp.status_code not in [200, 204, 422]:
                    raise RuntimeError(
                        f'Failed to download {_AD_NETWORK} report for {ds}: {resp.status_code} {resp.text}'
                    )
                
                if resp.text:
                    resp.encoding = 'utf-8'
                    report_str = resp.text
                    
                    # 确定时间范围
                    if range_val == 'yesterday':
                        start_ds = ds
                        end_ds = ds
                    else:
                        # last_7d: ds - 6 days
                        end_dt = datetime.strptime(ds, '%Y-%m-%d')
                        start_dt = end_dt - timedelta(days=6)
                        start_ds = start_dt.strftime('%Y-%m-%d')
                        end_ds = ds

                    # 添加日期列和账号 ID 列处理
                    lines = report_str.strip().split('\n')
                    if lines:
                        header = f"{lines[0]},date,range_type,account_id"
                        modified_lines = [header]
                        for line in lines[1:]:
                            if line.strip():
                                modified_lines.append(f"{line},{ds},{range_val},{account_id}")
                        report_str = '\n'.join(modified_lines)
                    
                    helper.save_report(
                        ad_network=_AD_NETWORK, 
                        ad_type=_AD_TYPE, 
                        report=report_str, 
                        exc_ds=ds, 
                        start_ds=start_ds, 
                        end_ds=end_ds,
                        custom=file_identifier  # 文件名使用 api_key 前几位
                    )
                    print(f"     ✅ Processed account {file_identifier} (account_id: {account_id}) for {range_val}")
                else:
                    print(f"     ⚠️ No data returned for {range_val}")
                    
            except Exception as e:
                print(f"     ❌ Error processing account {account_id} (index {account_index}): {e}")
                raise e
    
    print(f"\n✅ Saved {_AD_NETWORK} report for {ds}")

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