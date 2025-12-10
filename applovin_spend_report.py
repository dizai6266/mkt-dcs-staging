# Databricks notebook source
# MAGIC %md
# MAGIC # AppLovin Spend Report
# MAGIC
# MAGIC 该 Notebook 从 AppLovin API 获取广告消耗数据。
# MAGIC
# MAGIC - 支持多账号配置
# MAGIC - 按天逐日拉取数据
# MAGIC - 自动进行字段名标准化（小写、下划线格式）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import re
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

# 设置 feishu-notify
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'applovin'
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

def _normalize_csv_header(csv_content: str) -> str:
    """
    标准化 CSV 标题行：
    - 所有字段名转为小写
    - 多个单词用下划线连接 (如 "Conversion Rate" → "conversion_rate")
    - 特殊字段名映射：day → date, cost → spend, campaign → campaign_name, campaign_id_external → campaign_id
    """
    lines = csv_content.strip().split('\n')
    if not lines:
        return csv_content
    
    # 获取标题行
    header = lines[0]
    columns = header.split(',')
    
    # 处理每个列名
    new_columns = []
    for col in columns:
        original_col = col.strip()
        
        # 1. 处理特殊字符和空格，转为下划线格式
        processed_col = original_col
        processed_col = processed_col.replace(' ', '_')     # 空格转下划线
        processed_col = processed_col.replace('-', '_')     # 横线转下划线
        processed_col = processed_col.replace('/', '_')     # 斜杠转下划线
        processed_col = processed_col.replace('(', '')      # 移除左括号
        processed_col = processed_col.replace(')', '')      # 移除右括号
        processed_col = processed_col.replace('%', 'rate')  # 百分号转rate
        
        # 2. 转为小写
        processed_col = processed_col.lower()
        
        # 3. 处理连续的下划线
        processed_col = re.sub(r'_+', '_', processed_col)  # 多个下划线合并为一个
        processed_col = processed_col.strip('_')           # 去除首尾下划线
        
        # 4. 特殊字段名映射
        field_mapping = {
            'day': 'date',
            'cost': 'spend',
            'campaign': 'campaign_name',
            'campaign_id_external': 'campaign_id'
        }
        if processed_col in field_mapping:
            processed_col = field_mapping[processed_col]
    
        new_columns.append(processed_col)
    
    # 重新组合标题行
    new_header = ','.join(new_columns)
    lines[0] = new_header
    
    return '\n'.join(lines)


def fetch_spend_report_task(ds: str):
    """
    获取 AppLovin 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    
    特点：
    - 按天逐日拉取（避免大数据量问题）
    - 支持多账号配置
    - 自动标准化 CSV 字段名
    """
    print(f"📊 Fetching {_AD_NETWORK} spend report for {ds}")
    
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    initial_start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    
    cfg = helper.get_cfg(_AD_NETWORK)
    spend_accounts = cfg.get('spend', [])
    
    print(f"   📱 Found {len(spend_accounts)} account(s) to process")
    
    for item in spend_accounts:
        api_key = item.get('api_key')
        account_index = item.get('index')
        print(f"\n   🔑 Processing account: {account_index}")
        
        start_dt = initial_start_dt
        while start_dt <= end_dt:
            start_ds = start_dt.strftime('%Y-%m-%d')
            end_ds = start_ds  # 按天拉取
            
            print(f"      📆 Fetching date: {start_ds}")
            
            req_opt = dict(
                url='https://r.applovin.com/report',
                params={
                    'api_key': api_key,
                    'start': start_ds,
                    'end': end_ds,
                    'columns': 'day,impressions,clicks,ctr,conversions,conversion_rate,average_cpa,average_cpc,country,campaign,traffic_source,ad_type,cost,sales,first_purchase,size,device_type,platform,campaign_package_name,campaign_id_external,campaign_ad_type,ad,ad_id,creative_set,creative_set_id,roas_0d,roas_1d,roas_3d,roas_7d,unique_purchasers_0d,unique_purchasers_1d,unique_purchasers_3d,unique_purchasers_7d,ret_1d,ret_3d,ret_7d',
                    'format': 'csv',
                    'report_type': 'advertiser',
                }
            )
            
            # 发起请求
            resp = requests.get(**req_opt, timeout=(60, 300))
            
            if resp.status_code not in [200, 204, 422]:
                raise RuntimeError(
                    f'Failed to download {_AD_NETWORK} report for {end_ds}: {resp.status_code} {resp.text[:200]}'
                )
            
            if resp.text and resp.status_code == 200:
                resp.encoding = 'utf-8'
                report_str = resp.text
                
                # 标准化 CSV 字段名
                report_str = _normalize_csv_header(report_str)
                
                # 保存报告（使用 custom 参数区分账号）
                helper.save_report(
                    ad_network=_AD_NETWORK,
                    ad_type=_AD_TYPE,
                    report=report_str,
                    exc_ds=ds,
                    start_ds=start_ds,
                    end_ds=end_ds,
                    custom=account_index
                )
                print(f"      ✅ Saved report for {start_ds}")
            else:
                print(f"      ⚠️ No data for {start_ds}")
            
            start_dt += timedelta(days=1)
    
    print(f"\n✅ Completed {_AD_NETWORK} spend report for {ds}")

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
    helper.failure_callback(str(e), f"{_AD_NETWORK}_{_AD_TYPE}_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
