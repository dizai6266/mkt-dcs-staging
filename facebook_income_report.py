# Databricks notebook source
# MAGIC %md
# MAGIC # Facebook Income Report
# MAGIC
# MAGIC 该 Notebook 从 Facebook Ad Network Analytics API 获取收入数据。
# MAGIC
# MAGIC - 支持多个 Business ID
# MAGIC - 支持多种 Metrics 指标
# MAGIC - 使用 Graph API v16.0

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
_AD_NETWORK = 'facebook'
_AD_TYPE = 'income'
_DATE_RANGE = 7

# Business IDs
BUSINESS_IDS = ['2183226988562760', '187561025190109']

# Metrics 指标列表
_METRICS = [
    'fb_ad_network_request',
    'fb_ad_network_filled_request',
    'fb_ad_network_revenue',
    'fb_ad_network_cpm',
    'fb_ad_network_imp',
    'fb_ad_network_click',
    'fb_ad_network_bidding_response'
]

# 获取 Widget 参数
try:
    dbutils.widgets.text("ds", "", "Execution Date (YYYY-MM-DD)")
    dbutils.widgets.text("metric", "", "Specific Metric (optional)")
    ds_param = dbutils.widgets.get("ds")
    metric_param = dbutils.widgets.get("metric")
except:
    ds_param = ""
    metric_param = ""

if not ds_param:
    ds_param = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"📅 Execution Date: {ds_param}")
print(f"📊 Metric: {metric_param if metric_param else 'All metrics'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core Functions

# COMMAND ----------

def _fetch_income_report(business_id: str, metric: str, ds: str, cfg: dict):
    """
    获取单个 Business 的收入报告
    
    Args:
        business_id: Facebook Business ID
        metric: 指标名称
        ds: 执行日期
        cfg: 配置信息
        
    Returns:
        保存的文件路径
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    url = f'https://graph.facebook.com/v16.0/{business_id}/adnetworkanalytics/'
    params = {
        'metrics': f"['{metric}']",
        'breakdowns': "['country','property','placement_name','platform','display_format','delivery_method']",
        'since': start_ds,
        'until': end_ds,
        'access_token': cfg.get(business_id)
    }
    
    max_retries = 3
    for retry in range(max_retries):
        try:
            response_report = requests.get(url, params=params)
            page_data = json.loads(response_report.text)
            
            if 'data' in page_data.keys():
                data = page_data['data'][0]['results']
            else:
                raise RuntimeError(f"No data in response: {page_data}")
            
            # 处理分页
            if 'next' in page_data.get('paging', dict()).keys():
                next_url = page_data['paging']['next']
                while next_url:
                    response_report = requests.get(next_url)
                    page_data = json.loads(response_report.text)
                    data = data + page_data['data'][0]['results']
                    if 'next' in page_data.get('paging', dict()).keys():
                        next_url = page_data['paging']['next']
                    else:
                        break
            
            # 转换数据格式
            record_list = []
            for row in data:
                record = {
                    'time': row.get('time'),
                    row.get('metric'): row.get('value'),
                }
                # 展开 breakdowns
                for bd in row.get('breakdowns', []):
                    record[bd['key']] = bd.get('value')
                record_list.append(record)
            
            # 保存报告
            return helper.save_report(
                ad_network=_AD_NETWORK,
                ad_type=_AD_TYPE,
                report=json.dumps(record_list),
                exc_ds=ds,
                start_ds=start_ds,
                end_ds=end_ds,
                custom=f'{business_id}_{metric}'
            )
            
        except Exception as e:
            print(f'   ⚠️ Retry {retry + 1}: status_code={response_report.status_code}, error={e}')
            sleep(30)
    
    raise RuntimeError(f'Failed to fetch the income report for {business_id} {metric}')


def fetch_income_report_task(ds: str, metric: str = None):
    """
    获取 Facebook Income 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
        metric: 指定的指标，如果为空则处理所有指标
    """
    print(f"📊 Fetching {_AD_NETWORK} income report for {ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {ds}")
    
    # 处理的 metrics 列表
    metrics_to_process = [metric] if metric else _METRICS
    
    for m in metrics_to_process:
        print(f"\n--- Processing Metric: {m} ---")
        
        for business_id in BUSINESS_IDS:
            print(f"   📱 Fetching for Business ID: {business_id}")
            try:
                _fetch_income_report(business_id, m, ds, cfg)
                print(f"   ✅ Processed Business ID: {business_id}")
            except Exception as e:
                print(f"   ❌ Failed for Business ID {business_id}: {e}")
    
    print(f"\n✅ Saved {_AD_NETWORK} income report for {start_ds} to {ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK} Income")

try:
    fetch_income_report_task(ds_param, metric_param if metric_param else None)
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
