# Databricks notebook source
# MAGIC %md
# MAGIC # Fyber Income Report
# MAGIC
# MAGIC 该 Notebook 从 Fyber Revenue Desk API 获取发布者收入数据。
# MAGIC
# MAGIC - 使用 OAuth 1.0 签名认证
# MAGIC - 支持多 Publisher ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import json
import requests
import time
import random
import hmac
import hashlib
import base64
from operator import itemgetter
from urllib.parse import quote
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
_AD_TYPE = 'income'
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

def _fill_signature(s_url: str, consumer_key: str, consumer_secret: str) -> dict:
    """
    生成 OAuth 1.0 签名
    
    Args:
        s_url: 请求 URL
        consumer_key: OAuth Consumer Key
        consumer_secret: OAuth Consumer Secret
        
    Returns:
        包含签名参数的字典
    """
    data = {}
    data['oauth_consumer_key'] = consumer_key
    data['oauth_signature_method'] = 'HMAC-SHA1'
    data['oauth_timestamp'] = str(int(time.time()))
    data['oauth_nonce'] = str(random.random())
    data['oauth_version'] = '1.0'
    
    querystr = '&'.join(
        '{}={}'.format(k, quote(v)) 
        for k, v in sorted(data.items(), key=itemgetter(0))
    )
    str2sign = 'GET&{}&{}'.format(quote(s_url, safe=''), quote(querystr))
    sign = hmac.new(
        consumer_secret.encode('utf-8'), 
        str2sign.encode('utf-8'), 
        digestmod=hashlib.sha1
    ).digest()
    data['oauth_signature'] = str(base64.b64encode(sign), 'utf-8')
    
    return data


def _transform_income_data(data: dict) -> list:
    """
    转换 Fyber 收入数据格式
    
    Args:
        data: 原始 API 响应数据
        
    Returns:
        转换后的记录列表
    """
    results = []
    
    for app_data in data.get('apps', []):
        spots = app_data.get('spots', [])
        for spot in spots:
            spot_id = str(spot.get('spotId'))
            for unit in spot.get('units', []):
                result = {'spotid': spot_id}
                for key, value in unit.items():
                    if key == 'date':
                        # 转换时间戳为日期字符串
                        date_str = time.strftime('%Y-%m-%d', time.localtime(value))
                        result[key] = date_str
                    else:
                        result[key] = str(value)
                results.append(result)
    
    return results


def fetch_income_report_task(ds: str):
    """
    获取 Fyber Income 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} income report for {ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ts = str(time.mktime(time.strptime(str(end_dt), "%Y-%m-%d %H:%M:%S")))
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ts = str(time.mktime(time.strptime(str(start_dt), "%Y-%m-%d %H:%M:%S")))
    
    start_ds = start_dt.strftime('%Y-%m-%d')
    end_ds = end_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    income_config = cfg.get('income', [])
    print(f"   📱 Processing {len(income_config)} publisher(s)")
    
    for item in income_config:
        publisher_id = item.get('publisherId')
        consumer_key = str(item.get('comsumer_key'))
        consumer_secret = str(item.get('comsumer_secret')) + '&'
        
        print(f"\n   --- Processing Publisher: {publisher_id} ---")
        
        # 构建 API URL
        api_url = f'https://revenuedesk.fyber.com/iamp/services/performance/{publisher_id}/vamp/{start_ts}/{end_ts}'
        
        for retry in range(5):
            try:
                # 生成 OAuth 签名
                oauth_data = _fill_signature(api_url, consumer_key, consumer_secret)
                
                url = (
                    f"{api_url}?"
                    f"oauth_consumer_key={oauth_data['oauth_consumer_key']}"
                    f"&oauth_signature_method={oauth_data['oauth_signature_method']}"
                    f"&oauth_timestamp={oauth_data['oauth_timestamp']}"
                    f"&oauth_nonce={oauth_data['oauth_nonce']}"
                    f"&oauth_version={oauth_data['oauth_version']}"
                    f"&oauth_signature={oauth_data['oauth_signature']}"
                )
                
                headers = {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                }
                
                response = requests.get(url, headers=headers)
                data = response.json()
                
                if data.get('apps'):
                    # 转换数据格式
                    records = _transform_income_data(data)
                    
                    # 保存报告
                    helper.save_report(
                        ad_network=_AD_NETWORK,
                        ad_type=_AD_TYPE,
                        report=json.dumps(records),
                        exc_ds=ds,
                        start_ds=start_ds,
                        end_ds=end_ds,
                        custom=publisher_id
                    )
                    print(f"   ✅ Processed publisher: {publisher_id}")
                    break
                    
            except Exception as e:
                print(f'   ⚠️ Retry {retry + 1} for publisher {publisher_id}: {e}')
                sleep(10)
        else:
            print(f"   ❌ Failed to fetch report for publisher: {publisher_id}")
    
    print(f"\n✅ Saved {_AD_NETWORK} income report for {start_ds} to {end_ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK} Income")

try:
    fetch_income_report_task(ds_param)
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
