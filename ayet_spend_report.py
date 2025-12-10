# Databricks notebook source
# MAGIC %md
# MAGIC # Ayet Studios Spend Report
# MAGIC
# MAGIC 该 Notebook 从 AyetStudios API 获取广告消耗数据。
# MAGIC
# MAGIC - 支持多日回溯
# MAGIC - 按国家维度拆分数据
# MAGIC - 自动处理 API 响应格式

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import json
import logging
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

# 设置 feishu-notify
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'ayet'
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

def _fetch_detailed_data(api_key: str, target_date: str) -> list:
    """
    从 AyetStudios API 获取指定日期的详细数据
    
    Args:
        api_key: API 密钥
        target_date: 目标日期 (YYYY-MM-DD)
    
    Returns:
        list: 处理后的记录列表
    """
    url = "https://ayetstudios.com/api2/reporting/countries"
    
    params = {
        'apiKey': api_key,
        'startDate': target_date,
        'endDate': target_date
    }
    
    print(f"      📡 Fetching data for {target_date}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            logging.error(f"Request failed, status code: {response.status_code}, response: {response.text}")
            return []
        
        data = response.json()
        if data.get('status') != 'success':
            logging.error(f"API returned non-success status: {data.get('status')}")
            return []
        
        return _process_detailed_data(data, target_date)
        
    except Exception as e:
        logging.error(f"Request exception: {e}")
        return []


def _process_detailed_data(data: dict, query_date: str) -> list:
    """
    处理 API 响应，提取并展开详细的 campaign 和 country 数据
    
    Args:
        data: API 响应数据
        query_date: 查询日期
    
    Returns:
        list: 处理后的记录列表
    """
    campaigns = data.get('campaignData', [])
    detailed = data.get('detailed', {})
    
    if not detailed:
        logging.warning("No detailed data found")
        return []
    
    # 创建 campaign 映射
    campaign_map = {str(c.get('campaignId')): c for c in campaigns}
    
    # 处理详细数据
    detailed_results = []
    
    for campaign_id, countries in detailed.items():
        campaign_info = campaign_map.get(campaign_id, {})
        identifier = campaign_info.get('identifier', 'Unknown')
        package_name = campaign_info.get('packageName', 'Unknown')
        platform = campaign_info.get('platform', 'Unknown')
        
        for country, metrics in countries.items():
            record = {
                'date': query_date,
                'campaign_id': campaign_id,
                'identifier': identifier,
                'package_name': package_name,
                'platform': platform,
                'country_code': country,
                'impressions': metrics.get('impressions', 0),
                'clicks': metrics.get('clicks', 0),
                'conversions': metrics.get('conversions', 0),
                'adspend': metrics.get('adspend', 0),
                'conversion_rate': metrics.get('conversion_rate', 0)
            }
            detailed_results.append(record)
    
    return detailed_results


def fetch_spend_report_task(ds: str):
    """
    获取 AyetStudios 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    
    特点：
    - 按天逐日拉取
    - 按 campaign 和 country 维度展开
    - 合并多日数据后统一保存
    """
    print(f"📊 Fetching {_AD_NETWORK} spend report for {ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    api_key = cfg.get('apiKey')
    
    if not api_key:
        raise ValueError("API key not found in configuration")
    
    all_reports = []
    failed_dates = []
    
    print(f"   📆 Fetching {_DATE_RANGE} days of data ending on {ds}")
    
    # 循环拉取过去 N 天的数据
    for i in range(_DATE_RANGE):
        date_obj = datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=i)
        query_date = date_obj.strftime('%Y-%m-%d')
        
        try:
            detailed_data = _fetch_detailed_data(api_key, query_date)
            
            if detailed_data:
                all_reports.extend(detailed_data)
                print(f"      ✅ {query_date}: {len(detailed_data)} records")
            else:
                print(f"      ⚠️ {query_date}: No data")
                failed_dates.append(query_date)
            
        except Exception as e:
            logging.error(f"Failed to fetch data for {query_date}: {e}")
            failed_dates.append(query_date)
        
        # 避免 API 限流
        if i < _DATE_RANGE - 1:
            sleep(1)
    
    # 记录失败情况
    if failed_dates:
        logging.warning(f"Failed to fetch data for dates: {failed_dates}")
    
    if not all_reports:
        logging.error(f"No data retrieved for any date in the range")
        return
    
    # 按日期排序
    all_reports.sort(key=lambda x: x['date'])
    
    # 计算日期范围
    start_date = (datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=_DATE_RANGE - 1)).strftime('%Y-%m-%d')
    end_date = ds
    
    print(f"\n   📊 Total records: {len(all_reports)} ({start_date} to {end_date})")
    
    # 按日期统计
    date_counts = {}
    for record in all_reports:
        date = record['date']
        date_counts[date] = date_counts.get(date, 0) + 1
    print(f"   📋 Records per date: {date_counts}")
    
    # 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=json.dumps(all_reports),
        exc_ds=ds,
        start_ds=start_date,
        end_ds=end_date
    )
    
    print(f"\n✅ Saved {_AD_NETWORK} report for {start_date} to {end_date}")

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
