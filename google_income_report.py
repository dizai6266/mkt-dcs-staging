# Databricks notebook source
# MAGIC %md
# MAGIC # Google AdMob Income Report
# MAGIC
# MAGIC 该 Notebook 从 Google AdMob API 获取收入数据。
# MAGIC
# MAGIC - 支持多账号配置
# MAGIC - 按 AdUnit 维度获取报告
# MAGIC - 包含 Country、Format、Platform 等维度

# COMMAND ----------

# MAGIC %pip install google-api-python-client oauth2client

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta
import sys
import os

from googleapiclient import discovery
from googleapiclient.http import build_http
from oauth2client import client

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
_AD_NETWORK = 'google'
_AD_TYPE = 'income'
_DATE_RANGE = 7

# 账号列表
ACCOUNT_NAMES = ["admob-zengjie", "admob-4lb", "admob-gluonint", "admob-481536600510"]

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

def _get_account_id(service):
    """获取 AdMob 账户 ID"""
    accounts = service.accounts().list(pageSize=1).execute()
    if len(accounts['account']) == 1:
        account = accounts['account'][0]['name']
    else:
        raise RuntimeError('Multiple accounts were found.')
    return account


def _fetch_adunits(account_name: str):
    """
    获取账户的 AdUnit 列表
    
    Args:
        account_name: 账号名称
    
    Returns:
        AdUnit ID 列表
    """
    cfg = helper.get_cfg('google_income')
    
    credentials = client.Credentials.new_from_json(json.dumps(cfg.get(account_name)))
    http = credentials.authorize(http=build_http())
    admob = discovery.build('admob', 'v1', http=http)
    account = _get_account_id(admob)

    result = admob.accounts().adUnits().list(parent=account).execute()

    adunits = list()
    for item in result.get('adUnits', []):
        adunits.append(item['adUnitId'])

    return adunits, admob, account


def _fetch_income_report(ds: str, account_name: str, adunits: list, admob, account: str):
    """
    获取收入报告
    
    Args:
        ds: 执行日期
        account_name: 账号名称
        adunits: AdUnit ID 列表
        admob: AdMob 服务实例
        account: 账户 ID
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    start_date = {"year": int(start_dt.year), "month": int(start_dt.month), "day": int(start_dt.day)}
    end_date = {"year": int(end_dt.year), "month": int(end_dt.month), "day": int(end_dt.day)}

    result_reports = list()
    
    for ad_uni_id in adunits: 
        report_spec = {
            'date_range': {
                'start_date': start_date,
                'end_date': end_date
            },
            'dimensions': ['DATE', 'APP', 'AD_UNIT', 'COUNTRY', 'FORMAT', 'PLATFORM'],
            'metrics': ['AD_REQUESTS', 'MATCHED_REQUESTS', 'CLICKS', 'ESTIMATED_EARNINGS', 'IMPRESSIONS', 'IMPRESSION_RPM'],
            'localization_settings': {
                'currency_code': 'USD',
                'language_code': 'en-US'
            },
            'dimensionFilters': [{
                'dimension': 'AD_UNIT',
                'matchesAny': {'values': [ad_uni_id]}
            }]
        }
        request = {'report_spec': report_spec}
        result = admob.accounts().networkReport().generate(
            parent=account, body=request
        ).execute()
        result_reports.append(result)
        print(f'      {ad_uni_id} completed')
        time.sleep(1)

    data_reports = json.dumps(result_reports)
    
    return helper.save_report(
        ad_network=_AD_NETWORK, 
        ad_type=_AD_TYPE, 
        exc_ds=ds, 
        start_ds=start_ds, 
        end_ds=end_ds, 
        report=data_reports, 
        custom=account_name
    )


def _process_and_upload_report(file_path: str):
    """
    处理并上传报告到 S3
    
    Args:
        file_path: 本地文件路径
    """
    result_data = []
    with open(file_path, 'r') as f:
        data_reports = json.load(f)
        for data in data_reports:
            for i in range(0, len(data)):
                item = data[i]
                if item.get('header'):
                    pass
                if item.get('row'):
                    result_json = {}
                    dimensions = item['row']['dimensionValues']
                    metrics = item['row']['metricValues']

                    for key in dimensions.keys():
                        if key == 'DATE':
                            date = dimensions[key]['value'][0:4] + '-' + dimensions[key]['value'][4:6] + '-' + dimensions[key]['value'][6:8]
                            result_json.update({key: date})
                        if key == 'APP' or key == 'AD_UNIT':
                            key_id = str(key) + str('_ID')
                            key_name = str(key) + str('_NAME')
                            try:
                                result_json.update({key_id: dimensions[key]['value']})
                                result_json.update({key_name: dimensions[key]['displayLabel']})
                            except:
                                result_json.update({key: 'other'})
                        if key == 'COUNTRY' or key == 'FORMAT' or key == 'PLATFORM':
                            result_json.update({key: dimensions[key].get('value', '')})

                    for key in metrics.keys():
                        for inner_key in metrics[key].keys():
                            if inner_key == 'microsValue':
                                int_million = int(metrics[key][inner_key]) / 1000000
                                result_json.update({key: int_million})
                            else:
                                result_json.update({key: metrics[key][inner_key]})
                    result_data.append(result_json)

        if result_data:
            helper.upload_json_to_s3(data=result_data, file_path=file_path)


def fetch_income_report_task(ds: str):
    """
    获取 Google AdMob Income 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} income report for {ds}")
    
    for account_name in ACCOUNT_NAMES:
        print(f"\n   🔑 Processing account: {account_name}")
        
        try:
            # Step 1: 获取 AdUnits
            print(f"      📱 Fetching AdUnits...")
            adunits, admob, account = _fetch_adunits(account_name)
            print(f"      Found {len(adunits)} AdUnit(s)")
            
            # Step 2: 获取报告
            print(f"      📊 Fetching reports...")
            file_path = _fetch_income_report(ds, account_name, adunits, admob, account)
            
            # Step 3: 处理并上传
            if file_path:
                print(f"      📤 Processing and uploading...")
                _process_and_upload_report(file_path)
                print(f"      ✅ Completed")
            
        except Exception as e:
            print(f"      ❌ Error: {e}")
            continue
    
    print(f"\n✅ Completed {_AD_NETWORK} income report for {ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

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
