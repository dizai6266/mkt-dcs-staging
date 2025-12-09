# Databricks notebook source
# MAGIC %md
# MAGIC # Apple Search Ads Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Apple Search Ads API 获取广告消耗数据。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from functools import reduce
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
_AD_NETWORK = 'apple_search'
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

def _list_dict_duplicate_removal(oldlist):
    """去重列表中的字典元素"""
    func = lambda x, y: x if y in x else x + [y]
    result_list = reduce(func, [[], ] + oldlist)
    return result_list


def _handle_in_dict_data(originKey, data, target: dict):
    """处理嵌套字典数据，展平为单层"""
    for key in data.keys():
        target.update({
            originKey + '_' + key: str(data[key])
        })


def get_spend_token(cfg):
    """获取 Apple Search Ads API Token"""
    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    url = f'https://appleid.apple.com/auth/oauth2/token?grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope=searchadsorg'
    
    print(f"🔑 Requesting token...")
    r = requests.post(url)
    if r.status_code != 200:
        raise Exception(f"apple_search failed to fetch token, error: {r.text}, status_code: {r.status_code}")
    result = r.json()
    token = result.get("access_token")
    return token


def get_campaign_info(start_ds: str, end_ds: str, token, org_id):
    """获取 Campaign 信息"""
    headers = {
        "Authorization": f'Bearer {token}',
        "X-AP-Context": f'orgId={org_id}',
        "Content-Type": "application/json"
    }
    offset, limit = 0, 1000
    param = {
        "startTime": start_ds,
        "returnRowTotals": False,
        "returnGrandTotals": False,
        "endTime": end_ds,
        "granularity": "DAILY",
        "timeZone": "UTC",
        "selector": {
            "orderBy": [
                {
                    'field': "localSpend",
                    "sortOrder": "DESCENDING"
                }
            ],
            "conditions": [],
            "pagination": {
                "offset": offset,
                "limit": limit
            }
        }
    }

    results = []
    print(f"   📡 Fetching campaign info for Org {org_id}...")

    while True:
        param['selector']['pagination'] = {
            "offset": offset,
            "limit": limit
        }
        response = requests.post(
            f'https://api.searchads.apple.com/api/v5/reports/campaigns',
            json=param,
            headers=headers
        )
        if response.status_code != 200:
            raise Exception(
                f"apple_search failed to fetch campaign reports, error: {response.text}, status_code: {response.status_code}"
            )
        
        data_json = response.json()
        if 'data' not in data_json or 'reportingDataResponse' not in data_json['data']:
            print(f"     ⚠️ Unexpected response structure: {data_json.keys()}")
            break
             
        items = data_json['data']['reportingDataResponse'].get('row', [])
        results.extend(items)
        
        pagination = data_json.get('pagination')
        items_per_page = pagination.get('itemsPerPage', 0) if pagination else 0
        
        if items_per_page < limit:
            break
        offset += limit

    return results


def get_campaign_keyword(start_ds: str, end_ds: str, token, org_id, campaign_info):
    """获取 Campaign 关键词数据"""
    headers = {
        "Authorization": f'Bearer {token}',
        "X-AP-Context": f'orgId={org_id}',
        "Content-Type": "application/json"
    }

    offset, limit = 0, 1000
    param = {
        "startTime": start_ds,
        "returnRowTotals": False,
        "returnGrandTotals": False,
        "endTime": end_ds,
        "granularity": "DAILY",
        "timeZone": "UTC",
        "selector": {
            "orderBy": [
                {
                    'field': "localSpend",
                    "sortOrder": "DESCENDING"
                }
            ],
            "conditions": [],
            "pagination": {
                "offset": offset,
                "limit": limit
            }
        }
    }
    campaign_id = campaign_info['campaignId']

    results = []

    while True:
        param['selector']['pagination'] = {
            "offset": offset,
            "limit": limit
        }
        response = requests.post(
            f'https://api.searchads.apple.com/api/v5/reports/campaigns/{campaign_id}/keywords',
            json=param,
            headers=headers
        )
        if response.status_code != 200:
            raise Exception(
                f"apple_search failed to fetch campaign keywords reports, error: {response.text}, status_code: {response.status_code}"
            )
        
        data_json = response.json()
        data, pagination = data_json.get('data', {}), data_json.get('pagination')
        
        if data and 'reportingDataResponse' in data:
            items = data['reportingDataResponse'].get('row', [])
            results.extend(items)
        
        items_per_page = pagination.get('itemsPerPage', 0) if pagination else 0
        if items_per_page < limit:
            break
        offset += limit

    return results


def fetch_spend_report_task(ds: str):
    """
    获取 Apple Search Ads 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    def _parse_detail_data(report_data, campaign_info=dict()):
        headers, detail_data = [], []
        for row in report_data:
            if 'total' in row.keys():
                continue
            else:
                columns = row['granularity'][0].keys() if len(row['granularity']) else []
                headers.extend(columns)
                metadata_keys = row.get('metadata', dict()).keys()
                headers.extend(metadata_keys)
                break

        for row in report_data:
            if 'total' in row:
                continue
            else:
                granularities = row['granularity']
                for item in granularities:
                    result = {}
                    result.update(campaign_info)
                    for key in headers:
                        if key in item:
                            if type(item[key]) is dict:
                                _handle_in_dict_data(key, item[key], result)
                                continue
                            result.update({key: str(item[key])})
                        elif key in row['metadata']:
                            if type(row['metadata'][key]) is dict:
                                _handle_in_dict_data(key, row['metadata'][key], result)
                                continue
                            result.update({key: str(row['metadata'][key])})
                    detail_data.append(result)

        return detail_data

    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    org_ids = cfg.get("org_ids", [])

    token = get_spend_token(cfg)

    campaign_infos = list()
    
    print(f"📋 Processing {len(org_ids)} Organizations...")
    for org_info in org_ids:
        org_id, is_keyword_required = org_info[0], org_info[1]

        campaign_report = get_campaign_info(start_ds=start_ds, end_ds=end_ds, token=token, org_id=org_id)
        print(f"   Org {org_id}: Found {len(campaign_report)} campaigns. Keyword Required: {is_keyword_required}")

        if is_keyword_required:
            campaigns = list()
            for row in campaign_report:
                campaigns.append(
                    {
                        "campaignId": row['metadata']['campaignId'],
                        "campaignName": row['metadata']['campaignName'],
                        'appName': row['metadata']['app']['appName'],
                        'app_admId': row['metadata']['app']['adamId'],
                        'country_code': row['metadata']['countriesOrRegions'][0]
                    }
                )
            reduplicated_campaigns = _list_dict_duplicate_removal(campaigns)
            
            print(f"     🔑 Fetching keywords for {len(reduplicated_campaigns)} campaigns...")
            for campaign_info in reduplicated_campaigns:
                campaign_keyword_report = get_campaign_keyword(
                    start_ds=start_ds, end_ds=end_ds, token=token, org_id=org_id, campaign_info=campaign_info
                )
                detail_data = _parse_detail_data(campaign_keyword_report, campaign_info=campaign_info)
                campaign_infos.extend(detail_data)
        else:
            detail_data = _parse_detail_data(campaign_report)
            campaign_infos.extend(detail_data)

    print(f"📊 Total records: {len(campaign_infos)}")
    
    helper.save_report(
        ad_network=_AD_NETWORK, 
        ad_type=helper._AD_TYPE_SPEND, 
        report=json.dumps(campaign_infos), 
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
