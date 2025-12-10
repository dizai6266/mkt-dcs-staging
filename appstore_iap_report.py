# Databricks notebook source
# MAGIC %md
# MAGIC # App Store IAP Report
# MAGIC
# MAGIC 该 Notebook 从 App Store Connect API 获取 IAP 销售报告数据。
# MAGIC
# MAGIC - 支持多账号配置
# MAGIC - 支持多日回溯（默认 7 天）
# MAGIC - 自动处理 gzip 压缩响应
# MAGIC - 失败账号自动重试并汇报

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import gzip
import io
import os
import sys
from datetime import datetime, timedelta
from time import sleep

import jwt
import pandas as pd
import requests

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
_AD_NETWORK = 'app_store'
_AD_TYPE = 'iap'
_DATE_RANGE = 7  # 回溯天数

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

def _generate_token(key: dict) -> str:
    """
    生成 App Store Connect API JWT Token
    
    Args:
        key: 包含 kid, iss, secret, vendor 的配置字典
    
    Returns:
        str: JWT Token
    """
    kid = key['kid']
    iss = key['iss']
    secret = key['secret']
    time_e = int((datetime.now() + timedelta(minutes=19)).timestamp())
    
    headers = {
        'alg': 'ES256',
        'kid': kid,
        'typ': 'JWT'
    }
    payload = {
        'iss': iss,
        'exp': time_e,
        'aud': 'appstoreconnect-v1',
    }
    token_b = jwt.encode(payload, secret, algorithm='ES256', headers=headers)

    if isinstance(token_b, bytes):
        token = token_b.decode('utf-8')
    else:
        token = token_b
    return token


def _get_sales_reports(report_date: str, account: str, token: str, vendor_number: str) -> bytes:
    """
    从 App Store Connect API 获取销售报告
    
    Args:
        report_date: 报告日期 (YYYY-MM-DD)
        account: 账号名称
        token: JWT Token
        vendor_number: 供应商编号
    
    Returns:
        bytes: 报告内容 (gzip 压缩)，或 None 如果没有数据
    """
    url = 'https://api.appstoreconnect.apple.com/v1/salesReports'
    headers = {
        'Authorization': f'Bearer {token}'
    }
    params = {
        'filter[frequency]': 'DAILY',
        'filter[reportDate]': report_date,
        'filter[reportSubType]': 'SUMMARY',
        'filter[reportType]': 'SALES',
        'filter[vendorNumber]': vendor_number,
        'filter[version]': '1_1'
    }
    
    r = requests.get(url, params=params, headers=headers, timeout=(60, 300))
    
    if r.status_code != 200:
        if r.status_code == 404:
            print(f"      ⚠️ No report for {report_date} (404)")
            return None
        raise RuntimeError(f'Failed to get report for {report_date}, {r.status_code}: {r.text}')
    
    return r.content


def _process_and_save_report(report_content: bytes, report_date: str, exc_ds: str, account: str):
    """
    处理并保存报告：解压 gzip、转换为 JSONL 格式、上传 S3
    
    Args:
        report_content: gzip 压缩的报告内容
        report_date: 报告日期
        exc_ds: 执行日期
        account: 账号名称
    """
    # 解压 gzip 内容
    with gzip.open(io.BytesIO(report_content), 'rb') as f:
        report_text = f.read().decode('utf-8')
    
    # 读取 TSV 格式，转换为 JSON
    df = pd.read_csv(io.StringIO(report_text), sep='\t')
    
    # 转换为 JSONL 格式
    jsonl_lines = []
    for _, row in df.iterrows():
        jsonl_lines.append(row.to_json(force_ascii=False))
    jsonl_content = '\n'.join(jsonl_lines)
    
    # 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=jsonl_content,
        exc_ds=exc_ds,
        report_ds=report_date,
        custom=account,
        data_format='jsonl'
    )


def fetch_iap_report_task(ds: str):
    """
    获取 App Store IAP 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    
    特点：
    - 多账号并行处理
    - 多日回溯
    - 自动重试（最多 2 次）
    - 失败汇报
    """
    print(f"📊 Fetching {_AD_NETWORK} IAP report for {ds}")
    
    exc_dt = datetime.strptime(ds, '%Y-%m-%d')
    cfg = helper.get_cfg(_AD_NETWORK)
    
    total_success = 0
    total_failed = 0
    failed_accounts = []
    
    # 遍历每一天
    for day_offset in range(_DATE_RANGE):
        report_dt = exc_dt - timedelta(days=day_offset)
        report_date = report_dt.strftime('%Y-%m-%d')
        
        print(f"\n📆 Processing date: {report_date}")
        
        # 遍历每个账号
        for acc_obj in cfg:
            account = acc_obj.get('account')
            enabled = acc_obj.get('enabled', False)
            
            if not enabled:
                continue
            
            key = acc_obj.get('key')
            token = _generate_token(key)
            vendor_number = key.get('vendor')
            
            # 重试逻辑
            max_retries = 2
            success = False
            error = None
            
            for attempt in range(max_retries):
                try:
                    print(f"   📱 Fetching account: {account} (attempt {attempt + 1})")
                    
                    report_content = _get_sales_reports(report_date, account, token, vendor_number)
                    
                    if report_content:
                        _process_and_save_report(report_content, report_date, ds, account)
                        print(f"   ✅ {account} success")
                        total_success += 1
                    
                    success = True
                    break
                    
                except Exception as e:
                    print(f"   ⚠️ {account} failed (attempt {attempt + 1}): {e}")
                    error = e
                    sleep(10)
            
            if not success:
                failed_accounts.append({'account': account, 'date': report_date, 'error': str(error)})
                total_failed += 1
            
            # 请求间隔，避免限流
            sleep(3)
    
    # 汇报失败情况
    if failed_accounts:
        failed_info = '\n'.join([f"account: {f['account']}, date: {f['date']}, error: {f['error']}" for f in failed_accounts])
        print(f"\n⚠️ Some accounts failed:\n{failed_info}")
        
        # 发送告警（可选）
        try:
            helper.failure_callback(
                f"App Store IAP fetch partially failed.\n{failed_info}",
                f"{_AD_NETWORK}_{_AD_TYPE}_report"
            )
        except:
            pass
    
    print(f"\n✅ Completed {_AD_NETWORK} IAP report: {total_success} success, {total_failed} failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_iap_report_task(ds_param)
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
