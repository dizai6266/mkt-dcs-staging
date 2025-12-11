# Databricks notebook source
# MAGIC %md
# MAGIC # Google Play Sales Report
# MAGIC
# MAGIC 该 Notebook 从 Google Cloud Storage 获取 Google Play IAP 销售报告。
# MAGIC
# MAGIC - 支持多账号配置
# MAGIC - 从 Google Cloud Storage 下载销售报告
# MAGIC - 自动解压并上传到 S3

# COMMAND ----------

# MAGIC %pip install google-api-python-client oauth2client boto3 pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import gzip
import io
import os
import zipfile
from datetime import datetime, timedelta
from httplib2 import Http

import boto3
import pandas
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.client import SignedJwtAssertionCredentials

import sys

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
_AD_NETWORK = 'google_play'
_AD_TYPE = 'iap'
_REPORT_TYPE = 'sales'

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

def _put_object_to_s3(path: str, data):
    """上传数据到 S3"""
    cfg = helper.get_cfg('aws_s3')
    aws_key = cfg.get('aws_key')
    aws_secret = cfg.get('aws_secret')
    bucket = cfg.get('bucket')
    session = boto3.Session(
        aws_access_key_id=aws_key, 
        aws_secret_access_key=aws_secret,
    )
    s3 = session.resource('s3')
    s3.Bucket(bucket).put_object(Key=path, Body=data)


def _fetch_sales_report(ds: str):
    """
    从 Google Cloud Storage 获取销售报告
    
    Args:
        ds: 执行日期
    
    Returns:
        下载的文件列表
    """
    accounts = helper.get_cfg(_AD_NETWORK)
    filenames = []
    all_feishu_content = []
    
    date = datetime.strptime(ds, '%Y-%m-%d')
    
    # 计算月份
    if date.day > 1:
        mon = date.strftime("%Y%m")
    else:
        mon = (date + timedelta(days=-1)).strftime("%Y%m")
    
    prefix = f'{_REPORT_TYPE}/{_REPORT_TYPE}report_{mon}'
    file_path = os.path.join(helper._DATA_BASE_PATH, _AD_TYPE, _AD_NETWORK, ds)
    
    for account in accounts:
        print(f'\n   🔑 Processing account: {account["name"]}')

        api_key = account["api_key"]
        private_key = api_key["private_key"]
        client_email = api_key["client_email"]
        cloud_storage_bucket = account["bucket_name"]

        # 使用 OAuth2 服务器到服务器身份验证
        auth_url = 'https://www.googleapis.com/auth/devstorage.read_only'
        credentials = SignedJwtAssertionCredentials(client_email, private_key, auth_url)
        storage = build('storage', 'v1', http=credentials.authorize(Http()))

        try:
            bucket_obj = storage.objects().list(
                bucket=cloud_storage_bucket, 
                prefix=prefix
            ).execute()
        except Exception as e:
            print(f'      ⚠️ Get bucket error: {e}')
            continue

        if 'items' not in bucket_obj.keys():
            print(f'      ⚠️ No bucket found for this month')
            continue

        # 检查更新时间
        updated_str = bucket_obj["items"][0]["updated"]
        updated = datetime.strptime(updated_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        current_time = datetime.now()
        interval = current_time - updated

        account_name = account['name']
        
        # 检查时间间隔是否大于2天（仅特定账户）
        if interval.days >= 2 and account_name == 'contact@vertexgames.net':
            send_feishu_content = [
                f"账户：{account_name}\nbucket：{cloud_storage_bucket}\n最后一次更新时间：{updated_str}"
            ]
            all_feishu_content.append('\n\n'.join(send_feishu_content))
        else:
            print(f"      📅 Last updated: {updated_str}")

        report_to_download = bucket_obj['items'][0]['name']
        
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        
        filename = os.path.join(
            file_path, 
            f'{account["name"].split("@")[0]}_{account["name"].split("@")[1]}_{_REPORT_TYPE}_{mon}.zip'
        )
        
        with open(filename, 'wb') as f:
            request = storage.objects().get_media(
                bucket=account["bucket_name"], 
                object=report_to_download
            )
            media = MediaIoBaseDownload(f, request)

            done = False
            while not done:
                try:
                    progress, done = media.next_chunk()
                except:
                    raise RuntimeError('Download failed')
        
        filenames.append(filename)
        print(f'      ✅ Downloaded: {filename}')

    # 发送飞书通知（如果有需要）
    if all_feishu_content:
        helper.send_feishu(
            '8118cef8-e430-40ec-bd87-6df57c5888d7', 
            'Google Play IAP Last Updated', 
            all_feishu_content
        )

    return filenames


def _process_and_upload_to_s3(filenames: list):
    """
    处理并上传报告到 S3
    
    Args:
        filenames: 下载的文件列表
    """
    if not filenames:
        print("   ⚠️ No files to process")
        return
        
    for filename in filenames:
        print(f"\n   📦 Processing: {os.path.basename(filename)}")
        
        fz = zipfile.ZipFile(filename, 'r')
        file_dir, name = os.path.split(filename)
        
        for file in fz.namelist():
            fz.extract(file, file_dir)
            path = os.path.join(file_dir, file)
            
            # 创建压缩输出临时文件
            temp_gz_path = path + '.gz'
            with gzip.open(temp_gz_path, 'wt', encoding='utf-8') as gz_file:
                # 分块处理 CSV 并直接写入 gzip 文件
                chunk_size = 100000
                for chunk in pandas.read_csv(
                    path,
                    quotechar='"',
                    escapechar='\\',
                    low_memory=False,
                    chunksize=chunk_size
                ):
                    chunk.to_json(gz_file, orient='records', lines=True)
                    gz_file.write('\n')
            
            # 上传压缩文件到 S3
            with open(temp_gz_path, 'rb') as f:
                upload_path = os.path.join(
                    file_dir.replace(helper._DATA_BASE_PATH, "reports"), 
                    name.replace(".zip", ".gz")
                )
                _put_object_to_s3(upload_path, f.read())
                print(f"      ✅ Uploaded to S3: {upload_path}")
            
            # 清理临时文件
            os.remove(temp_gz_path)
            os.remove(path)


def fetch_sales_report_task(ds: str):
    """
    获取 Google Play 销售报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} {_REPORT_TYPE} report for {ds}")
    
    # Step 1: 下载报告
    print("\n   📥 Downloading reports from Google Cloud Storage...")
    filenames = _fetch_sales_report(ds)
    print(f"\n   📦 Downloaded {len(filenames)} file(s)")
    
    # Step 2: 处理并上传
    print("\n   📤 Processing and uploading to S3...")
    _process_and_upload_to_s3(filenames)
    
    print(f"\n✅ Completed {_AD_NETWORK} {_REPORT_TYPE} report for {ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK} {_REPORT_TYPE}")

try:
    fetch_sales_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_{_REPORT_TYPE}_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
