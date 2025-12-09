# Databricks notebook source
# MAGIC %md
# MAGIC # Amazon IAP Report
# MAGIC
# MAGIC 该 Notebook 从 Amazon API 获取 IAP 销售报告数据。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import gzip
import io
import os
import shutil
import zipfile
from datetime import datetime, timedelta
import sys
import pandas as pd
import requests
from typing_extensions import TypedDict

# 动态添加当前目录到 sys.path 以加载 utils
current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

from utils import helper
from utils.config_manager import get_env_mode, setup_feishu_notify
import importlib
importlib.reload(helper)

# 设置 feishu-notify(路径已在 config_manager 中配置)
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'amazon'
_AD_TYPE = 'iap'

# --- [日期参数] ---
try:
    dbutils.widgets.text("ds", "", "Date (YYYY-MM-DD)")
    ds_param = dbutils.widgets.get("ds")
except:
    ds_param = ""

if not ds_param:
    ds_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"📅 Execution Date: {ds_param}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core Functions

# COMMAND ----------

class AmazonAccessToken(TypedDict):
    access_token: str
    scope: str
    token_type: str
    expires_in: int


def _get_access_token(client_id, client_secret) -> AmazonAccessToken:
    resp = requests.post(
        'https://api.amazon.com/auth/o2/token',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "adx_reporting::appstore:marketer"
        }
    )
    result = resp.json()
    print(f"   🔑 Access token received")
    if result.get('error'):
        raise RuntimeError(result.get('error_description'))
    return result


def _get_sale_report_url(amz_token: AmazonAccessToken, year: int, month: int):
    resp = requests.get(
        f'https://developer.amazon.com/api/appstore/download/report/sales/{year}/{month}',
        headers={'Authorization': f'{amz_token.get("token_type")} {amz_token.get("access_token")}'}
    )
    return resp.text


def _download_report(url: str, ds: str, client_id: str):
    dir_path = os.path.join(helper._DATA_BASE_PATH, _AD_TYPE, _AD_NETWORK, ds)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    filename = client_id + '-' + url.split('/')[-1].split('?')[0]
    full_path = os.path.join(dir_path, filename)
    with requests.get(url, stream=True) as r:
        with open(full_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    print(f"   📥 Downloaded: {filename}")
    return full_path


def _process_and_upload(file_path, year, month, ds):
    sio = io.StringIO()
    
    with zipfile.ZipFile(file_path, 'r') as zf:
        filename = zf.namelist()[0]
        report = zf.read(filename).decode('utf-8').replace('/', '-')
        df = pd.read_csv(io.StringIO(report))
        
        # 删除不在请求月份内的数据
        fix_month = f'{month}' if month >= 10 else f'0{month}'
        df = df.drop(df[df['Transaction Time'] < f'{year}-{fix_month}-01'].index)
        df.to_json(sio, orient='records', lines=True)
    
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=sio.getvalue(),
        exc_ds=ds,
        start_ds=f'{year}-{fix_month}-01',
        end_ds=f'{year}-{fix_month}-28'
    )


def fetch_iap_report_task(ds: str):
    print(f"📊 Fetching {_AD_NETWORK} IAP report for {ds}")
    
    cfg = helper.get_cfg('amazon')
    iap_clients = cfg.get('iap')
    
    for client in iap_clients:
        client_id = client.get('client_id')
        client_secret = client.get('client_secret')
        print(f"\n   📱 Processing client: {client_id[:8]}...")
        
        amz_token = _get_access_token(client_id, client_secret)
        
        curr_dt = datetime.strptime(ds, '%Y-%m-%d')
        last_month_dt = curr_dt.replace(day=1) + timedelta(days=-1)
        
        for t in [curr_dt, last_month_dt]:
            year, month = t.year, t.month
            print(f"   📅 Fetching {year}-{month:02d}")
            
            sale_url = _get_sale_report_url(amz_token, int(year), int(month))
            
            if not (sale_url.startswith("https://") or sale_url.startswith("http://")):
                raise Exception(f'no valid sale_url found: {sale_url}, {year}/{month}')
            
            sale_path = _download_report(sale_url, ds, client_id)
            _process_and_upload(sale_path, year, month, ds)
            print(f"   ✅ Processed {year}-{month:02d}")
    
    print(f"\n✅ Saved {_AD_NETWORK} report for {ds}")

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
    helper.failure_callback(str(e), f"{_AD_NETWORK}_iap_report")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

if env_mode != 'staging':
    print("⚠️ 非 staging 模式,跳过本地 preview。")
else:
    try:
        base_root = getattr(helper, "_DATA_BASE_PATH", None) or os.path.join(os.getcwd(), "data_output")
        preview_root = os.path.join(base_root, _AD_TYPE, _AD_NETWORK)
        print(f"🔎 Scanning preview files under: {preview_root}")

        if os.path.exists(preview_root):
            preview_files = [
                os.path.join(preview_root, f)
                for f in os.listdir(preview_root)
                if f.endswith('.json')
            ][:3]
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