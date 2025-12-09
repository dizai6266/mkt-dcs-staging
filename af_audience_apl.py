# Databricks notebook source
# MAGIC %md
# MAGIC # AppsFlyer Audience APL
# MAGIC
# MAGIC 该 Notebook 用于上传 AppsFlyer 自定义受众数据（APL版本）。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import time
from datetime import datetime, timedelta
import logging
import requests
import json
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

# 设置 feishu-notify（路径已在 config_manager 中配置）
Notifier = setup_feishu_notify()

from databricks import sql

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_TASK_NAME = 'af_audience_apl'

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

def do_af_audience_process_apl(**context):
    """
    处理 AF 受众上传任务（APL版本）
    
    Args:
        **context: 上下文参数（兼容 Airflow）
    """
    secret_conf = helper.get_cfg('af_audience_apl')
    audience_infos = secret_conf.get('audience_infos')

    db_conn_conf = secret_conf['db_conn_conf']
    conn = sql.connect(
        server_hostname=db_conn_conf.get('server_hostname'),
        http_path=db_conn_conf.get('http_path'),
        access_token=db_conn_conf.get('access_token')
    )

    for target_audience, audience_detail_info in audience_infos.items():
        api_token = audience_detail_info.get('api_token')
        items = audience_detail_info.get('items')
        is_valid = audience_detail_info.get('is_valid')
        if not is_valid:
            continue

        for item in items:
            audience_name, audience_id = item.get('audience_name'), item.get('audience_id')
            app_name, platform, app_id = item.get('app_name'), item.get('platform'), item.get('app_id')
            import_key, import_model, mock_identifiers = item.get('import_key'), item.get('import_model'), item.get('mock_identifiers')
            sql_text = item.get('sql_text')
            if import_model not in ['overwrite', 'add']:
                raise ValueError(f'Invalid import model: {import_model}')

            # 上传受众成员 
            if target_audience == 'af_audience':

                logging.info(f'**********> start to upload af audience: {audience_name} {audience_id}')

                if import_model == 'overwrite':
                    # 先使用 mock gaid 进行覆盖
                    url = f"https://hq1.appsflyer.com/api/audiences-import-api/v2/{import_model}"
                    headers = {
                        "authorization": f"Bearer {api_token}"
                    }
                    data = {
                        "import_key": import_key,
                        "platform": platform.lower(),
                        "devices": mock_identifiers
                    }
                    response = requests.post(url, headers=headers, json=data)
                    print(response.text) 
                    time.sleep(30.)

                cursor = conn.cursor()
                cursor.execute(sql_text)
                sql_result = cursor.fetchall()
                cursor.close()

                identities = list()
                for item in sql_result:
                    identities.append(item[0].strip() if item[0] else '')
                logging.info(f'number of devices: {len(identities)}')

                devices = list()
                if platform == 'Android':
                    devices = [{"gaid": gaid, "app_id": app_id} for gaid in identities]
                elif platform == 'iOS':
                    devices = [{"idfv": idfv, "app_id": app_id} for idfv in identities]
                else:
                    raise ValueError(f'Invalid platform: {platform}')

                url = f"https://hq1.appsflyer.com/api/audiences-import-api/v2/add"
                headers = {
                    "authorization": f"Bearer {api_token}"
                }
                curstep, stepnum = 0, 10000
                while curstep <= len(devices):
                    data = {
                        "import_key": import_key,
                        "platform": platform.lower(),
                        "devices": devices[curstep: curstep + stepnum]
                    }
                    response = requests.post(url, headers=headers, json=data)
                    print(response.text) 
                    curstep += stepnum
                    time.sleep(.5)

    conn.close()


def upload_af_audience_apl_task(ds: str):
    """
    上传 AF 受众任务主函数（APL版本）
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📅 Processing AF Audience APL for {ds}")
    
    do_af_audience_process_apl()
    
    print(f"✅ AF Audience APL completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_TASK_NAME}")

try:
    upload_af_audience_apl_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_TASK_NAME}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Validation

# COMMAND ----------

env_mode = get_env_mode()
print(f"\n🔍 Data Validation (ENV_MODE={env_mode})")

if env_mode != 'staging':
    print("⚠️ 非 staging 模式，跳过数据验证。")
else:
    print("✅ AF Audience APL 任务执行完成，请检查上传结果。")
