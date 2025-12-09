# Databricks notebook source
# MAGIC %md
# MAGIC # Aarki Audience
# MAGIC
# MAGIC 该 Notebook 用于上传 Aarki 自定义受众数据。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import math
import random
import time
from datetime import datetime, timedelta
import logging
import requests
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

from databricks.sql import connect as databricks_connect

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_TASK_NAME = 'aarki_audience'

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

def gen_ran_string(num):
    """生成随机字符串"""
    ran = ''
    for _ in range(num):
        ran += str(int(random.random() * 10)) 
    return ran


def do_aarki_audience_process(**context):
    """
    处理 Aarki 受众上传任务
    
    Args:
        **context: 上下文参数（兼容 Airflow）
    """
    secret_conf = helper.get_cfg('aarki_audience')
    items = secret_conf.get('items')
    db_conn_conf = secret_conf['db_conn_conf']
 
    conn = databricks_connect(
        server_hostname=db_conn_conf.get('server_hostname'),
        http_path=db_conn_conf.get('http_path'),
        access_token=db_conn_conf.get('access_token')
    )

    # 上传受众成员
    for _item in items:

        api_key, audience_infos = _item.get('api_key'), _item.get('audience_infos')

        for item in audience_infos:

            audience_id, audience_name, sql_text = item.get('audience_id'), item.get('audience_name'), item.get('sql_text')

            logging.info(f'**********> start to fecth audience: {audience_id} {audience_name}')

            cursor = conn.cursor()
            cursor.execute(sql_text)
            sql_result = cursor.fetchall()
            cursor.close()

            identities = list()
            for item in sql_result:
                identities.append(item[0].strip() if item[0] else '')

            data = {
                "id": '',
                "timestamp_ms": int(time.time() * 1000),
                "api_key": api_key,
                "audience_id": audience_id,
                "audience_name": audience_name,
                "action": "add",
            }
            nums = math.ceil((len(identities) / 10000))
            for i in range(nums):
                logging.info(f'start to upload {audience_name} membership, idx: {i} all {nums}')
                ran = gen_ran_string(4)
                if 'ios' in audience_name:
                    data['identities'] = {
                        "idfa": identities[i*10000: (i+1)* 10000]
                    }
                    data['id'] = f'd9999b4e-{ran}-b4db-b603281ec436'
                else:
                    data['identities'] = {
                        "gaid": identities[i*10000: (i+1)* 10000]
                    }
                    data['id'] = f'd9999b4e-{ran}-b4db-b603281ec436'

                url = 'http://audiences.aarki.net/membership'
                headers = {
                    'Content-Type': 'application/json'
                }
                response = requests.post(url, json=data, headers=headers)

                logging.info(response.status_code)
                logging.info(response.json())

                time.sleep(1.)

    conn.close()


def upload_aarki_audience_task(ds: str):
    """
    上传 Aarki 受众任务主函数
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📅 Processing Aarki Audience for {ds}")
    
    do_aarki_audience_process()
    
    print(f"✅ Aarki Audience completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_TASK_NAME}")

try:
    upload_aarki_audience_task(ds_param)
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
    print("✅ Aarki Audience 任务执行完成，请检查上传结果。")
