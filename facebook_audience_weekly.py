# Databricks notebook source
# MAGIC %md
# MAGIC # Facebook Audience Weekly
# MAGIC
# MAGIC 该 Notebook 用于每周上传 Facebook 自定义受众数据。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import math
import time
from datetime import datetime, timedelta
import logging
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
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.api import FacebookAdsApi

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_TASK_NAME = 'facebook_audience_weekly'

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

def do_facebook_audience_weekly_process(**context):
    """
    处理 Facebook 受众每周上传任务
    
    Args:
        **context: 上下文参数（兼容 Airflow）
    """
    secret_conf = helper.get_cfg('facebook_audience_weekly')
    access_token, audience_info = secret_conf.get('access_token'), secret_conf.get('audience_info', [])
    db_conn_conf = secret_conf['db_conn_conf']
 
    FacebookAdsApi.init(access_token=access_token)

    conn = sql.connect(
        server_hostname=db_conn_conf.get('server_hostname'),
        http_path=db_conn_conf.get('http_path'),
        access_token=db_conn_conf.get('access_token')
    )

    for item in audience_info:
        sql_text, audience_ids = item.get('sql_text'), item.get('audience_ids').split(',')
        import_model, mock_identifiers = item.get('import_model'), item.get('mock_identifiers')
        if import_model not in ['overwrite', 'add']:
            raise ValueError(f'Invalid import model: {import_model}')

        for audience_id in audience_ids:
            # 首先覆盖受众成员
            if import_model == 'overwrite':
                audience = CustomAudience(audience_id)
                session_id = int(time.time())
                session={
                    "session_id": session_id, # 设置时间戳相关的64位数值
                    "batch_seq": 0,
                    "last_batch_flag": True, # 是否最后一批
                    "estimated_num_total": 1
                }
                params = audience.format_params(
                    schema = [
                        CustomAudience.Schema.MultiKeySchema.madid
                    ],
                    users=mock_identifiers,
                    is_raw=True,
                    app_ids=None,
                    pre_hashed=None,
                    session=session
                )
                res = audience.create_users_replace(
                    params=params,
                    batch=None,
                    pending=True
                )
                time.sleep(60.)

            with conn.cursor() as cursor:
                cursor.execute(sql_text)
                sql_result = cursor.fetchall()

            audience_data = list()
            for item in sql_result:
                audience_data.append([item[0].strip() if item[0] else ''])
            print('本次待上传 audience 数量：', len(audience_data))

            # 追加用户 users
            print(f'audience: {audience_id} 开始上传...')

            batch_num = 10000
            estimated_num_total = math.ceil(len(audience_data) / batch_num) * batch_num
            batchs = math.ceil(len(audience_data) / batch_num)
            session_id = int(time.time())

            for i in range(batchs):
                session={
                    "session_id": session_id, # 设置时间戳相关的64位数值
                    "batch_seq": i+1,
                    "last_batch_flag": True if i == batchs-1 else False, # 是否最后一批
                    "estimated_num_total": estimated_num_total
                }
                batch_data = audience_data[i*batch_num: (i+1)*batch_num]
                audience = CustomAudience(audience_id)
                res = audience.add_users(
                    schema = [
                        CustomAudience.Schema.MultiKeySchema.madid
                    ],
                    users=batch_data,
                    is_raw=True,
                    app_ids=None,
                    pre_hashed=None,
                    session=session
                )
                print(res.json())
                time.sleep(1.)
    
    conn.close()


def upload_facebook_audience_weekly_task(ds: str):
    """
    上传 Facebook 受众每周任务主函数
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📅 Processing Facebook Audience Weekly for {ds}")
    
    do_facebook_audience_weekly_process()
    
    print(f"✅ Facebook Audience Weekly completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_TASK_NAME}")

try:
    upload_facebook_audience_weekly_task(ds_param)
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
    print("✅ Facebook Audience Weekly 任务执行完成，请检查上传结果。")
