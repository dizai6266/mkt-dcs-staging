# Databricks notebook source
# MAGIC %md
# MAGIC # Facebook Audience
# MAGIC
# MAGIC 该 Notebook 用于上传 Facebook 自定义受众数据。

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
from utils.config_manager import get_env_mode
import importlib
importlib.reload(helper)

# 添加 feishu-notify 路径（根据环境自动切换）
_feishu_notify_path = '/Workspace/Repos/Shared/feishu-notify' if get_env_mode() == 'prod' else '/Workspace/Users/dizai@joycastle.mobi/feishu-notify'
sys.path.append(_feishu_notify_path)
from notifier import Notifier

import sqlalchemy as sa
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
_TASK_NAME = 'facebook_audience'

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

def do_facebook_audience_process(**context):
    """
    处理 Facebook 受众上传任务
    
    Args:
        **context: 上下文参数（兼容 Airflow）
    """
    secret_conf = helper.get_cfg('facebook_audience')
    access_token, audience_info = secret_conf.get('access_token'), secret_conf.get('audience_info', [])
    db_conn_conf = secret_conf['db_conn_conf']
 
    FacebookAdsApi.init(access_token=access_token)

    engine = sa.create_engine(db_conn_conf, echo=False)

    for item in audience_info:
        sql, audience_ids = item.get('sql'), item.get('audience_ids').split(',')

        with engine.connect() as conn:
            sql_text = sql
            res = conn.execute(sa.text(sql_text))
            sql_result = res.fetchall()

        audience_data = list()
        for item in sql_result:
            audience_data.append([item[0].strip() if item[0] else '', item[1].strip() if item[1] else ''])
        print('本次待上传 audience 数量：', len(audience_data))

        # 上传受众成员
        for audience_id in audience_ids:
            """ 如何追加用户 users 就是追加    
            """
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
                        CustomAudience.Schema.MultiKeySchema.email, CustomAudience.Schema.MultiKeySchema.madid
                    ],
                    users=batch_data,
                    is_raw=True,
                    app_ids=None,
                    pre_hashed=None,
                    session=session
                )
                print(res.json())
                time.sleep(1.)


def upload_facebook_audience_task(ds: str):
    """
    上传 Facebook 受众任务主函数
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📅 Processing Facebook Audience for {ds}")
    
    do_facebook_audience_process()
    
    print(f"✅ Facebook Audience completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_TASK_NAME}")

try:
    upload_facebook_audience_task(ds_param)
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
    print("✅ Facebook Audience 任务执行完成，请检查上传结果。")
