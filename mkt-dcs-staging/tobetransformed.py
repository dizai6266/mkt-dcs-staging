# -*- coding:utf-8 -*-

import math, random
import time
from datetime import datetime,timedelta
import logging
import requests
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sqlalchemy as sa
from databricks import sql
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.api import FacebookAdsApi

from dags.utils import helper

default_args = {
    'owner': 'joycastle',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'email': ['notify@joycastle.mobi'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': helper.failure_callback
}

""" **************************** Facebook Audience 上传 **************************** """
def do_facebook_audience_process(**context):
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


def do_facebook_audience_weekly_process(**context):
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
 

with DAG(
        f'facebook_audience', catchup=False,
        default_args=default_args, schedule_interval='30 3 * * *',
) as mkt_facebook_audience_dag:
    task = PythonOperator(
        task_id=f'upload_task',
        python_callable=do_facebook_audience_process,
        provide_context=True,
    )
    task


with DAG(
        f'facebook_audience_weekly', catchup=False,
        default_args=default_args, schedule_interval='30 3 * * 1',
) as mkt_facebook_audience_weekly_dag:
    task = PythonOperator(
        task_id=f'upload_task',
        python_callable=do_facebook_audience_weekly_process,
        provide_context=True,
    )
    task


""" **************************** Aarki Audience 上传 **************************** """
def do_aarki_audience_process(**context):

    def gen_ran_string(num):
        ran = ''
        for _ in range(num):
            ran += str(int(random.random() * 10)) 
        return ran

    secret_conf = helper.get_cfg('aarki_audience')
    items = secret_conf.get('items')
    db_conn_conf = secret_conf['db_conn_conf']
 
    conn = sql.connect(
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


with DAG(
        f'aarki_audience', catchup=False,
        default_args=default_args, schedule_interval='30 3 * * *',
) as mkt_aarki_audience_dag:
    task = PythonOperator(
        task_id=f'upload_task',
        python_callable=do_aarki_audience_process,
        provide_context=True,
    )
    task


""" **************************** AF(或其他类似规则渠道) Audience 上传 **************************** """
def do_af_audience_process(**context):

    secret_conf = helper.get_cfg('af_audience')
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
            import_key = item.get('import_key')
            options = item.get('options')

            # 获取设备信息
            country, days, pay = options.get('country'), options.get('days'), options.get('pay')
            country_sql_str = "'" + "','".join(country) + "'" 
            country_info = f"country_code in ({country_sql_str})" if country != ['Others'] else "country_code not in ('UK','GB','DE','CA','AU','NL','US')"
            sql_text = f"""
                WITH aaa AS (
                    SELECT DISTINCT app_id, appsflyer_id
                    FROM appsflyer.player a
                    WHERE app_name = '{app_name}'
                        AND platform = '{platform}'
                        AND {country_info}
                        AND first_iap_time IS NOT NULL
                        AND nvl(total_iap, 0) > {pay[0]} and nvl(total_iap, 0) <= {pay[1]}
                        AND is_tester = false and install_time >= '2022-01-01'
                ),
                bbb AS (
                    SELECT a.app_id, a.appsflyer_id, MAX(busi_date) AS busi_date_max
                    FROM aaa a
                    JOIN appsflyer.dws_daily_player_behavior b
                        ON a.appsflyer_id = b.appsflyer_id AND a.app_id = b.app_id
                    GROUP BY 1, 2
                    HAVING busi_date_max >= current_date - {days[1]} and busi_date_max < current_date - {days[0]}
                )
                SELECT DISTINCT advertising_id
                FROM bbb a
                JOIN appsflyer.player b
                    ON a.appsflyer_id = b.appsflyer_id AND a.app_id = b.app_id AND NVL(b.advertising_id, '') <> ''
                ;
            """

            logging.info(f'**********> start to fecth devices for audience: {options}')

            cursor = conn.cursor()
            cursor.execute(sql_text)
            sql_result = cursor.fetchall()
            cursor.close()

            identities = list()
            for item in sql_result:
                identities.append(item[0].strip() if item[0] else '')

            logging.info(f'number of devices: {len(identities)}')

            # 上传受众成员 
            if target_audience == 'af_audience':

                logging.info(f'**********> start to upload af audience: {audience_name} {audience_id}')

                devices = [{"gaid": gaid, "app_id": app_id} for gaid in identities]
                url = f"https://hq1.appsflyer.com/api/audiences-import-api/v2/overwrite"
                headers = {
                    "authorization": f"Bearer {api_token}"
                }
                curstep, stepnum = 0, 20000
                while curstep <= len(devices):
                    data = {
                        "import_key": import_key,
                        "platform": platform.lower()              ,
                        "devices": devices[curstep: curstep + stepnum]
                    }
                    response = requests.post(url, headers=headers, json=data)
                    print(response.text) 
                    curstep += stepnum
                    time.sleep(.5)

            # if target_audience == 'bigabid_audience':

            #     logging.info(f'**********> start to upload bigabid audience: {audience_name} {audience_id}')

            #     devices = [gaid for gaid in identities]
            #     url = f"https://paudience.bigabidserv.com/{api_token}/override_users?audience_id={audience_id}"
            #     headers = {
            #         'Content-Type': 'application/json'
            #     }
            #     curstep, stepnum = 0, 20000
            #     while curstep <= len(devices):
            #         payload = json.dumps({
            #             "idfas": devices[curstep: curstep + stepnum]
            #         })
            #         response = requests.post(url, headers=headers, data=payload)
            #         print(response.text) 
            #         curstep += stepnum
            #         time.sleep(2.)

    conn.close()


def do_af_audience_process_2(**context):

    secret_conf = helper.get_cfg('af_audience_2')
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
            import_key = item.get('import_key')
            options = item.get('options')

            # 获取设备信息
            country, days, pay = options.get('country'), options.get('days'), options.get('pay')
            country_sql_str = "'" + "','".join(country) + "'" 
            country_info = f"country_code in ({country_sql_str})" if country != ['Others'] else "country_code not in ('UK','GB','DE','CA','AU','NL','US')"
            event_archived = options.get('event_archived')
            sql_text = f"""
                WITH aaa AS (
                    SELECT DISTINCT app_id, appsflyer_id
                    FROM appsflyer.player a
                    WHERE app_name = '{app_name}'
                        AND platform = '{platform}'
                        AND {country_info}
                        AND nvl(total_iap, 0) > {pay[0]} and nvl(total_iap, 0) <= {pay[1]}
                        AND is_tester = false and install_time >= '2023-01-01'
                ),
                event_player AS (
                    SELECT DISTINCT a.app_id, a.appsflyer_id
                    FROM aaa a
                    join appsflyer.ods_events b
                        on a.appsflyer_id = b.appsflyer_id and a.app_id = b.app_id
                    WHERE b.event_name = '{event_archived}'
                        and event_time >= '2023-01-01'
                ),
                bbb AS (
                    SELECT a.app_id, a.appsflyer_id, MAX(busi_date) AS busi_date_max
                    FROM aaa a
                    join event_player b
                        on a.appsflyer_id = b.appsflyer_id and a.app_id = b.app_id
                    JOIN appsflyer.dws_daily_player_behavior c
                        ON a.appsflyer_id = c.appsflyer_id AND a.app_id = c.app_id
                    GROUP BY 1, 2
                    HAVING busi_date_max >= current_date - {days[1]} and busi_date_max < current_date - {days[0]}
                )
                SELECT DISTINCT advertising_id
                FROM bbb a
                JOIN appsflyer.player b
                    ON a.appsflyer_id = b.appsflyer_id AND a.app_id = b.app_id AND NVL(b.advertising_id, '') <> ''
                ;
            """

            logging.info(f'**********> start to fecth devices for audience: {options}')

            cursor = conn.cursor()
            cursor.execute(sql_text)
            sql_result = cursor.fetchall()
            cursor.close()

            identities = list()
            for item in sql_result:
                identities.append(item[0].strip() if item[0] else '')

            logging.info(f'number of devices: {len(identities)}')

            # 上传受众成员 
            if target_audience == 'af_audience':

                logging.info(f'**********> start to upload af audience: {audience_name} {audience_id}')

                devices = [{"gaid": gaid, "app_id": app_id} for gaid in identities]
                url = f"https://hq1.appsflyer.com/api/audiences-import-api/v2/add"
                headers = {
                    "authorization": f"Bearer {api_token}"
                }
                curstep, stepnum = 0, 10000
                while curstep <= len(devices):
                    data = {
                        "import_key": import_key,
                        "platform": platform.lower()              ,
                        "devices": devices[curstep: curstep + stepnum]
                    }
                    response = requests.post(url, headers=headers, json=data)
                    print(response.text) 
                    curstep += stepnum
                    time.sleep(.5)

    conn.close()


def do_af_audience_process_apl(**context):

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


with DAG(
        f'af_audience', catchup=False,
        default_args=default_args, schedule_interval='30 3 * * *',
) as mkt_af_audience_dag:
    task = PythonOperator(
        task_id=f'upload_task',
        python_callable=do_af_audience_process,
        provide_context=True,
    )
    task


with DAG(
        f'af_audience_2', catchup=False,
        default_args=default_args, schedule_interval='30 3 * * *',
) as mkt_af_audience_dag_2:
    task = PythonOperator(
        task_id=f'upload_task',
        python_callable=do_af_audience_process_2,
        provide_context=True,
    )
    task


with DAG(
        f'af_audience_apl', catchup=False,
        default_args=default_args, schedule_interval='30 3 * * *',
) as mkt_af_audience_dag_apl:
    task = PythonOperator(
        task_id=f'upload_task',
        python_callable=do_af_audience_process_apl,
        provide_context=True,
    )
    task