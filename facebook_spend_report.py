# Databricks notebook source
# MAGIC %md
# MAGIC # Facebook Spend Report
# MAGIC
# MAGIC 该 Notebook 从 Facebook Marketing API 获取广告消耗数据。
# MAGIC
# MAGIC - 使用异步 Job 获取 Insights
# MAGIC - 多线程并行处理
# MAGIC - 支持多账户

# COMMAND ----------

# MAGIC %pip install facebook-business pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from facebook_business.adobjects.adaccountuser import AdAccountUser as AdUser
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

# 设置 feishu-notify
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'facebook'
_AD_TYPE = 'spend'
_DATE_RANGE = 3

# 语言映射表（中文/越南文 -> 英文）
LAN_MAP = {
    # 处理中文表头
    '报告开始日期': 'Reporting Starts', '报告结束日期': 'Reporting Ends', 
    '国家/地区': 'Country', '货币': 'Currency',
    '帐户编号': 'Account ID', '帐户名称': 'Account Name', 
    '广告编号': 'Ad ID', '广告名称': 'Ad Name', '广告组编号': 'Ad Set ID', 
    '广告组名称': 'Ad Set Name', '广告系列编号': 'Campaign ID', 
    '广告系列名称': 'Campaign Name', '地区（广告组设置）': 'Location (Ad Set Settings)', 
    '移动应用安装': 'Mobile App Installs', '应用安装': 'App Installs', 
    '展示次数': 'Impressions', '点击量（全部）': 'Clicks (All)', 
    '"花费金额 (USD)"': 'Amount Spent (USD)', '花费金额 (USD)': 'Amount Spent (USD)', 
    '平台': 'platform',
    '\"视频播放进度达 25% 的次数\"': 'video plays at 25%"', 
    '\"视频播放进度达 50% 的次数\"': 'video plays at 50%"', 
    '\"视频播放进度达 75% 的次数\"': 'video plays at 75%"', 
    '\"视频播放进度达 95% 的次数\"': 'video plays at 95%"', 
    '\"视频播放进度达 100% 的次数\"': 'video plays at 100%"',
    '视频播放量': 'video plays', '视频平均播放时长': 'video average play time',
    # 处理越南文表头
    '"Bắt đầu báo cáo"': 'Reporting Starts', '"Kết thúc báo cáo"': 'Reporting Ends', 
    '"Quốc gia"': 'Country', '"Đơn vị tiền tệ"': 'Currency',
    '"ID tài khoản"': 'Account ID', '"Tên tài khoản"': 'Account Name', 
    '"Mã quảng cáo"': 'Ad ID', '"Tên quảng cáo"': 'Ad Name', 
    '"ID nhóm quảng cáo"': 'Ad Set ID', '"Tên nhóm quảng cáo"': 'Ad Set Name', 
    '"ID chiến dịch"': 'Campaign ID', '"Tên chiến dịch"': 'Campaign Name', 
    '"Lượt cài đặt ứng dụng di động"': 'Mobile App Installs', 
    '"Lượt cài đặt ứng dụng"': 'App Installs', 
    '"Lượt hiển thị"': 'Impressions', '"Số lần nhấp (Tất cả)"': 'Clicks (All)', 
    '"Số tiền đã chi tiêu (USD)"': 'Amount Spent (USD)',
}

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

def _get_insights(account_info, start_ds, ds, cfg):
    """
    获取单个账户的 Insights 异步任务
    
    Args:
        account_info: (account, category) 元组
        start_ds: 开始日期
        ds: 结束日期
        cfg: 配置信息
        
    Returns:
        (category, report_run_id, account_id, start_ds, ds) 或失败时 report_run_id=-1
    """
    account, category = account_info[0], account_info[1]
    account_id = account['account_id']
    print(f'   📡 Getting insights for account: {account_id}')
    
    report_info = None
    fields = [
        'account_currency', 'account_id', 'account_name', 'ad_id', 'ad_name', 
        'adset_id', 'adset_name', 'campaign_id', 'campaign_name', 'impressions',
        'clicks', 'spend', 'cpc', 'cpm', 'ctr', 'actions',
        'cost_per_ad_click', 'cost_per_conversion',
    ]
    breakdowns = ['country']
    
    params = {
        'level': 'ad',
        'breakdowns': breakdowns,
        'time_range': {'since': start_ds, 'until': ds},
        'time_increment': 1,
        'fields': fields,
    }
    
    max_retries = 2
    for retry in range(max_retries):
        try:
            async_job = account.get_insights(params=params, is_async=True)
            async_job.api_get()
            
            # 等待异步任务完成
            max_wait = 24
            wait_count = 0
            while async_job['async_percent_completion'] < 100:
                if async_job['async_status'] not in ['Job Running', 'Job Not Started', 'Job Started']:
                    raise RuntimeError(f"{account_id} failed, status: {async_job['async_status']}")
                wait_count += 1
                if wait_count > max_wait:
                    raise RuntimeError(f"{account_id} timeout, status: {async_job['async_status']}")
                sleep(5)
                async_job.api_get()
            
            sleep(1)
            async_job.api_get()
            report_run_id = async_job['report_run_id']
            report_info = (category, report_run_id, account_id, start_ds, ds)
            print(f'   ✅ Got insights for account: {account_id}')
            break
            
        except Exception as e:
            print(f"   ⚠️ Retry {retry + 1} for account {account_id}: {e}")
            sleep((retry + 1) * 2.5)
    
    if report_info is None:
        report_info = (category, -1, account_id, start_ds, ds)
    
    return report_info


def _get_insights_wrapper(task_args):
    """多线程调用包装函数"""
    account_info, start_ds, ds, cfg = task_args
    return _get_insights(account_info, start_ds, ds, cfg)


def _fetch_export_report(report_id, category, params, cfg):
    """
    下载导出报告
    
    Args:
        report_id: 报告运行 ID
        category: 类别（可为 None）
        params: 包含 account, exec_ds, start_ds, ds 的字典
        cfg: 配置信息
        
    Returns:
        保存的文件路径
    """
    account = params.get("account")
    exec_ds = params.get('exec_ds')
    start_ds = params.get("start_ds")
    ds = params.get("ds")
    
    url = 'https://www.facebook.com/ads/ads_insights/export_report/'
    report_params = {
        'report_run_id': report_id,
        'format': 'csv',
        'access_token': cfg.get('market')
    }
    
    for retry in range(3):
        try:
            resp = requests.get(url, params=report_params)
            if resp.status_code not in [200, 204]:
                raise RuntimeError(f'Failed to download report: {resp.text}')
            
            report_str = resp.text
            if '"<!doctype html>' in report_str:
                raise RuntimeError(f'{params} fetch report failed, got HTML response')
            
            # 替换表头语言
            first_line = report_str.split('\n')[0]
            dimensions = first_line.split(',')
            new_dimensions = [LAN_MAP.get(item, item) for item in dimensions]
            new_first_line = ",".join(new_dimensions)
            report_str = report_str.replace(first_line, new_first_line, 1)
            
            # 保存报告
            return helper.save_report(
                ad_network=f'{_AD_NETWORK}_{category}' if category else _AD_NETWORK,
                ad_type=_AD_TYPE,
                report=report_str,
                exc_ds=exec_ds,
                start_ds=start_ds,
                end_ds=ds,
                custom=account
            )
            
        except Exception as e:
            print(f"   ⚠️ Retry {retry + 1} downloading report: {e}")
            sleep(2)
    
    # 所有重试失败
    helper.sql_error_bot(
        title=f'Facebook spend report {account}',
        text=f'Facebook failed get report\n\n {category} {account} {report_id}'
    )
    raise RuntimeError(f'{category} {account} {report_id} Failed to get the report')


def fetch_spend_report_task(ds: str):
    """
    获取 Facebook Spend 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} spend report for {ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    start_dt = end_dt + timedelta(days=-_DATE_RANGE)
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {ds}")
    
    # 初始化 Facebook API
    market = cfg.get('market')
    FacebookAdsApi.init(access_token=market)
    
    # 获取账户列表
    fields = ['id', 'account_id', 'account_status', 'age', 'amount_spent', 'name', 
              'business', 'business_name', 'disable_reason']
    me = AdUser(fbid='me')
    
    account_list = None
    for retry in range(3):
        try:
            account_list = list(me.get_ad_accounts(fields=fields))
            if account_list:
                break
        except Exception as e:
            print(f'   ⚠️ Retry {retry + 1} getting ad accounts: {e}')
            sleep(20)
    
    if not account_list:
        raise RuntimeError('Failed to get ad accounts')
    
    # 添加额外账户
    for account_id in cfg.get('accounts_not_obtained', []):
        extra_account = AdAccount(account_id)
        extra_account.api_get(fields=fields)
        account_list.append(extra_account)
    
    print(f"   📱 Found {len(account_list)} account(s)")
    
    # 构建任务列表
    tasks = []
    split_accounts = cfg.get('market_split_date_accounts', '').strip(',').split(',')
    miss_accounts = cfg.get('market_miss_accounts', '').strip(',').split(',')
    category = None  # 普通 spend report 不分 category
    
    for account in account_list:
        if account["account_id"] in miss_accounts:
            continue
        if account["account_status"] != 1:
            continue
        
        is_split_account = False
        for split_info in split_accounts:
            if not split_info:
                continue
            splits = split_info.split('|')
            split_account_id, split_step = splits[0], int(splits[1])
            if account["account_id"] == split_account_id:
                is_split_account = True
                s_dt = start_dt
                while s_dt <= end_dt:
                    c_dt = min(s_dt + timedelta(days=split_step), end_dt)
                    tasks.append(((account, category), s_dt.strftime('%Y-%m-%d'), c_dt.strftime('%Y-%m-%d'), cfg))
                    s_dt = c_dt + timedelta(days=1)
        
        if not is_split_account:
            tasks.append(((account, category), start_ds, ds, cfg))
    
    # 使用多线程并行执行
    results = []
    max_workers = min(3, len(tasks))
    
    if tasks:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(_get_insights_wrapper, task): task for task in tasks}
            for future in as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    task = future_to_task[future]
                    account_info = task[0]
                    print(f"   ❌ Task failed for account {account_info[0]['account_id']}: {e}")
    
    # 处理结果
    report_accounts = []
    failed_accounts = []
    
    for report_info in results:
        if not report_info:
            continue
        if report_info[1] == -1:
            failed_accounts.append(report_info)
        else:
            report_accounts.append(report_info)
    
    # 重试失败的账户
    last_failed_accounts = []
    for account_info in failed_accounts:
        for ac in account_list:
            if ac['account_id'] == account_info[2]:
                report_info = _get_insights((ac, account_info[0]), account_info[3], account_info[4], cfg)
                if report_info[1] != -1:
                    report_accounts.append(report_info)
                else:
                    last_failed_accounts.append(report_info)
    
    if last_failed_accounts:
        helper.sql_error_bot(
            title=f'Facebook spend report',
            text='Facebook failed get_insights\n\n' + str(last_failed_accounts)
        )
    
    # 下载并保存报告
    for (cat, report_id, account, start_ds_item, ds_item) in report_accounts:
        print(f'   📥 Fetching report for {account}')
        try:
            _fetch_export_report(
                report_id=report_id,
                category=cat,
                params={"account": account, "exec_ds": ds, "start_ds": start_ds_item, "ds": ds_item},
                cfg=cfg
            )
        except Exception as e:
            print(f"   ❌ Failed to fetch report for {account}: {e}")
    
    print(f"\n✅ Saved {_AD_NETWORK} spend report for {start_ds} to {ds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK} Spend")

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

helper.validate_and_preview_data(_AD_TYPE, _AD_NETWORK)
