# Databricks Notebook 开发规范

本文档定义了 MKT DCS 项目中 Databricks Notebook 的开发规范，确保代码一致性、可维护性和可靠性。

---

## 目录

1. [整体结构](#整体结构)
2. [各部分详解](#各部分详解)
3. [失败回调机制](#失败回调机制)
4. [命名规范](#命名规范)
5. [代码风格](#代码风格)
6. [日志输出规范](#日志输出规范)
7. [模板示例](#模板示例)

---

## 整体结构

每个 Notebook 必须严格遵循以下 **5 个部分** 的结构：

```
┌────────────────────────────────────────────┐
│  # Title & Description                      │
├────────────────────────────────────────────┤
│  ## 1. Setup & Imports                      │
├────────────────────────────────────────────┤
│  ## 2. Configuration                        │
├────────────────────────────────────────────┤
│  ## 3. Task Logic                           │
├────────────────────────────────────────────┤
│  ## 4. Execution                            │
├────────────────────────────────────────────┤
│  ## 5. Data Validation                      │
└────────────────────────────────────────────┘
```

---

## 各部分详解

### Part 1: Title & Description

**必须包含**：
- Notebook 标题（使用 H1）
- 简短的功能描述

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # {AdNetwork} Spend Report
# MAGIC
# MAGIC 该 Notebook 从 {AdNetwork} API 获取广告消耗数据。
```

### Part 2: Setup & Imports

**必须包含**：
1. 标准库导入
2. 第三方库导入
3. 项目内部模块导入
4. 环境初始化确认

```python
# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
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
```

### Part 3: Configuration

**必须包含**：
1. `_AD_NETWORK` 常量定义
2. `_DATE_RANGE` 常量（如适用）
3. Widget 参数获取（使用 try-except 兼容本地运行）
4. 参数验证和默认值

```python
# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = 'ad_network_name'
_DATE_RANGE = 7  # 日期范围（天）

# 获取 Widget 参数
try:
    dbutils.widgets.text("ds", "", "Execution Date (YYYY-MM-DD)")
    ds_param = dbutils.widgets.get("ds")
except:
    ds_param = ""

if not ds_param:
    ds_param = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"📅 Execution Date: {ds_param}")
```

### Part 4: Task Logic

**必须包含**：
1. 主任务函数 `fetch_spend_report_task(ds: str)`
2. 辅助函数（如需要）
3. 完整的 docstring

```python
# MAGIC %md
# MAGIC ## 3. Task Logic

# COMMAND ----------

def fetch_spend_report_task(ds: str):
    """
    获取 {AdNetwork} 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    # 1. 计算日期范围
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    # 2. 获取配置
    cfg = helper.get_cfg(_AD_NETWORK)
    
    # 3. 调用 API
    # ... API 调用逻辑 ...
    
    # 4. 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK, 
        ad_type=helper._AD_TYPE_SPEND, 
        report=report_data, 
        exc_ds=ds, 
        start_ds=start_ds, 
        end_ds=end_ds
    )
    print(f"✅ Saved {_AD_NETWORK} report for {start_ds} to {end_ds}")
```

### Part 5: Execution

**必须包含**：
1. Job 启动日志
2. try-except 包裹的任务执行
3. **on_failure_callback**: 失败时调用 `helper.failure_callback()`
4. 重新抛出异常（保持 Databricks Job 失败状态）

```python
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
```

### Part 6: Data Validation

**必须包含**：
1. 环境模式检查
2. staging 模式下的数据预览
3. 异常处理

```python
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
```

---

## 失败回调机制

### 基本用法

```python
try:
    fetch_spend_report_task(ds_param)
    print("\n✅ Job Finished Successfully")
except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    # on_failure_callback: 失败时发送飞书通知
    helper.failure_callback(str(e), f"{_AD_NETWORK}_spend_report")
    raise e  # 必须重新抛出，保持 Job 失败状态
```

### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `exception_msg` | `str` | 异常信息文本 |
| `job_name` | `str` | Job 名称，建议格式: `{ad_network}_spend_report` |

### 通知内容

失败时会发送飞书通知，包含：
- **JOB**: Job 名称
- **ERROR**: 错误详情

### 高级用法（使用 feishu-notify）

如需更丰富的通知功能，可使用 `feishu-notify` 模块：

```python
from feishu_notify import Notifier

notifier = Notifier(webhook="https://...", source="Databricks")

# 发送错误通知
notifier.error(
    title=f"任务失败: {_AD_NETWORK}",
    error_msg=str(e),
    task_name=f"{_AD_NETWORK}_spend_report",
    link_url="https://databricks.com/job/xxx"
)
```

---

## 命名规范

### 文件命名

| 类型 | 格式 | 示例 |
|------|------|------|
| Spend 报告 | `{ad_network}_spend_report.py` | `aarki_spend_report.py` |
| Asset 报告 | `{ad_network}_asset_spend_report.py` | `applovin_asset_spend_report.py` |
| 归因报告 | `{ad_network}_attribution_report.py` | `appsflyer_attribution_report.py` |

### 常量命名

| 常量 | 说明 | 示例 |
|------|------|------|
| `_AD_NETWORK` | 广告网络标识（小写，下划线分隔） | `'apple_search'` |
| `_DATE_RANGE` | 日期范围（天数） | `7` |
| `_AD_TYPE_*` | 报告类型（从 helper 导入） | `helper._AD_TYPE_SPEND` |

### 函数命名

| 函数 | 说明 |
|------|------|
| `fetch_spend_report_task(ds)` | 主任务函数 |
| `get_xxx_token(cfg)` | 获取 Token |
| `get_xxx_info(...)` | 获取特定信息 |
| `_parse_xxx_data(...)` | 内部解析函数（下划线前缀） |

---

## 代码风格

### 通用规范

1. **缩进**: 4 空格
2. **行宽**: 不超过 120 字符
3. **空行**: 函数之间 2 空行，逻辑块之间 1 空行
4. **注释**: 使用中文注释，复杂逻辑必须注释

### 导入顺序

```python
# 1. 标准库
import os
import sys
import json
from datetime import datetime, timedelta

# 2. 第三方库
import requests
import pandas as pd

# 3. 项目内部模块
from utils import helper
from utils.config_manager import get_env_mode
```

### 字符串格式化

使用 f-string：

```python
# ✅ 推荐
print(f"Date: {start_ds} to {end_ds}")

# ❌ 不推荐
print("Date: {} to {}".format(start_ds, end_ds))
print("Date: %s to %s" % (start_ds, end_ds))
```

### 异常处理

```python
# ✅ 推荐：具体异常 + 上下文信息
try:
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f"API Error: {response.status_code} {response.text[:200]}")
except Exception as e:
    print(f"❌ Error: {e}")
    raise

# ❌ 不推荐：吞掉异常
try:
    response = requests.get(url)
except:
    pass
```

---

## 日志输出规范

### Emoji 前缀

| Emoji | 含义 | 使用场景 |
|-------|------|----------|
| 🔧 | 配置 | 环境配置信息 |
| ✅ | 成功 | 操作完成 |
| ❌ | 失败 | 错误发生 |
| ⚠️ | 警告 | 非致命问题 |
| 📅 | 日期 | 执行日期 |
| 📆 | 范围 | 日期范围 |
| 📡 | 请求 | API 请求 |
| 📋 | 列表 | 数据统计 |
| 📊 | 数据 | 数据量统计 |
| 🔑 | 认证 | Token 获取 |
| 🔎 | 搜索 | 文件扫描 |
| 🚀 | 启动 | Job 开始 |

### 日志格式

```python
# 阶段开始
print(f"🚀 Starting Job for {_AD_NETWORK}")

# 配置信息
print(f"📅 Execution Date: {ds_param}")
print(f"📆 Date Range: {start_ds} to {end_ds}")

# API 调用
print(f"📡 Fetching report from: {url}")

# 数据统计
print(f"📊 Total records: {len(records)}")

# 操作完成
print(f"✅ Saved {_AD_NETWORK} report")

# 错误信息
print(f"❌ Job Failed: {e}")

# 警告信息
print(f"⚠️ No data returned")
```

---

## 模板示例

完整的 Notebook 模板：

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # {AdNetwork} Spend Report
# MAGIC
# MAGIC 该 Notebook 从 {AdNetwork} API 获取广告消耗数据。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
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
_AD_NETWORK = 'ad_network_name'
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

def fetch_spend_report_task(ds: str):
    """
    获取 {AdNetwork} 消耗报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    end_dt = datetime.strptime(ds, '%Y-%m-%d')
    end_ds = ds
    start_dt = end_dt + timedelta(days=-(_DATE_RANGE))
    start_ds = start_dt.strftime('%Y-%m-%d')
    
    print(f"📆 Date Range: {start_ds} to {end_ds}")
    
    cfg = helper.get_cfg(_AD_NETWORK)
    # TODO: 实现 API 调用逻辑
    
    # 保存报告
    helper.save_report(
        ad_network=_AD_NETWORK, 
        ad_type=helper._AD_TYPE_SPEND, 
        report=report_data, 
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
```

---

## Checklist

新增 Notebook 前，请确认以下事项：

- [ ] 文件名符合 `{ad_network}_spend_report.py` 格式
- [ ] 包含完整的 5 个部分结构
- [ ] `_AD_NETWORK` 常量已正确定义
- [ ] 主函数 `fetch_spend_report_task(ds)` 已实现
- [ ] Execution 部分使用 try-except 并调用 `helper.failure_callback()`
- [ ] 异常被正确重新抛出 (`raise e`)
- [ ] Data Validation 部分已添加
- [ ] 日志输出使用规范的 Emoji 前缀
- [ ] 函数包含完整的 docstring

---

## 更新记录

| 日期 | 版本 | 更新内容 |
|------|------|----------|
| 2025-12-09 | v1.0 | 初始版本 |

