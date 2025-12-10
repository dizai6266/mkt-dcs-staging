# Databricks Notebook 开发规范 (NOTEBOOK_GUIDELINES.md)

> 本规范适用于从 Airflow DAG 迁移到 Databricks Notebook 的数据报告任务。遵循本规范可确保代码一致性，便于维护和 AI 辅助开发。

---

## 目录结构

```
mkt-dcs-staging/
├── utils/
│   ├── helper.py           # 核心工具函数（上传、保存、通知等）
│   └── config_manager.py   # 配置管理（环境、密钥、S3路径）
├── notebooks/
│   ├── iap/
│   │   └── amazon_iap_report.py
│   ├── spend/
│   │   ├── applovin_asset_spend_report.py
│   │   └── apple_search_spend_report.py
│   └── income/
│       └── ...
└── data_output/            # 本地输出目录（staging/dev 模式）
```

---

## Notebook 标准结构（6 个部分）

每个 Notebook 必须包含以下 6 个部分，按顺序排列：

### Part 1: 标题与说明

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # {广告网络} {报告类型} Report
# MAGIC
# MAGIC 简要说明该 Notebook 的功能。
```

**示例** [1]：
```python
# MAGIC # Amazon IAP Report
# MAGIC
# MAGIC 该 Notebook 从 Amazon API 获取 IAP 销售报告数据。
```

---

### Part 2: Setup & Imports

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import gzip
import io
import json
import os
import shutil
import zipfile
from datetime import datetime, timedelta
import sys

import pandas as pd
import requests

# 动态添加当前目录到 sys.path
current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

from utils import helper
from utils.config_manager import get_env_mode, setup_feishu_notify
import importlib
importlib.reload(helper)

# 设置飞书通知
Notifier = setup_feishu_notify()

print(f"🔧 Environment Mode: {get_env_mode()}")
print(f"✅ Environment Setup Complete. Current Dir: {os.getcwd()}")
```

---

### Part 3: Configuration

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# --- [配置参数] ---
_AD_NETWORK = '{广告网络名}'    # 例如: 'amazon', 'applovin', 'apple_search'
_AD_TYPE = '{报告类型}'          # 可选值: 'iap', 'spend', 'income', 'attribution'

# --- [日期参数] ---
try:
    dbutils.widgets.text("ds", "", "Date (YYYY-MM-DD)")
    ds_param = dbutils.widgets.get("ds")
except:
    ds_param = ""

if not ds_param:
    ds_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

print(f"📅 Execution Date: {ds_param}")
```

**配置参数说明**：

| 变量 | 说明 | 示例值 |
|------|------|--------|
| `_AD_NETWORK` | 广告网络标识（小写） | `'amazon'`, `'applovin'`, `'apple_search'` |
| `_AD_TYPE` | 报告类型 | `'iap'`, `'spend'`, `'income'`, `'attribution'` |
| `ds_param` | 执行日期 | `'2025-12-09'` |

---

### Part 4: Core Functions

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core Functions

# COMMAND ----------

# 在此定义所有业务逻辑函数
```

#### 4.1 核心函数命名规范

| 函数类型 | 命名格式 | 示例 |
|----------|----------|------|
| 主任务函数 | `fetch_{type}_report_task(ds)` | `fetch_iap_report_task(ds)` |
| 数据处理函数 | `_process_and_upload(...)` | `_process_and_upload(file_path, year, month, ds, client_index)` |
| API 调用函数 | `_get_{resource}(...)` | `_get_access_token(...)`, `_get_sale_report_url(...)` |
| 辅助函数 | `_helper_name(...)` | `_get_month_last_day(year, month)` |

#### 4.2 主任务函数模板

```python
def fetch_{type}_report_task(ds: str):
    """
    获取 {AD_NETWORK} {TYPE} 报告
    
    Args:
        ds: 执行日期 (YYYY-MM-DD)
    """
    print(f"📊 Fetching {_AD_NETWORK} report for {ds}")
    
    # 1. 获取配置
    cfg = helper.get_cfg('{config_name}')
    
    # 2. 遍历账号/客户端
    for index, item in enumerate(cfg.get('{key}'), start=1):
        print(f"\n   📱 Processing item {index}...")
        
        # 3. 获取数据
        # ...
        
        # 4. 处理并保存
        helper.save_report(
            ad_network=_AD_NETWORK,
            ad_type=_AD_TYPE,
            report=report_data,      # 支持 CSV/JSON/JSONL 格式，自动转换
            exc_ds=ds,
            start_ds=start_date,
            end_ds=end_date,
            custom=index             # 可选：用于区分多账号
        )
        
        print(f"   ✅ Processed item {index}")
    
    print(f"\n✅ Saved {_AD_NETWORK} report for {ds}")
```

#### 4.3 helper.save_report() 参数说明

```python
helper.save_report(
    ad_network: str,      # 必填：广告网络名
    ad_type: str,         # 必填：报告类型
    report: str,          # 必填：报告数据（支持 CSV/JSON/JSONL，自动检测转换）
    exc_ds: str,          # 必填：执行日期
    start_ds: str,        # 可选：数据开始日期
    end_ds: str,          # 可选：数据结束日期
    report_ds: str,       # 可选：报告日期（与 start_ds/end_ds 二选一）
    custom: any,          # 可选：自定义标识（用于文件名区分多账号）
    data_format: str      # 可选：强制指定格式 ('csv'/'jsonl'/'json_array')
)
```

**生成的文件名规则**：

| 参数组合 | 文件名格式 | 示例 |
|----------|------------|------|
| `custom` + `start_ds` + `end_ds` | `{network}_{custom}_{start}_to_{end}` | `applovin_1_2025-12-02_to_2025-12-08` |
| `start_ds` + `end_ds` | `{network}_{start}_to_{end}` | `amazon_2025-12-01_to_2025-12-31` |
| `report_ds` | `{network}_{report_ds}` | `facebook_2025-12-09` |

**支持的数据格式**（自动检测）：

| 格式 | 识别特征 | 处理方式 |
|------|----------|----------|
| JSONL | 每行以 `{` 开头 `}` 结尾 | 直接验证，不转换 |
| JSON Array | 以 `[` 开头 | 转换为 JSONL |
| JSON Object | 以 `{` 开头（单行） | 转换为单行 JSONL |
| **API Response** 🆕 | `{"code":200,"results":[...]}` | 自动提取 `results` 数组 |
| CSV | 其他情况 | 转换为 JSONL |

**特别说明**：
- `API Response` 格式会自动识别 `results`, `data`, `items`, `records` 等常见字段
- 即使是大文件（流式处理只读取 4KB），也能通过启发式方法检测格式

---

### Part 5: Execution

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution

# COMMAND ----------

print(f"🚀 Starting Job for {_AD_NETWORK}")

try:
    fetch_{type}_report_task(ds_param)
    print("\n✅ Job Finished Successfully")

except Exception as e:
    print(f"\n❌ Job Failed: {e}")
    helper.failure_callback(str(e), f"{_AD_NETWORK}_{_AD_TYPE}_report")
    raise e  # 必须重新抛出，保持 Job 失败状态
```

---

### Part 6: Data Validation

```python
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
        preview_root = os.path.join(base_root, _AD_TYPE, _AD_NETWORK)
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

## 环境模式说明

| 模式 | 本地文件 | S3 上传 | 用途 |
|------|----------|---------|------|
| `dev` | 完整数据 (`.jsonl`) | ❌ | 本地开发调试 |
| `staging` | 5MB 预览 (`.preview`) | ✅ (`reports_staging/`) | 测试验证 |
| `prod` | ❌ | ✅ (`reports/`) | 生产环境 |

---

## 常见迁移模式

### 模式 A：单账号 + 单日期范围（最简单）

**适用场景**：Aarki, AppLovin Asset 等
**参考模板**：`aarki_spend_report.py` ⭐

```python
def fetch_spend_report_task(ds: str):
    cfg = helper.get_cfg(_AD_NETWORK)
    
    req_opt = dict(
        url='https://api.example.com/report',
        params={'token': cfg.get('token'), 'start': start_ds, 'end': end_ds}
    )
    
    # 一行搞定！自动处理格式检测、转换、上传
    helper.fetch_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds,
        **req_opt
    )
```

### 模式 B：多账号

**适用场景**：AppLovin（多个 api_key）

```python
def fetch_spend_report_task(ds: str):
    cfg = helper.get_cfg('applovin')
    
    for item in cfg.get('spend'):
        account_index = item.get('index')
        api_key = item.get('api_key')
        
        req_opt = dict(url='...', params={'api_key': api_key, ...})
        
        helper.fetch_report(
            ad_network=_AD_NETWORK,
            ad_type=_AD_TYPE,
            exc_ds=ds,
            start_ds=start_ds,
            end_ds=end_ds,
            custom=account_index,  # 区分多账号文件名
            **req_opt
        )
```

### 模式 C：API 响应包装格式

**适用场景**：AppLovin Income/MAX Revenue（返回 `{"code":200,"results":[...]}`）
**参考模板**：`applovin_income_report.py`

```python
# 无需特殊处理！系统自动识别并提取 results 数组
helper.fetch_report(
    ad_network=_AD_NETWORK,
    ad_type=_AD_TYPE,
    exc_ds=ds,
    start_ds=start_ds,
    end_ds=end_ds,
    **req_opt
)
```

### 模式 D：需要展开嵌套字段 🆕

**适用场景**：AppLovin MAX Config（需要展开 `ad_network_settings`）
**参考模板**：`applovin_max_report.py`

```python
from utils.data_parser import convert_applovin_max_config

def fetch_max_report_task(ds: str):
    all_records = []
    
    for ad_unit in ad_units:
        response = requests.get(f'https://api.../ad_unit/{ad_unit}', ...)
        
        # 使用专用转换器展开 ad_network_settings
        jsonl_content, row_count = convert_applovin_max_config(response.text)
        
        for line in jsonl_content.split('\n'):
            if line.strip():
                all_records.append(line)
    
    # 保存合并后的数据
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report_content='\n'.join(all_records),
        exc_ds=ds,
        start_ds=start_ds,
        end_ds=end_ds,
        data_format='jsonl'
    )
```

**展开效果**：
```
输入: {"id":"xxx", "ad_network_settings": {"UNITY": {...}, "ADMOB": {...}}}
输出:
{"id":"xxx", "network":"UNITY", "ad_network_ad_unit_id":"..."}
{"id":"xxx", "network":"ADMOB", "ad_network_ad_unit_id":"..."}
```

### 模式 E：多层嵌套 + 合并

**适用场景**：Apple Search Ads（Org → Campaign → Keywords）

```python
def fetch_spend_report_task(ds: str):
    all_data = []
    
    for org in cfg.get('spend'):
        campaigns = _get_campaigns(org)
        
        for campaign in campaigns:
            report = _get_campaign_report(campaign)
            detail_data = _parse_detail_data(report, campaign_info=campaign)
            all_data.extend(detail_data)
    
    # 合并所有数据后保存
    helper.save_report(
        ad_network=_AD_NETWORK,
        ad_type=_AD_TYPE,
        report=json.dumps(all_data),
        exc_ds=ds,
        report_ds=ds
    )
```

---

## 数据格式解析器 (data_parser.py) 🆕

`utils/data_parser.py` 提供了强大的数据格式自动识别和转换功能。

### 支持的数据格式

| 格式 | 示例 | 自动处理 |
|------|------|----------|
| CSV | `col1,col2\nval1,val2` | ✅ 转换为 JSONL |
| JSONL | `{"a":1}\n{"a":2}` | ✅ 直接验证 |
| JSON Array | `[{"a":1},{"a":2}]` | ✅ 转换为 JSONL |
| JSON Object | `{"a":1}` | ✅ 转换为单行 JSONL |
| API Response | `{"code":200,"results":[...]}` | ✅ 提取 `results` 数组 |

### 核心函数

```python
from utils.data_parser import (
    detect_format,           # 检测数据格式
    convert_to_jsonl,        # 转换为 JSONL
    expand_applovin_max_ad_unit,  # 展开 ad_network_settings
    convert_applovin_max_config,  # AppLovin MAX 配置转换
)
```

### 使用示例

#### 1. 格式检测

```python
from utils.data_parser import detect_format, DataFormat

# API 响应格式
fmt = detect_format('{"code":200,"results":[{"day":"2025-12-09"}]}')
print(fmt)  # DataFormat.API_RESPONSE

# CSV 格式
fmt = detect_format('day,revenue\n2025-12-09,100')
print(fmt)  # DataFormat.CSV
```

#### 2. 转换为 JSONL

```python
from utils.data_parser import convert_to_jsonl

# API 响应自动提取 results
api_resp = '{"code":200,"results":[{"day":"2025-12-09","revenue":100}]}'
jsonl, count, fmt = convert_to_jsonl(api_resp)
print(f"Converted {count} rows")
print(jsonl)  # {"day":"2025-12-09","revenue":100}
```

#### 3. 展开 AppLovin MAX 配置

```python
from utils.data_parser import expand_applovin_max_ad_unit

ad_unit = {
    "id": "abc123",
    "name": "Test_iOS_Inter",
    "platform": "ios",
    "ad_network_settings": {
        "UNITY_BIDDING": {"disabled": False, "ad_network_ad_unit_id": "unity_xxx"},
        "ADMOB_BIDDING": {"disabled": False, "ad_network_ad_unit_id": "admob_yyy"}
    }
}

records = expand_applovin_max_ad_unit(ad_unit)
# 输出 2 条记录，每个 network 一行
for r in records:
    print(r)
# {'id': 'abc123', 'name': 'Test_iOS_Inter', 'platform': 'ios', 'network': 'UNITY_BIDDING', 'ad_network_ad_unit_id': 'unity_xxx'}
# {'id': 'abc123', 'name': 'Test_iOS_Inter', 'platform': 'ios', 'network': 'ADMOB_BIDDING', 'ad_network_ad_unit_id': 'admob_yyy'}
```

### 添加自定义转换器

如果遇到新的嵌套数据格式，可以在 `data_parser.py` 中添加新的转换函数：

```python
def expand_your_nested_data(data: dict) -> List[dict]:
    """
    展开你的嵌套数据
    
    参考 expand_applovin_max_ad_unit() 的实现
    """
    base_fields = ['id', 'name', ...]
    base_record = {k: data.get(k) for k in base_fields}
    
    nested_items = data.get('nested_field', [])
    expanded = []
    
    for item in nested_items:
        record = base_record.copy()
        record['nested_key'] = item.get('key')
        expanded.append(record)
    
    return expanded
```

---

## 辅助函数库

### 获取月份最后一天

```python
def _get_month_last_day(year: int, month: int) -> int:
    """获取指定月份的最后一天"""
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1)
    else:
        next_month_first = datetime(year, month + 1, 1)
    return (next_month_first - timedelta(days=1)).day
```

### CSV 添加额外列

```python
def _add_columns_to_csv(csv_str: str, extra_columns: dict) -> str:
    """给 CSV 数据添加额外列"""
    lines = csv_str.strip().split('\n')
    if not lines:
        return csv_str
    
    # 添加 header
    extra_keys = ','.join(extra_columns.keys())
    header = f"{lines[0]},{extra_keys}"
    
    # 添加数据
    extra_values = ','.join(str(v) for v in extra_columns.values())
    modified_lines = [header]
    for line in lines[1:]:
        if line.strip():
            modified_lines.append(f"{line},{extra_values}")
    
    return '\n'.join(modified_lines)
```

---

## Checklist

### 新增 Notebook 前，请确认以下事项：

**基础配置**
- [ ] 设置正确的 `_AD_NETWORK` 和 `_AD_TYPE`
- [ ] 配置已添加到 Databricks Secrets（`secret_{network_name}`）
- [ ] 主函数命名遵循 `fetch_{type}_report_task(ds)` 格式

**数据处理**
- [ ] 使用 `helper.fetch_report()` 或 `helper.save_report()` 保存数据
- [ ] 如果是 API 响应包装格式（`{"code":200,"results":[...]}`），无需特殊处理
- [ ] 如果有嵌套字段需要展开，添加自定义转换器（参考 `expand_applovin_max_ad_unit()`）

**错误处理**
- [ ] 包含 try-except 和 `helper.failure_callback()`
- [ ] 包含 Data Validation 部分

**测试验证**
- [ ] 在 staging 环境测试通过
- [ ] 检查 `data_output/raw_download/` 下的原始响应（调试用）
- [ ] Preview 文件可正常读取（`pd.read_json(file, lines=True)`）
- [ ] 输出格式符合预期（每行一个 JSON 对象）

---

## 常见问题排查

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| Preview 文件读取失败 | JSON 格式错误 | 检查是否有特殊字符，使用 `ensure_ascii=False` |
| 多账号文件覆盖 | 未使用 `custom` 参数 | 添加 `custom=index` 区分文件名 |
| S3 上传失败 | 配置缺失 | 检查 Secrets 中的 S3 配置 |
| 数据被截断 | 5MB preview 限制 | 正常现象，完整数据在 S3 |
| **格式检测为 UNKNOWN** 🆕 | 流式处理只读取 4KB | 系统会自动重新检测，或检查 `.raw` 文件 |
| **API 响应未正确解析** 🆕 | 非标准包装格式 | 检查字段名是否在 `API_RESPONSE_DATA_KEYS` 中 |
| **嵌套数据未展开** 🆕 | 需要自定义转换器 | 参考 `expand_applovin_max_ad_unit()` 添加转换器 |
| **Preview 显示原始格式** 🆕 | 格式转换失败 | 检查 `raw_download/` 下的原始响应格式 |

### 调试步骤

1. **查看原始响应**（staging 模式）：
   ```
   data_output/raw_download/{ad_type}/{ad_network}/{date}/*.raw
   ```

2. **测试格式检测**：
   ```python
   from utils.data_parser import detect_format
   with open('xxx.raw', 'r') as f:
       sample = f.read(1000)
   print(detect_format(sample))
   ```

3. **手动测试转换**：
   ```python
   from utils.data_parser import convert_to_jsonl
   jsonl, count, fmt = convert_to_jsonl(sample_data)
   print(f"Format: {fmt}, Rows: {count}")
   ```