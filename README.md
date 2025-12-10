# MKT DCS Staging

Marketing Data Collection System - Databricks Notebooks 项目。

本项目包含用于从各广告渠道收集消耗报告和上传受众数据的 Databricks Notebooks。

---

## 🚀 快速开始：添加新渠道

> 5 分钟快速添加一个新的广告渠道报告

### Step 1: 复制模板

复制 `aarki_spend_report.py` 作为模板（最简洁的示例）

### Step 2: 修改配置

```python
# --- [配置参数] ---
_AD_NETWORK = 'your_network'   # 渠道名（小写，用于文件名和路径）
_AD_TYPE = 'spend'              # 报告类型: spend/income/iap/mediation/attribution
_DATE_RANGE = 7                 # 回溯天数
```

### Step 3: 修改 API 请求

```python
req_opt = dict(
    url='https://api.your-network.com/report',
    params={'api_key': cfg.get('api_key'), 'start': start_ds, 'end': end_ds},
    headers={'Authorization': f'Bearer {cfg.get("token")}'}  # 可选
)

helper.fetch_report(
    ad_network=_AD_NETWORK,
    ad_type=_AD_TYPE,
    exc_ds=ds,
    start_ds=start_ds,
    end_ds=end_ds,
    **req_opt
)
```

### Step 4: 添加密钥配置

```bash
databricks secrets put --scope dcs-staging-secret --key secret_your_network
# 输入 JSON: {"api_key": "xxx", "token": "xxx"}
```

**就这么简单！** 🎉 系统会自动处理：
- ✅ 数据格式检测（CSV/JSON/JSONL/API响应）
- ✅ 流式下载（大文件不会 OOM）
- ✅ 转换为 JSONL 格式
- ✅ 上传到 S3
- ✅ 保存本地预览（staging 模式）

👉 详细开发规范请看 [NOTEBOOK_GUIDELINES.md](./NOTEBOOK_GUIDELINES.md)

---

## 📁 项目结构

```
mkt-dcs-staging/
├── README.md                      # 本文档
├── NOTEBOOK_GUIDELINES.md         # Notebook 开发规范（必读）
├── config/
│   └── dag_id_to_s3_paths.json    # S3 路径参考文档（仅供参考）
├── utils/
│   ├── config_manager.py          # 配置管理器（环境模式、密钥读取）
│   ├── data_parser.py             # 🆕 数据格式解析器（自动格式转换）
│   └── helper.py                  # 通用工具函数（S3 上传、报告保存等）
├── *_spend_report.py              # 消耗报告 Notebooks
├── *_income_report.py             # 收入报告 Notebooks
├── *_audience.py                  # 受众上传 Notebooks
└── data_output/                   # 本地数据输出目录（staging 模式）
```

---

## 🔧 环境配置

### 环境模式

项目支持两种环境模式：

| 模式 | 说明 | S3 配置 | 本地文件 |
|------|------|---------|----------|
| `staging` | 测试模式 | 上传到 staging bucket | 保存 5MB 预览到本地 |
| `prod` | 生产模式 | 上传到 prod bucket | 不保存本地文件 |

### 设置环境模式

#### 方式一：修改 `config_manager.py`

编辑 `utils/config_manager.py` 文件顶部的配置：

```python
# ===== 集中配置区域 =====
DEFAULT_ENV_MODE = 'staging'  # 修改这里：'staging' 或 'prod'
# ========================
```

#### 方式二：设置环境变量（Databricks 推荐）

```bash
# 在 Databricks Cluster 的环境变量中设置
ENV_MODE=prod
```

---

## 🔐 配置管理

### 配置优先级

配置加载优先级（从高到低）：

1. **Databricks Secrets**（根据环境自动选择 scope）✅ 推荐
   - staging: `dcs-staging-secret`
   - prod: `dcs-prod-secret`
2. **环境变量**（CI/CD 场景）
3. **本地文件 `config/variables.json`**（本地开发 fallback）

### S3 路径规则

S3 路径由代码内置逻辑生成，**不依赖配置文件**：

```
# staging 环境
reports_staging/{ad_type}/{ad_network}/{date}/

# prod 环境
reports/{ad_type}/{ad_network}/{date}/
```

例如：`reports_staging/spend/aarki/2024-01-15/`

### 配置格式

#### Databricks Secrets（推荐）

敏感配置存储在 Databricks Secret Scope 中，根据环境自动选择：

| 环境 | Secret Scope |
|------|--------------|
| staging | `dcs-staging-secret` |
| prod | `dcs-prod-secret` |

```bash
# 查看已有 secrets
databricks secrets list --scope dcs-staging-secret  # staging
databricks secrets list --scope dcs-prod-secret     # prod

# 添加新 Secret（JSON 格式）
databricks secrets put --scope dcs-staging-secret --key secret_new_config
```

#### 环境变量（备用）

环境变量名需大写，格式为 `SECRET_{CONFIG_NAME}`：

```bash
# S3 配置
export SECRET_AWS_S3_PROD='{"aws_key":"xxx","aws_secret":"xxx","bucket":"prod-bucket"}'
export SECRET_AWS_S3_STAGING='{"aws_key":"xxx","aws_secret":"xxx","bucket":"staging-bucket"}'

# 飞书通知
export SECRET_ENV='{"feishu_botid":"xxx"}'

# 各渠道配置
export SECRET_APPSFLYER_SPEND='{"token":"xxx"}'
export SECRET_APPLE_SEARCH='{"client_id":"xxx","client_secret":"xxx","org_ids":[...]}'
```

#### 本地配置文件（本地开发 fallback）

创建 `config/variables.json`（**注意：不要提交到 Git**）：

```json
{
  "secret_aws_s3_prod": {
    "aws_key": "YOUR_AWS_KEY",
    "aws_secret": "YOUR_AWS_SECRET",
    "bucket": "your-prod-bucket"
  },
  "secret_aws_s3_staging": {
    "aws_key": "YOUR_AWS_KEY",
    "aws_secret": "YOUR_AWS_SECRET",
    "bucket": "your-staging-bucket"
  },
  "secret_env": {
    "feishu_botid": "YOUR_FEISHU_BOT_ID"
  }
}
```

---

## ☁️ Databricks 部署

### 1. 上传代码

将项目文件上传到 Databricks Workspace：

```
/Workspace/Repos/Shared/mkt-dcs-staging/
├── utils/
├── config/
├── *_spend_report.py
└── ...
```

### 2. 配置 Secrets

Secrets 根据环境存储在不同的 scope 中：
- staging: `dcs-staging-secret`
- prod: `dcs-prod-secret`

如需添加新配置：
```bash
# staging 环境
databricks secrets put --scope dcs-staging-secret --key secret_new_config

# prod 环境
databricks secrets put --scope dcs-prod-secret --key secret_new_config
```

### 3. 创建 Job

在 Databricks 中创建 Job：

- **Task**: 选择对应的 Notebook
- **Cluster**: 选择或创建计算集群
- **Parameters**: 
  - `ds`: 执行日期（可选，默认为昨天）
- **Schedule**: 设置调度时间

### 4. 环境变量

在 Cluster 配置中添加环境变量：

```
ENV_MODE=prod
```

---

## 📋 Notebook 列表

### 消耗报告 (Spend Report)

| Notebook | 渠道 | 数据格式 | 说明 |
|----------|------|----------|------|
| `aarki_spend_report.py` | Aarki | CSV | ⭐ 最简模板 |
| `apple_search_spend_report.py` | Apple Search Ads | JSON | 嵌套数据结构 |
| `applovin_asset_spend_report.py` | AppLovin | CSV | 多账号 |

### 收入报告 (Income/Revenue Report)

| Notebook | 渠道 | 数据格式 | 说明 |
|----------|------|----------|------|
| `applovin_income_report.py` | AppLovin | API响应 | `{"code":200,"results":[...]}` |
| `applovin_max_revenue_report.py` | AppLovin MAX | API响应 | 同上 |
| `applovin_max_ad_revenue_report.py` | AppLovin MAX | API响应 | 广告收入 |

### 配置报告 (Mediation Config)

| Notebook | 渠道 | 数据格式 | 说明 |
|----------|------|----------|------|
| `applovin_max_report.py` | AppLovin MAX | JSON | 🔧 需展开 `ad_network_settings` |

### IAP 报告

| Notebook | 渠道 | 数据格式 | 说明 |
|----------|------|----------|------|
| `amazon_iap_report.py` | Amazon | CSV/ZIP | 按月获取，需解压 |

---

## 📖 开发规范

请参阅 [NOTEBOOK_GUIDELINES.md](./NOTEBOOK_GUIDELINES.md) 了解：

- Notebook 结构规范（5 个标准部分）
- 失败回调机制
- 命名规范
- 代码风格
- 日志输出规范

---

## 🔍 调试技巧

### 检查配置加载

```python
# 在 Notebook 中运行
from utils.config_manager import get_env_mode, get_s3_config

print(f"Environment Mode: {get_env_mode()}")
print(f"S3 Config: {get_s3_config()}")
```

### 查看原始 API 响应（staging 模式）

在 staging 模式下，系统会自动保存原始响应的前 3MB：

```
data_output/raw_download/{ad_type}/{ad_network}/{date}/{filename}.raw
```

### 测试数据格式解析

```python
from utils.data_parser import detect_format, convert_to_jsonl, DataFormat

# 测试格式检测
sample = '{"code":200,"results":[{"day":"2025-12-09","revenue":100}]}'
fmt = detect_format(sample)
print(f"Detected format: {fmt.value}")  # 输出: api_response

# 测试转换
jsonl, count, _ = convert_to_jsonl(sample)
print(f"Converted {count} rows:\n{jsonl}")
# 输出: {"day":"2025-12-09","revenue":100}
```

### 测试 AppLovin MAX 配置展开

```python
from utils.data_parser import expand_applovin_max_ad_unit

ad_unit = {
    "id": "xxx",
    "name": "Test",
    "platform": "ios",
    "ad_network_settings": {
        "UNITY_BIDDING": {"ad_network_ad_unit_id": "unity_123"},
        "ADMOB_BIDDING": {"ad_network_ad_unit_id": "admob_456"}
    }
}

records = expand_applovin_max_ad_unit(ad_unit)
print(f"Expanded to {len(records)} records")
for r in records:
    print(r)
```

---

## ⚠️ 注意事项

1. **运行环境**：所有 Notebook 必须在 Databricks 集群上运行，SQL 查询使用 `spark.sql()` 执行
2. **敏感信息**：`config/variables.json` 包含敏感信息，确保已添加到 `.gitignore`
3. **环境切换**：切换环境前确认 S3 bucket 配置正确，避免数据写入错误位置
4. **大文件处理**：对于大文件，使用流式处理避免内存溢出
5. **失败通知**：生产环境确保飞书 Bot 配置正确，以便及时收到失败通知

---

## 📞 联系方式

如有问题，请联系项目维护者。
