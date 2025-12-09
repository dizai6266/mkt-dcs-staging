# MKT DCS Staging

Marketing Data Collection System - Databricks Notebooks 项目。

本项目包含用于从各广告渠道收集消耗报告和上传受众数据的 Databricks Notebooks。

---

## 📁 项目结构

```
mkt-dcs-staging/
├── README.md                      # 本文档
├── NOTEBOOK_GUIDELINES.md         # Notebook 开发规范
├── config/
│   ├── variables.json             # 本地配置文件（敏感信息，不提交 Git）
│   └── dag_id_to_s3_paths.json    # DAG ID 到 S3 路径的映射配置
├── utils/
│   ├── config_manager.py          # 配置管理器（环境模式、密钥读取）
│   └── helper.py                  # 通用工具函数（S3 上传、报告保存等）
├── *_spend_report.py              # 消耗报告 Notebooks
├── *_audience.py                  # 受众上传 Notebooks
└── data_output/                   # 本地数据输出目录（staging/dev 模式）
```

---

## 🔧 环境配置

### 环境模式

项目支持三种环境模式：

| 模式 | 说明 | S3 配置 | 本地文件 |
|------|------|---------|----------|
| `dev` | 开发模式 | 不上传 S3 | 保存完整数据到本地 |
| `staging` | 测试模式 | 上传到 staging bucket | 保存 5MB 预览到本地 |
| `prod` | 生产模式 | 上传到 prod bucket | 不保存本地文件 |

### 设置环境模式

#### 方式一：修改 `config_manager.py`（推荐本地开发）

编辑 `utils/config_manager.py` 文件顶部的配置：

```python
# ===== 集中配置区域 =====
DEFAULT_ENV_MODE = 'staging'  # 修改这里：'dev'、'staging' 或 'prod'
FORCE_DEFAULT_ENV_MODE = False  # 设置为 True 可强制使用 DEFAULT_ENV_MODE
# ========================
```

#### 方式二：设置环境变量（Databricks 推荐）

```bash
# Linux/macOS
export ENV_MODE=staging

# 或在 Databricks Cluster 的环境变量中设置
```

#### 方式三：强制覆盖（临时调试）

```bash
export FORCE_DEFAULT_ENV_MODE=true
export ENV_MODE=dev
```

---

## 🔐 配置管理

### 配置优先级

配置加载优先级（从高到低）：

1. **Databricks Secrets**（生产环境推荐）
2. **环境变量**（CI/CD 推荐）
3. **本地文件 `config/variables.json`**（本地开发）

### 配置格式

#### Databricks Secrets

在 Databricks 中创建 Secret Scope `airflow_secrets`：

```bash
# 使用 Databricks CLI
databricks secrets create-scope --scope airflow_secrets

# 添加 Secret（JSON 格式）
databricks secrets put --scope airflow_secrets --key secret_aws_s3_prod
# 然后输入 JSON 内容
```

#### 环境变量

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

#### 本地配置文件

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
  },
  "secret_appsflyer_spend": {
    "token": "YOUR_TOKEN"
  },
  "secret_apple_search": {
    "client_id": "xxx",
    "client_secret": "xxx",
    "org_ids": [
      ["org_id_1", true],
      ["org_id_2", false]
    ]
  }
}
```

---

## 🚀 本地开发

### 1. 克隆项目

```bash
git clone https://github.com/dizai6266/mkt-dcs-staging.git
cd mkt-dcs-staging/mkt-dcs-staging
```

### 2. 安装依赖

```bash
pip install pandas boto3 requests
# 根据需要安装其他依赖
pip install facebook-business  # Facebook Audience
pip install databricks-sql-connector  # Databricks SQL
```

### 3. 配置环境

```bash
# 创建本地配置文件
cp config/variables.json.example config/variables.json
# 编辑 variables.json 填入真实配置

# 设置为开发模式
# 编辑 utils/config_manager.py，设置 DEFAULT_ENV_MODE = 'dev'
```

### 4. 运行 Notebook

```bash
# 使用 Python 直接运行（会执行 Notebook 中的代码）
python appsflyer_spend_report.py

# 或在 Jupyter/Databricks 中打开运行
```

### 5. 检查输出

开发模式下，数据会保存到 `data_output/` 目录：

```
data_output/
├── spend/
│   └── appsflyer_spend/
│       └── 2024-01-15/
│           └── appsflyer_spend_2024-01-08_to_2024-01-15
└── income/
    └── ...
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

```bash
# 创建 Secret Scope
databricks secrets create-scope --scope airflow_secrets

# 添加必要的 Secrets
databricks secrets put --scope airflow_secrets --key secret_aws_s3_prod
databricks secrets put --scope airflow_secrets --key secret_env
# ... 其他配置
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

| Notebook | 渠道 | 调度 |
|----------|------|------|
| `appsflyer_spend_report.py` | AppsFlyer | 每日 |
| `apple_search_spend_report.py` | Apple Search Ads | 每日 |
| `applovin_asset_spend_report.py` | AppLovin | 每日 |
| `aarki_spend_report.py` | Aarki | 每日 |

### 受众上传 (Audience Upload)

| Notebook | 渠道 | 调度 |
|----------|------|------|
| `facebook_audience.py` | Facebook | 每日 |
| `facebook_audience_weekly.py` | Facebook | 每周一 |
| `aarki_audience.py` | Aarki | 每日 |
| `af_audience.py` | AppsFlyer | 每日 |
| `af_audience_2.py` | AppsFlyer (v2) | 每日 |
| `af_audience_apl.py` | AppsFlyer (APL) | 每日 |

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

### 1. 检查配置加载

```python
# 在 Notebook 中运行
from utils.config_manager import get_env_mode, get_s3_config

print(f"Environment Mode: {get_env_mode()}")
print(f"S3 Config: {get_s3_config()}")
```

### 2. 运行配置管理器测试

```bash
cd mkt-dcs-staging
python -m utils.config_manager
```

### 3. 查看本地输出

```bash
# 查看生成的数据文件
ls -la data_output/spend/

# 预览 JSONL 数据
head -5 data_output/spend/appsflyer_spend/2024-01-15/appsflyer_spend_*.preview
```

---

## ⚠️ 注意事项

1. **敏感信息**：`config/variables.json` 包含敏感信息，确保已添加到 `.gitignore`
2. **环境切换**：切换环境前确认 S3 bucket 配置正确，避免数据写入错误位置
3. **大文件处理**：对于大文件，使用流式处理避免内存溢出
4. **失败通知**：生产环境确保飞书 Bot 配置正确，以便及时收到失败通知

---

## 📞 联系方式

如有问题，请联系项目维护者。
