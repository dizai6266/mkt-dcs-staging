import os
import logging
import json
import base64
import os
os.environ['ENV_MODE'] = 'staging'

# 配置日志
logger = logging.getLogger(__name__)

# ===== 集中配置区域 =====
# 在这里修改默认环境模式：'staging' 或 'prod'
# 如果设置了环境变量 ENV_MODE，会优先使用环境变量
DEFAULT_ENV_MODE = 'staging'  # 修改这里即可改变所有报告的默认环境模式

# feishu-notify 路径配置
FEISHU_NOTIFY_PATH_PROD = '/Workspace/Repos/Shared/feishu-notify'
FEISHU_NOTIFY_PATH_STAGING = '/Workspace/Users/dizai@joycastle.mobi/feishu-notify'
# ========================

# 检测运行环境
IS_DATABRICKS = False
try:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    IS_DATABRICKS = True
except ImportError:
    pass

def _find_project_root():
    """
    查找项目根目录（通过查找包含 config 目录的位置）
    返回项目根目录路径，如果找不到则返回当前工作目录
    """
    search_dir = os.getcwd()
    
    # 向上查找，找到包含 config 目录的位置
    for _ in range(10):  # 最多向上查找10层
        config_dir = os.path.join(search_dir, 'config')
        if os.path.exists(config_dir) and os.path.isdir(config_dir):
            return search_dir
        parent = os.path.dirname(search_dir)
        if parent == search_dir:  # 到达根目录
            break
        search_dir = parent
    
    # 如果没找到，使用当前工作目录
    return os.getcwd()

def init_env_mode():
    """
    初始化环境模式
    如果环境变量 ENV_MODE 未设置，则使用 DEFAULT_ENV_MODE
    此函数在模块加载时自动调用
    """
    if not os.getenv('ENV_MODE'):
        os.environ['ENV_MODE'] = DEFAULT_ENV_MODE
        logger.info(f"🔧 ENV_MODE not set, using default: {DEFAULT_ENV_MODE}")
    else:
        existing_env_mode = os.getenv('ENV_MODE')
        logger.info(f"🔧 ENV_MODE from environment: {existing_env_mode}")

def get_env_mode():
    """
    获取环境模式：prod 或 staging
    默认返回 DEFAULT_ENV_MODE（可通过修改文件顶部 DEFAULT_ENV_MODE 变量来改变）
    """
    # 确保环境模式已初始化
    if not os.getenv('ENV_MODE'):
        init_env_mode()
    
    env_mode = os.getenv('ENV_MODE', DEFAULT_ENV_MODE).lower()
    if env_mode not in ['prod', 'staging']:
        logger.warning(f"⚠️ Invalid ENV_MODE: {env_mode}, defaulting to '{DEFAULT_ENV_MODE}'")
        return DEFAULT_ENV_MODE
    return env_mode

# 模块加载时自动初始化环境模式
init_env_mode()

def get_feishu_notify_path():
    """
    获取 feishu-notify 模块的路径
    - prod: 使用共享路径 FEISHU_NOTIFY_PATH_PROD
    - staging: 使用开发路径 FEISHU_NOTIFY_PATH_STAGING
    """
    env_mode = get_env_mode()
    if env_mode == 'prod':
        return FEISHU_NOTIFY_PATH_PROD
    return FEISHU_NOTIFY_PATH_STAGING

def setup_feishu_notify():
    """
    设置 feishu-notify 模块路径并导入 Notifier
    
    Usage:
        from utils.config_manager import setup_feishu_notify
        Notifier = setup_feishu_notify()
    
    Returns:
        Notifier class from feishu-notify module
    """
    import sys
    feishu_path = get_feishu_notify_path()
    if feishu_path not in sys.path:
        sys.path.append(feishu_path)
    
    try:
        from notifier import Notifier
        return Notifier
    except ImportError as e:
        logger.warning(f"⚠️ Failed to import Notifier from {feishu_path}: {e}")
        return None

def get_secret_config(config_name):
    """
    统一的配置获取入口
    优先级（按顺序尝试，全部失败才报错）:
    1. Databricks Secrets（如果可用）
    2. 环境变量 (SECRET_NAME)
    3. 本地文件 variables.json (fallback)
    """
    full_key = f'secret_{config_name}'
    errors = []
    
    # --- 优先级 1: Databricks Secrets ---
    if IS_DATABRICKS:
        try:
            secret_val = dbutils.secrets.get(scope="airflow_secrets", key=full_key)
            if secret_val:
                logger.info(f"✅ Loading config {config_name} from Databricks Secrets: {full_key}")
                try:
                    # 尝试直接解析 JSON
                    return json.loads(secret_val)
                except json.JSONDecodeError:
                    # 兼容旧的 Base64 编码格式
                    try:
                        return json.loads(base64.b64decode(secret_val.encode('utf-8')).decode('utf-8'))
                    except Exception as e:
                        logger.warning(f"Failed to decode Databricks secret {full_key}: {e}")
                        errors.append(f"Databricks secret decode error: {e}")
        except Exception as e:
            # Databricks secrets.get 如果 key 不存在会抛错，这是正常的
            logger.debug(f"Databricks secret {full_key} not found: {e}")
            errors.append(f"Databricks secret not found: {e}")

    # --- 优先级 2: 环境变量 ---
    env_key = full_key.upper()
    env_val = os.getenv(env_key)
    
    if env_val:
        logger.info(f"✅ Loading config {config_name} from Environment Variable: {env_key}")
        try:
            # 尝试解析 JSON
            return json.loads(env_val)
        except json.JSONDecodeError:
            # 尝试 Base64 解码 (兼容旧的 Base64 编码字符串)
            try:
                return json.loads(base64.b64decode(env_val.encode('utf-8')).decode('utf-8'))
            except Exception as e:
                logger.warning(f"Failed to decode env var {env_key}: {e}")
                errors.append(f"Env var decode error: {e}")
    else:
        errors.append(f"Env var {env_key} not set")

    # --- 优先级 3: 本地文件 variables.json (fallback) ---
    try:
        # 查找项目根目录
        project_root = _find_project_root()
        
        # 只查找 config/variables.json（相对路径）
        json_path = os.path.join(project_root, 'config', 'variables.json')
        
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r') as f:
                    all_vars = json.load(f)
                
                if full_key in all_vars:
                    logger.info(f"✅ Loading config {config_name} from local file: {json_path}")
                    val = all_vars[full_key]
                    
                    if isinstance(val, str):
                        try:
                            return json.loads(val)
                        except json.JSONDecodeError:
                            try:
                                return json.loads(base64.b64decode(val.encode('utf-8')).decode('utf-8'))
                            except:
                                return val
                    return val
            except Exception as e:
                logger.debug(f"Error reading {json_path}: {e}")
                errors.append(f"Local file read error: {e}")
        else:
            logger.debug(f"Config file not found: {json_path}")
            errors.append(f"Local file not found: {json_path}")

    except Exception as e:
        logger.warning(f"Error reading local variables.json: {e}")
        errors.append(f"Local file read error: {e}")

    # --- 所有方式都失败，抛出异常 ---
    error_msg = f'Miss the config of {config_name}. Tried: {"; ".join(errors)}'
    logger.error(error_msg)
    raise ValueError(error_msg)

def get_s3_config():
    """
    根据环境模式获取 S3 配置
    - prod: 返回 aws_s3_prod 配置
    - staging: 返回 aws_s3_staging 配置
    """
    env_mode = get_env_mode()
    
    if env_mode == 'prod':
        logger.info("🚀 [PROD MODE] Loading prod S3 config")
        return get_secret_config('aws_s3_prod')
    else:
        logger.info("🧪 [STAGING MODE] Loading staging S3 config")
        return get_secret_config('aws_s3_staging')

def get_dag_s3_path_config():
    """
    读取 dag_id_to_s3_paths.json 配置文件
    返回字典：{dag_id: s3_path_template}
    """
    try:
        # 查找项目根目录
        project_root = _find_project_root()
        
        # 读取 config/dag_id_to_s3_paths.json
        config_path = os.path.join(project_root, 'config', 'dag_id_to_s3_paths.json')
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            logger.warning(f"⚠️ dag_id_to_s3_paths.json not found at: {config_path}")
            return {}
    except Exception as e:
        logger.warning(f"⚠️ Failed to load dag_id_to_s3_paths.json: {e}")
        return {}

def get_s3_path_for_dag(dag_id: str, exc_ds: str = None):
    """
    根据 DAG ID 和环境模式获取 S3 路径模板，并替换变量
    
    支持两种配置方式：
    1. 简单配置：{"dag_id": "reports/spend/xxx/{{ds}}/*"} - 所有环境共用
    2. 环境区分：{"dag_id": {"prod": "reports/spend/xxx/{{ds}}/*", "staging": "staging/reports/spend/xxx/{{ds}}/*"}}
    
    Args:
        dag_id: DAG ID 或 job name
        exc_ds: 执行日期 (YYYY-MM-DD)，用于替换 {{ds}}
    
    Returns:
        S3 路径字符串，如果找不到配置则返回 None
    """
    path_config = get_dag_s3_path_config()
    env_mode = get_env_mode()
    
    if dag_id not in path_config:
        logger.warning(f"⚠️ DAG ID '{dag_id}' not found in dag_id_to_s3_paths.json")
        return None
    
    path_template_config = path_config[dag_id]
    
    # 判断配置格式：如果是字典，说明区分了环境；如果是字符串，所有环境共用
    path_template = None
    if isinstance(path_template_config, dict):
        # 环境区分配置
        if env_mode in path_template_config:
            path_template = path_template_config[env_mode]
        elif 'default' in path_template_config:
            # 如果没有对应环境的配置，使用 default
            path_template = path_template_config['default']
            logger.warning(f"⚠️ No {env_mode} path config for {dag_id}, using default")
        else:
            logger.warning(f"⚠️ No {env_mode} or default path config for {dag_id}")
            return None
    else:
        # 简单配置：所有环境共用
        path_template = path_template_config
    
    # 确保 path_template 是字符串类型
    if not isinstance(path_template, str):
        logger.error(f"❌ Invalid path_template type for {dag_id}: {type(path_template)}, expected str")
        logger.error(f"   path_template_config: {path_template_config}")
        logger.error(f"   env_mode: {env_mode}")
        logger.error(f"   path_template value: {path_template}")
        if isinstance(path_template, dict):
            logger.error(f"   path_template keys: {list(path_template.keys())}")
        return None
    
    # 替换模板变量
    if exc_ds:
        path_template = path_template.replace('{{ds}}', exc_ds)
    
    # 移除末尾的 /* 通配符（如果存在），因为我们要上传具体文件
    if path_template.endswith('/*'):
        path_template = path_template[:-2]
    
    return path_template


def _mask_sensitive_info(value, max_show=4):
    """隐藏敏感信息，只显示前几个字符"""
    if not value:
        return value
    str_value = str(value)
    if len(str_value) <= max_show:
        return '*' * len(str_value)
    return str_value[:max_show] + '*' * (len(str_value) - max_show)


def main():
    """
    测试 main 函数：打印配置管理器的相关信息
    用于调试和验证配置加载情况
    """
    print("=" * 80)
    print("📋 Config Manager Test Report")
    print("=" * 80)
    
    # 1. 环境信息
    print("\n🔧 Environment Information:")
    print(f"  - IS_DATABRICKS: {IS_DATABRICKS}")
    print(f"  - DEFAULT_ENV_MODE: {DEFAULT_ENV_MODE}")
    
    # 显示环境变量的原始值（在 init_env_mode 之前）
    env_mode_before_init = os.getenv('ENV_MODE')
    print(f"  - ENV_MODE (env var, before init): {env_mode_before_init if env_mode_before_init else 'Not set'}")
    
    env_mode = get_env_mode()
    print(f"  - Current ENV_MODE (resolved): {env_mode}")
    
    # 2. 项目根目录
    print("\n📁 Project Root:")
    project_root = _find_project_root()
    print(f"  - Project Root: {project_root}")
    print(f"  - Current Working Directory: {os.getcwd()}")
    
    # 3. 配置文件路径
    print("\n📄 Configuration Files:")
    variables_path = os.path.join(project_root, 'config', 'variables.json')
    dag_paths_path = os.path.join(project_root, 'config', 'dag_id_to_s3_paths.json')
    print(f"  - variables.json: {variables_path}")
    print(f"    Exists: {os.path.exists(variables_path)}")
    print(f"  - dag_id_to_s3_paths.json: {dag_paths_path}")
    print(f"    Exists: {os.path.exists(dag_paths_path)}")
    
    # 4. S3 配置（隐藏敏感信息）
    print("\n☁️  S3 Configuration:")
    try:
        s3_config = get_s3_config()
        print(f"  - Bucket: {s3_config.get('bucket', 'N/A')}")
        print(f"  - AWS Key: {_mask_sensitive_info(s3_config.get('aws_key', 'N/A'))}")
        print(f"  - AWS Secret: {_mask_sensitive_info(s3_config.get('aws_secret', 'N/A'))}")
        print(f"  - Config Keys: {list(s3_config.keys())}")
    except Exception as e:
        print(f"  ❌ Failed to load S3 config: {e}")
    
    # 5. DAG S3 路径配置
    print("\n🗺️  DAG S3 Path Configuration:")
    try:
        dag_path_config = get_dag_s3_path_config()
        print(f"  - Total DAGs configured: {len(dag_path_config)}")
        if dag_path_config:
            print(f"  - DAG IDs: {list(dag_path_config.keys())[:10]}")  # 只显示前10个
            if len(dag_path_config) > 10:
                print(f"    ... and {len(dag_path_config) - 10} more")
            
            # 测试几个示例 DAG 的路径解析
            print("\n  📍 Example DAG Path Resolution:")
            test_dag_ids = list(dag_path_config.keys())[:3]  # 测试前3个
            test_ds = "2024-01-15"
            for dag_id in test_dag_ids:
                s3_path = get_s3_path_for_dag(dag_id, exc_ds=test_ds)
                config_value = dag_path_config[dag_id]
                config_type = "dict (env-specific)" if isinstance(config_value, dict) else "string (shared)"
                print(f"    - {dag_id}:")
                print(f"        Config Type: {config_type}")
                if isinstance(config_value, dict):
                    print(f"        Available Envs: {list(config_value.keys())}")
                print(f"        Resolved Path ({env_mode}): {s3_path}")
    except Exception as e:
        print(f"  ❌ Failed to load DAG path config: {e}")
    
    # 6. 测试配置获取（示例）
    print("\n🔑 Configuration Loading Test:")
    test_configs = ['env', 'aws_s3_prod', 'aws_s3_staging']
    for config_name in test_configs:
        try:
            config = get_secret_config(config_name)
            if isinstance(config, dict):
                print(f"  ✅ {config_name}: Loaded (keys: {list(config.keys())})")
                # 如果是 env 配置，显示部分信息
                if config_name == 'env' and 'feishu_botid' in config:
                    print(f"      feishu_botid: {_mask_sensitive_info(config.get('feishu_botid'))}")
            else:
                print(f"  ✅ {config_name}: Loaded (type: {type(config).__name__})")
        except Exception as e:
            print(f"  ❌ {config_name}: Failed - {str(e)[:100]}")
    
    # 7. 环境模式测试
    print("\n🧪 Environment Mode Test:")
    print(f"  - Current Mode: {env_mode}")
    print(f"  - S3 Config Source: ", end="")
    if env_mode == 'prod':
        print("aws_s3_prod")
    else:
        print("aws_s3_staging")
    
    print("\n" + "=" * 80)
    print("✅ Test Complete")
    print("=" * 80)


if __name__ == "__main__":
    # 配置日志级别，让测试输出更清晰
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )
    main()
