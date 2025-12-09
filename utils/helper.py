import base64
import gzip
import io
import json
import os
import logging
import requests
import pandas

# 假设 databricks 环境中有 boto3
import boto3

# 引入适配后的配置管理
from .config_manager import get_s3_config, get_secret_config, get_env_mode, build_s3_path

_AD_TYPE_INCOME = "income"
_AD_TYPE_SPEND = "spend"
_AD_TYPE_IAP = "iap"
_AD_TYPE_SPEND_MONITOR = "spend_monitor"
_AD_TYPE_ATTRIBUTE = "attribution"
_DATA_BASE_PATH = None

# 根据环境模式确定数据根目录
# 注意：在 Databricks Runtime 11.3+ 中，/Volumes/ 是推荐的存储位置，支持大文件
# 但为了兼容性，我们先使用 Workspace 路径，但要注意文件大小限制
_env_mode = get_env_mode()
# if _env_mode == 'prod':
#     # Prod 模式下通常不需要本地落地文件，除非为了调试
#     # 如果为了统一逻辑，可以设置为 Workspace 路径
#     _DATA_BASE_PATH = None 
# else:
_DATA_BASE_PATH = os.path.join(os.getcwd(), "data_output")

def get_cfg(cfg_name: str):
    """
    获取配置，优先从 Databricks Secrets 获取，其次环境变量，最后 variables.json
    """
    # 特殊处理：如果是 'env' 配置，包含了 botid 等信息
    if cfg_name == 'env':
        return get_secret_config('env')

    # 尝试直接获取同名 secret (例如 'appsflyer')
    return get_secret_config(cfg_name)

def upload_data_to_s3(data: bytes, s3_subpath: str, exc_ds: str = None, filename: str = None):
    """
    直接从内存数据上传到 S3（压缩为 Gzip）

    Args:
        data: 原始数据（bytes）
        s3_subpath: S3 子路径，如 'spend/aarki', 'iap/amazon'
        exc_ds: 执行日期 (YYYY-MM-DD)
        filename: 可选的文件名
    """
    if not data:
        logging.warning("⚠️ No data to upload")
        return

    env_mode = get_env_mode()

    # dev 模式不上传 S3
    if env_mode == 'dev':
        logging.info(f"🔧 [DEV MODE] Skip uploading data to S3")
        return

    # 构建 S3 路径（使用新的 build_s3_path 函数）
    s3_path_template = build_s3_path(s3_subpath, exc_ds)

    # 生成文件名
    if not filename:
        filename = f"{s3_subpath.replace('/', '_')}_{exc_ds}.jsonl"

    s3_path = f"{s3_path_template}/{filename}"
    s3_path_gz = s3_path + '.gz'

    # 压缩数据
    bio = io.BytesIO()
    with gzip.GzipFile(fileobj=bio, mode='wb') as f:
        f.write(data)
    compressed_data = bio.getvalue()

    try:
        cfg = get_s3_config()
        aws_key = cfg.get('aws_key')
        aws_secret = cfg.get('aws_secret')
        bucket = cfg.get('bucket')

        if not all([aws_key, aws_secret, bucket]):
            raise ValueError(f"Incomplete S3 config for {env_mode} mode")

        session = boto3.Session(
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
        )
        s3 = session.resource('s3')
        print(f"📤 Uploading to s3://{bucket}/{s3_path_gz} [{env_mode.upper()}]")

        s3.Bucket(bucket).put_object(Key=s3_path_gz, Body=compressed_data)
        logging.info(f"✅ Successfully uploaded to s3://{bucket}/{s3_path_gz}")

    except Exception as e:
        error_msg = f"Failed to upload data to S3: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

def _get_s3_path(file_path: str, dag_id: str = None, exc_ds: str = None):
    """
    (已弃用) 旧的 S3 路径生成逻辑，仅作 fallback
    """
    # 简单保留原有逻辑作为 fallback，实际应该都走 dag_id_to_s3_paths.json
    relative_path = file_path.replace(_DATA_BASE_PATH, "").lstrip("/")
    # 简单替换：把 data_output 路径转为 reports 路径
    # 这里只是示例，实际已被 upload_data_to_s3 取代
    return f"reports/{relative_path}"
    
def _get_read_csv_error_handling_kwargs():
    """根据 Pandas 版本返回正确的错误处理参数"""
    try:
        pandas_version = tuple(map(int, pandas.__version__.split('.')[:2]))
        if pandas_version >= (1, 3):
            return {'on_bad_lines': 'skip'}
        else:
            return {'error_bad_lines': False}
    except:
        # 默认使用新版参数
        return {'on_bad_lines': 'skip'}

def convert_df_to_jsonl(df):
    """
    将 DataFrame 转换为多行 JSON (JSONL) 格式
    
    Args:
        df: pandas DataFrame
    
    Returns:
        bytes: JSONL 格式的数据（UTF-8 编码）
    """
    # 确保日期相关列保持为字符串类型（避免 to_json 转换为时间戳）
    date_columns = ['date', 'report_date', 'start_ds', 'end_ds', 'exc_ds']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # 使用 json.dumps 手动构建 JSONL，确保日期字段保持为字符串
    lines = []
    for _, row in df.iterrows():
        record = row.to_dict()
        for col in date_columns:
            if col in record:
                record[col] = str(record[col])
        lines.append(json.dumps(record, ensure_ascii=False))
    
    return '\n'.join(lines).encode('utf-8')

def fetch_report(ad_network: str, ad_type: str, exc_ds: str, start_ds=None, end_ds=None, report_ds=None, custom=None, **req_opt):
    req_opt['timeout'] = req_opt.get('timeout', 1800)
    
    # 开启流式下载，防止大文件撑爆内存
    req_opt['stream'] = True
    
    resp = requests.get(**req_opt)
    
    if resp.status_code not in [200, 204, 422]:
        raise RuntimeError(
            f'Failed to download {ad_network} report for {exc_ds}: {resp.status_code} {resp.text[:200]}' # 只打印前200字符，防止报错信息过长
        )
    
    # 如果是 204 (No Content)，直接返回空文件
    if resp.status_code == 204:
        return save_report(ad_network=ad_network, ad_type=ad_type, report_content=b"", exc_ds=exc_ds, start_ds=start_ds, end_ds=end_ds, report_ds=report_ds, custom=custom)

    # 直接传递 response 对象给 save_report 进行流式写入
    return save_report(ad_network=ad_network, ad_type=ad_type, response=resp, exc_ds=exc_ds, start_ds=start_ds, end_ds=end_ds, report_ds=report_ds, custom=custom)

def save_report(ad_network: str, ad_type: str, report=None, response=None, report_content=None, exc_ds=None, start_ds=None, end_ds=None, report_ds=None, custom=None):
    """
    保存报告数据并根据环境模式自动处理上传（支持流式处理大文件）：

    - dev: 保存完整数据到本地，不上传 S3
    - staging: 保存 5MB 预览到本地，完整数据上传 S3
    - prod: 不保存本地，完整数据直接上传 S3
    """
    env_mode = get_env_mode()

    # 生成文件名
    if not report_ds and not custom:
        filename = f"{ad_network}_{start_ds}_to_{end_ds}"
    elif report_ds:
        filename = f"{ad_network}_{report_ds}"
    elif custom:
        filename = f"{ad_network}_{custom}_{start_ds}_to_{end_ds}"

    # 判断是否为流式数据（response 对象）
    is_streaming = response is not None
    
    # 对于流式数据（大文件），使用分块处理避免 OOM
    if is_streaming:
        return _save_report_streaming(
            ad_network=ad_network,
            ad_type=ad_type,
            response=response,
            filename=filename,
            exc_ds=exc_ds,
            env_mode=env_mode
        )
    
    # 对于非流式数据（小文件），使用原有逻辑
    upload_data = b''
    
    try:
        # 收集原始数据
        if report_content is not None:
            raw_data = report_content
        elif report:
            raw_data = report.encode('utf-8')
        else:
            raw_data = b''

        # 转换为 JSONL
        if raw_data:
            try:
                from io import StringIO
                text_data = raw_data.decode('utf-8')
                
                # 尝试检测是否为 JSON 格式（以 [ 或 { 开头）
                is_json = False
                text_stripped = text_data.strip()
                if text_stripped.startswith('[') or text_stripped.startswith('{'):
                    try:
                        # 尝试解析为 JSON
                        json_data = json.loads(text_data)
                        if isinstance(json_data, list):
                            # JSON 数组：转换为 DataFrame 再转 JSONL
                            df = pandas.DataFrame(json_data)
                            is_json = True
                        elif isinstance(json_data, dict):
                            # 单个 JSON 对象：转换为单行 DataFrame
                            df = pandas.DataFrame([json_data])
                            is_json = True
                    except (json.JSONDecodeError, ValueError):
                        pass  # 不是有效的 JSON，继续尝试 CSV
                
                if not is_json:
                    # 尝试作为 CSV 读取
                    csv_io = StringIO(text_data)
                    try:
                        df = pandas.read_csv(csv_io, on_bad_lines='skip')
                    except TypeError:
                        # Pandas < 1.3.0 不支持 on_bad_lines
                        csv_io.seek(0)  # 重置读取位置
                        df = pandas.read_csv(csv_io, error_bad_lines=False)
                
                # 确保日期列保持为字符串
                date_columns = ['date', 'report_date', 'start_ds', 'end_ds', 'exc_ds']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str)
                
                upload_data = convert_df_to_jsonl(df)
                format_type = "JSON" if is_json else "CSV"
                print(f"✅ Converted {format_type} to JSONL format ({len(df)} rows)")
            except Exception as e:
                logging.warning(f"⚠️ Failed to convert to JSONL: {e}, saving as original")
                upload_data = raw_data

    except Exception as e:
        logging.error(f"❌ Error processing data: {e}")
        raise

    # 根据环境模式处理
    if env_mode == 'dev':
        # dev: 保存完整数据到本地
        if _DATA_BASE_PATH is None:
            raise ValueError("DATA_BASE_PATH not set for dev mode")

        file_path = f'{_DATA_BASE_PATH}/{ad_type}/{ad_network}/{exc_ds}/'
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        full_path = f'{file_path}{filename}'
        print(f"Saving to: {full_path}")

        with open(full_path, 'wb') as f:
            f.write(upload_data)
        print(f"✅ Saved complete data ({len(upload_data)} bytes)")
        return full_path

    elif env_mode == 'staging':
        # staging: 保存 5MB 预览到本地 + 上传完整数据到 S3
        if _DATA_BASE_PATH is None:
            raise ValueError("DATA_BASE_PATH not set for staging mode")

        file_path = f'{_DATA_BASE_PATH}/{ad_type}/{ad_network}/{exc_ds}/'
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        preview_path = f'{file_path}{filename}.preview'
        print(f"Saving preview to: {preview_path}")

        # 保存 5MB 预览
        preview_size = min(5 * 1024 * 1024, len(upload_data))
        with open(preview_path, 'wb') as f:
            f.write(upload_data[:preview_size])
        print(f"✅ Saved preview ({preview_size} bytes)")

        # 同时上传完整数据到 S3
        s3_subpath = f"{ad_type}/{ad_network}"  # 例如: spend/aarki, iap/amazon
        upload_data_to_s3(upload_data, s3_subpath, exc_ds, filename)

        # 返回本地 preview 路径，便于上游做本地预览
        return preview_path

    else:  # prod
        # prod: 直接上传完整数据到 S3
        s3_subpath = f"{ad_type}/{ad_network}"  # 例如: spend/aarki, iap/amazon
        upload_data_to_s3(upload_data, s3_subpath, exc_ds, filename)
        return None

def _save_report_streaming(ad_network: str, ad_type: str, response, filename: str, exc_ds: str, env_mode: str):
    """
    流式处理大文件：
    1. 先将原始数据流式下载到临时文件（避免 TextIOWrapper 包装网络流的不稳定性）
    2. 分块读取临时 CSV 文件，转换为 JSONL
    3. 流式写入本地/S3（S3 使用临时文件缓存压缩数据）
    """
    import tempfile
    import shutil
    
    # 构建 S3 子路径
    s3_subpath = f"{ad_type}/{ad_network}"  # 例如: spend/aarki, iap/amazon
    
    # 获取 S3 配置（如果需要上传）
    s3_config = None
    if env_mode in ['staging', 'prod']:
        s3_config = get_s3_config()
        if not s3_config:
            raise ValueError(f"Cannot get S3 config for {env_mode} mode")
    
    # 准备本地文件路径（如果需要保存）
    local_file = None
    preview_file = None
    
    if _DATA_BASE_PATH is None and env_mode in ['dev', 'staging']:
         raise ValueError("DATA_BASE_PATH not set")
         
    # 确保目录存在
    if env_mode in ['dev', 'staging']:
        file_path = f'{_DATA_BASE_PATH}/{ad_type}/{ad_network}/{exc_ds}/'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
            
        if env_mode == 'dev':
            # Dev 模式：只保存完整本地文件
            local_file = f'{file_path}{filename}'
        elif env_mode == 'staging':
            # Staging 模式：只保存 Preview 文件
            preview_file = f'{file_path}{filename}.preview'
    
    # 准备 S3 上传路径
    s3_bucket = None
    s3_key = None
    s3_client = None
    
    if env_mode in ['staging', 'prod']:
        # 使用 build_s3_path 构建路径
        s3_path_template = build_s3_path(s3_subpath, exc_ds)
        s3_key = f"{s3_path_template}/{filename}.gz"
        
        session = boto3.Session(
            aws_access_key_id=s3_config['aws_key'],
            aws_secret_access_key=s3_config['aws_secret'],
        )
        s3_client = session.client('s3')
        s3_bucket = s3_config['bucket']
        print(f"📤 Will upload to s3://{s3_bucket}/{s3_key} [{env_mode.upper()}]")
    
    # 资源句柄初始化
    raw_temp_file = tempfile.TemporaryFile(mode='w+b') # 存储原始下载数据
    s3_temp_file = None # 存储压缩后的上传数据
    s3_gzip_file = None
    local_f = None
    preview_f = None

    try:
        # Step 1: 下载原始数据到临时文件
        # 使用 shutil.copyfileobj 高效传输，避免手动 chunk 循环
        print("⬇️  Downloading stream to temporary file...")
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, raw_temp_file)
        raw_temp_file.seek(0) # 重置指针到文件开头
        print("✅ Download complete.")

        # Step 2: 准备输出流
        if local_file:
            local_f = open(local_file, 'wb')
        
        if preview_file:
            preview_f = open(preview_file, 'wb')
            
        if s3_bucket:
            s3_temp_file = tempfile.TemporaryFile(mode='w+b')
            s3_gzip_file = gzip.GzipFile(fileobj=s3_temp_file, mode='wb')

        # Step 3: 分块处理
        print("⏳ Starting CSV parsing and processing...")
        chunk_size = 10000  # 减小到 1万行，提高响应速度
        total_rows = 0
        preview_size = 5 * 1024 * 1024  # 5MB
        preview_written = 0
        chunk_count = 0
        
        date_columns = ['date', 'report_date', 'start_ds', 'end_ds', 'exc_ds']
        
        # 从临时文件读取 CSV，使用本地文件句柄
        pandas_version = tuple(map(int, pandas.__version__.split('.')[:2]))
        read_csv_kwargs = {'chunksize': chunk_size}
        if pandas_version >= (1, 3):
            read_csv_kwargs['on_bad_lines'] = 'skip'
        else:
            read_csv_kwargs['error_bad_lines'] = False
        for chunk_df in pandas.read_csv(raw_temp_file, **read_csv_kwargs):
            chunk_count += 1
            if chunk_count % 10 == 1: # 每10个chunk打印一次，避免日志过多，但首个chunk会打印
                print(f"   Processing chunk {chunk_count} (rows so far: {total_rows})...")

            # 确保日期列保持为字符串
            for col in date_columns:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str)
            
            # 转换为 JSONL
            chunk_jsonl = convert_df_to_jsonl(chunk_df)
            total_rows += len(chunk_df)
            
            # 写入本地完整文件
            if local_f:
                local_f.write(chunk_jsonl)
            
            # 写入预览文件
            if preview_f and preview_written < preview_size:
                remaining = preview_size - preview_written
                if len(chunk_jsonl) <= remaining:
                    preview_f.write(chunk_jsonl)
                    preview_written += len(chunk_jsonl)
                else:
                    preview_f.write(chunk_jsonl[:remaining])
                    preview_written = preview_size
            
            # 写入 S3 压缩流
            if s3_gzip_file:
                s3_gzip_file.write(chunk_jsonl)
        
        print(f"✅ CSV parsing complete. Total rows: {total_rows}")
        
        # Step 4: 完成写入并上传
        if s3_gzip_file:
            s3_gzip_file.close() # 必须先关闭 gzip 以写入 footer
            s3_gzip_file = None  # 标记已关闭
            s3_temp_file.seek(0) # 重置指针
            
            print(f"📤 Uploading to S3 (streaming from temp file)...")
            s3_client.upload_fileobj(
                Fileobj=s3_temp_file,
                Bucket=s3_bucket,
                Key=s3_key
            )
            logging.info(f"✅ Successfully uploaded to s3://{s3_bucket}/{s3_key}")
        
        print(f"✅ Processed {total_rows} rows (streaming mode)")
        
    finally:
        # 关闭所有文件句柄
        if local_f: local_f.close()
        if preview_f: preview_f.close()
        if s3_gzip_file: 
            try: s3_gzip_file.close()
            except: pass
        if s3_temp_file: s3_temp_file.close() # 会自动删除
        if raw_temp_file: raw_temp_file.close() # 会自动删除
    
    # 返回路径
    if env_mode == 'dev':
        print(f"✅ Saved complete data to: {local_file}")
        return local_file
    elif env_mode == 'staging':
        print(f"✅ Saved preview ({preview_written} bytes) to: {preview_file}")
        return preview_file
    else:  # prod
        return None

# 简化的飞书发送，去除 DingDing
def send_feishu(bot_access_token, title, infos):
    # 迁移测试安全模式：只打印日志，不发送真实 HTTP 请求
    # if not bot_access_token:
    #     return
    # url = f'https://open.feishu.cn/open-apis/bot/v2/hook/{bot_access_token}'

    # 简化版 content 构造
    content_text = "\n".join(infos)
    
    # 模拟发送，只在 Driver 日志中打印
    print(f"--- [MOCKED FEISHU NOTIFICATION] ---")
    print(f"Title: {title}")
    print(f"Content: {content_text}")
    print(f"Token (Hidden): {bot_access_token[:5]}...")
    print(f"------------------------------------")
    return

    # 以下为真实发送逻辑，暂时屏蔽
    # data = {
    #     "msg_type": "text",
    #     "content": {
    #         "text": f"{title}\n\n{content_text}"
    #     }
    # }

    # try:
    #     requests.post(url, json=data)
    # except Exception as e:
    #     print(f"Feishu send error: {e}")

def failure_callback(exception_msg, job_name):
    """
    Databricks 专用的失败回调
    """
    try:
        secret_env = get_cfg('env')
        feishu_botid = secret_env.get('feishu_botid') if secret_env else None
        if feishu_botid:
            send_feishu(
                feishu_botid, 
                'DATABRICKS JOB FAILURE',
                [
                    f'**JOB:** {job_name}',
                    f'**ERROR:** {exception_msg}'
                ]
            )
        else:
            print(f"⚠️ Cannot send failure notification: Missing config of env. Please set env var SECRET_ENV or secret secret_env")
            print(f"Job Failed: {job_name}")
            print(f"Error: {exception_msg}")
    except Exception as e:
        print(f"⚠️ Failed to send failure notification: {e}")
        print(f"Job Failed: {job_name}")
        print(f"Error: {exception_msg}")
