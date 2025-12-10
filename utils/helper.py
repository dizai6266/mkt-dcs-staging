import gzip
import io
import json
import os
import logging
import requests
import pandas
import boto3
from .config_manager import get_s3_config, get_secret_config, get_env_mode, build_s3_path
from .data_parser import (
    DataFormat,
    detect_format,
    convert_to_jsonl,
    StreamingParser,
    records_to_jsonl,
)

_AD_TYPE_INCOME = "income"
_AD_TYPE_SPEND = "spend"
_AD_TYPE_IAP = "iap"
_AD_TYPE_SPEND_MONITOR = "spend_monitor"
_AD_TYPE_ATTRIBUTE = "attribution"

_DATA_BASE_PATH = None
_env_mode = get_env_mode()
_DATA_BASE_PATH = os.path.join(os.getcwd(), "data_output")


# ============================================================================
# 配置相关函数
# ============================================================================

def get_cfg(cfg_name: str):
    """获取配置"""
    if cfg_name == 'env':
        return get_secret_config('env')
    return get_secret_config(cfg_name)


# ============================================================================
# 文件保存相关函数（内部使用）
# ============================================================================

def _save_preview_by_lines(jsonl_content: str, preview_path: str, max_size: int = 5 * 1024 * 1024):
    """
    按行截断保存 preview，确保不会截断到 JSON 中间
    """
    lines = jsonl_content.split('\n')
    preview_lines = []
    preview_size = 0
    
    for line in lines:
        line_bytes = len((line + '\n').encode('utf-8'))
        if preview_size + line_bytes > max_size:
            break
        preview_lines.append(line)
        preview_size += line_bytes
    
    with open(preview_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(preview_lines))
    
    return preview_size, len(preview_lines)


# ============================================================================
# S3 上传相关函数
# ============================================================================

def upload_data_to_s3(data: bytes, s3_subpath: str, exc_ds: str = None, filename: str = None):
    """直接从内存数据上传到 S3（压缩为 Gzip）"""
    if not data:
        logging.warning("⚠️ No data to upload")
        return
    
    env_mode = get_env_mode()
    
    if env_mode == 'dev':
        logging.info(f"🔧 [DEV MODE] Skip uploading data to S3")
        return
    
    s3_path_template = build_s3_path(s3_subpath, exc_ds)
    
    if not filename:
        filename = f"{s3_subpath.replace('/', '_')}_{exc_ds}.jsonl"
    
    # 确保文件名有 .gz 后缀
    if not filename.endswith('.gz'):
        s3_path_gz = f"{s3_path_template}/{filename}.gz"
    else:
        s3_path_gz = f"{s3_path_template}/{filename}"
    
    # 压缩数据
    bio = io.BytesIO()
    with gzip.GzipFile(fileobj=bio, mode='wb') as f:
        f.write(data if isinstance(data, bytes) else data.encode('utf-8'))
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


# ============================================================================
# 报告获取与保存相关函数
# ============================================================================

def fetch_report(ad_network: str, ad_type: str, exc_ds: str, start_ds=None, end_ds=None, report_ds=None, custom=None, **req_opt):
    """获取报告并保存"""
    req_opt['timeout'] = req_opt.get('timeout', 1800)
    req_opt['stream'] = True
    
    resp = requests.get(**req_opt)
    
    if resp.status_code not in [200, 204, 422]:
        raise RuntimeError(
            f'Failed to download {ad_network} report for {exc_ds}: {resp.status_code} {resp.text[:200]}'
        )
    
    if resp.status_code == 204:
        return save_report(
            ad_network=ad_network, ad_type=ad_type, report_content=b"",
            exc_ds=exc_ds, start_ds=start_ds, end_ds=end_ds, report_ds=report_ds, custom=custom
        )
    
    return save_report(
        ad_network=ad_network, ad_type=ad_type, response=resp,
        exc_ds=exc_ds, start_ds=start_ds, end_ds=end_ds, report_ds=report_ds, custom=custom
    )


def save_report(
    ad_network: str, 
    ad_type: str, 
    report=None, 
    response=None, 
    report_content=None, 
    exc_ds=None, 
    start_ds=None, 
    end_ds=None, 
    report_ds=None, 
    custom=None,
    data_format=None
):
    """
    保存报告数据并根据环境模式自动处理上传
    
    自动识别数据格式：CSV, JSON, JSONL, API 响应（如 {"code":200,"results":[...]}）
    
    文件名生成规则: {ad_network}_{date_range}[_{custom}]
    """
    env_mode = get_env_mode()
    
    # --- [Filename Generation Logic] ---
    # 1. 确定日期部分
    if start_ds and end_ds:
        date_part = f"{start_ds}_to_{end_ds}"
    elif report_ds:
        date_part = f"{report_ds}"
    else:
        date_part = f"{exc_ds}"
        
    # 2. 拼接基础文件名: channel_YYYY-mm-dd_to_YYYY-mm-dd
    filename_base = f"{ad_network}_{date_part}"
    
    # 3. 如果有 custom (account_id)，追加到最后: ...[_account_id]
    if custom:
        filename = f"{filename_base}_{custom}"
    else:
        filename = filename_base
    
    # 判断是否为流式数据（response 对象）
    is_streaming = response is not None
    
    if is_streaming:
        return _save_report_streaming(
            ad_network=ad_network,
            ad_type=ad_type,
            response=response,
            filename=filename,
            exc_ds=exc_ds,
            env_mode=env_mode
        )
    
    # === 非流式数据处理 ===
    
    # 收集原始数据
    if report_content is not None:
        raw_data = report_content if isinstance(report_content, bytes) else report_content.encode('utf-8')
    elif report:
        raw_data = report if isinstance(report, bytes) else report.encode('utf-8')
    else:
        raw_data = b''
    
    if not raw_data:
        logging.warning("⚠️ No data to save")
        return None
    
    # 使用 data_parser 模块转换为 JSONL
    try:
        text_data = raw_data.decode('utf-8')
        
        # 转换 data_format 参数（如果有）
        fmt = None
        if data_format:
            try:
                fmt = DataFormat(data_format)
            except ValueError:
                fmt = None
        
        jsonl_content, row_count, detected_format = convert_to_jsonl(text_data, fmt)
        
        if detected_format == DataFormat.UNKNOWN:
            logging.warning("⚠️ Could not convert to JSONL, saving as original")
            jsonl_content = text_data
        else:
            print(f"✅ Converted {detected_format.value} to JSONL format ({row_count} rows)")
        
    except Exception as e:
        logging.error(f"❌ Error converting data: {e}")
        raise
    
    # 验证 JSONL 格式
    if jsonl_content:
        first_line = jsonl_content.split('\n')[0].strip()
        if first_line:
            try:
                json.loads(first_line)
            except json.JSONDecodeError as e:
                logging.error(f"❌ Invalid JSONL format after conversion: {e}")
                logging.error(f"   First 200 chars: {first_line[:200]}")
                raise ValueError(f"Invalid JSONL format: {e}")
    
    # 根据环境模式处理
    upload_data = jsonl_content.encode('utf-8')
    
    if env_mode == 'dev':
        # dev: 保存完整数据到本地
        if _DATA_BASE_PATH is None:
            raise ValueError("DATA_BASE_PATH not set for dev mode")
        
        file_path = f'{_DATA_BASE_PATH}/{ad_type}/{ad_network}/{exc_ds}/'
        os.makedirs(file_path, exist_ok=True)
        
        full_path = f'{file_path}{filename}.jsonl'
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(jsonl_content)
        
        print(f"✅ Saved complete data to: {full_path} ({len(upload_data)} bytes)")
        return full_path
    
    elif env_mode == 'staging':
        # staging: 保存 5MB 预览到本地 + 上传完整数据到 S3
        if _DATA_BASE_PATH is None:
            raise ValueError("DATA_BASE_PATH not set for staging mode")
        
        file_path = f'{_DATA_BASE_PATH}/{ad_type}/{ad_network}/{exc_ds}/'
        os.makedirs(file_path, exist_ok=True)
        
        preview_path = f'{file_path}{filename}.preview'
        
        # 按行截断保存 preview
        preview_size, preview_rows = _save_preview_by_lines(jsonl_content, preview_path)
        print(f"✅ Saved preview: {preview_path} ({preview_size} bytes, {preview_rows} rows)")
        
        # 上传完整数据到 S3
        s3_subpath = f"{ad_type}/{ad_network}"
        upload_data_to_s3(upload_data, s3_subpath, exc_ds, filename)
        
        return preview_path
    
    else:  # prod
        # prod: 直接上传完整数据到 S3
        s3_subpath = f"{ad_type}/{ad_network}"
        upload_data_to_s3(upload_data, s3_subpath, exc_ds, filename)
        return None


def _save_report_streaming(ad_network: str, ad_type: str, response, filename: str, exc_ds: str, env_mode: str):
    """
    流式处理大文件，自动识别数据格式（CSV/JSON/JSONL/API响应）
    """
    import tempfile
    import shutil
    
    s3_subpath = f"{ad_type}/{ad_network}"
    
    s3_config = None
    if env_mode in ['staging', 'prod']:
        s3_config = get_s3_config()
        if not s3_config:
            raise ValueError(f"Cannot get S3 config for {env_mode} mode")
    
    local_file = None
    preview_file = None
    
    if _DATA_BASE_PATH is None and env_mode in ['dev', 'staging']:
        raise ValueError("DATA_BASE_PATH not set")
    
    if env_mode in ['dev', 'staging']:
        file_path = f'{_DATA_BASE_PATH}/{ad_type}/{ad_network}/{exc_ds}/'
        os.makedirs(file_path, exist_ok=True)
        
        if env_mode == 'dev':
            local_file = f'{file_path}{filename}.jsonl'
        elif env_mode == 'staging':
            preview_file = f'{file_path}{filename}.preview'
    
    s3_bucket = None
    s3_key = None
    s3_client = None
    
    if env_mode in ['staging', 'prod']:
        s3_path_template = build_s3_path(s3_subpath, exc_ds)
        s3_key = f"{s3_path_template}/{filename}.gz"
        
        session = boto3.Session(
            aws_access_key_id=s3_config['aws_key'],
            aws_secret_access_key=s3_config['aws_secret'],
        )
        s3_client = session.client('s3')
        s3_bucket = s3_config['bucket']
        print(f"📤 Will upload to s3://{s3_bucket}/{s3_key} [{env_mode.upper()}]")
    
    raw_temp_file = tempfile.TemporaryFile(mode='w+b')
    s3_temp_file = None
    s3_gzip_file = None
    local_f = None
    preview_lines = []
    preview_size = 0
    max_preview_size = 5 * 1024 * 1024
    
    try:
        # 1. 下载响应内容到临时文件
        print("⬇️  Downloading stream to temporary file...")
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, raw_temp_file)
        raw_temp_file.seek(0)
        print("✅ Download complete.")
        
        # 2. 使用 StreamingParser 自动检测格式并解析
        parser = StreamingParser(chunk_size=10000)
        data_format = parser.detect_format_from_file(raw_temp_file)
        raw_temp_file.seek(0)
        
        if local_file:
            local_f = open(local_file, 'w', encoding='utf-8')
        
        if s3_bucket:
            s3_temp_file = tempfile.TemporaryFile(mode='w+b')
            s3_gzip_file = gzip.GzipFile(fileobj=s3_temp_file, mode='wb')
        
        print(f"⏳ Starting data parsing and processing...")
        total_rows = 0
        chunk_count = 0
        
        # 3. 使用 StreamingParser 流式解析
        for records, batch_size in parser.parse_file(raw_temp_file, data_format):
            chunk_count += 1
            if chunk_count % 10 == 1:
                print(f"   Processing chunk {chunk_count} (rows so far: {total_rows})...")
            
            # 转换记录为 JSONL 行
            chunk_lines = []
            for record in records:
                line = json.dumps(record, ensure_ascii=False)
                chunk_lines.append(line)
                
                # 收集 preview 行
                if preview_size < max_preview_size:
                    line_size = len((line + '\n').encode('utf-8'))
                    if preview_size + line_size <= max_preview_size:
                        preview_lines.append(line)
                        preview_size += line_size
            
            chunk_jsonl = '\n'.join(chunk_lines) + '\n'
            total_rows += batch_size
            
            # 写入本地完整文件
            if local_f:
                local_f.write(chunk_jsonl)
            
            # 写入 S3 压缩流
            if s3_gzip_file:
                s3_gzip_file.write(chunk_jsonl.encode('utf-8'))
        
        print(f"✅ Data parsing complete. Total rows: {total_rows}")
        
        # 保存 preview 文件
        if preview_file:
            with open(preview_file, 'w', encoding='utf-8') as pf:
                pf.write('\n'.join(preview_lines))
            print(f"✅ Saved preview: {preview_file} ({preview_size} bytes, {len(preview_lines)} rows)")
        
        # 完成 S3 上传
        if s3_gzip_file:
            s3_gzip_file.close()
            s3_gzip_file = None
            s3_temp_file.seek(0)
            
            print(f"📤 Uploading to S3...")
            s3_client.upload_fileobj(
                Fileobj=s3_temp_file,
                Bucket=s3_bucket,
                Key=s3_key
            )
            logging.info(f"✅ Successfully uploaded to s3://{s3_bucket}/{s3_key}")
        
        print(f"✅ Processed {total_rows} rows (streaming mode)")
        
    finally:
        if local_f:
            local_f.close()
        if s3_gzip_file:
            try:
                s3_gzip_file.close()
            except:
                pass
        if s3_temp_file:
            s3_temp_file.close()
        if raw_temp_file:
            raw_temp_file.close()
    
    if env_mode == 'dev':
        print(f"✅ Saved complete data to: {local_file}")
        return local_file
    elif env_mode == 'staging':
        return preview_file
    else:
        return None


# ============================================================================
# 通知相关函数
# ============================================================================

def send_feishu(bot_access_token, title, infos):
    """简化的飞书发送"""
    content_text = "\n".join(infos)
    print(f"--- [MOCKED FEISHU NOTIFICATION] ---")
    print(f"Title: {title}")
    print(f"Content: {content_text}")
    print(f"Token (Hidden): {bot_access_token[:5]}...")
    print(f"------------------------------------")
    return


def failure_callback(exception_msg, job_name):
    """Databricks 专用的失败回调"""
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
            print(f"⚠️ Cannot send failure notification: Missing config")
            print(f"Job Failed: {job_name}")
            print(f"Error: {exception_msg}")
    except Exception as e:
        print(f"⚠️ Failed to send failure notification: {e}")
        print(f"Job Failed: {job_name}")
        print(f"Error: {exception_msg}")


# ============================================================================
# 数据验证与预览相关函数
# ============================================================================

def validate_and_preview_data(ad_type: str, ad_network: str):
    """
    在 staging 模式下扫描并预览 preview 文件
    
    该函数会：
    1. 扫描指定 ad_type 和 ad_network 下的所有 .preview 文件
    2. 手动逐行读取 JSONL 格式的 preview 文件（避免 Spark Arrow 类型转换问题）
    3. 显示预览数据的前 5 行
    
    Args:
        ad_type: 广告类型，如 'spend', 'income', 'iap'
        ad_network: 广告网络名称，如 'aarki', 'applovin'
    
    Returns:
        None（仅用于打印预览信息）
    """
    env_mode = get_env_mode()
    
    if env_mode != 'staging':
        print("⚠️ 非 staging 模式，跳过本地 preview。")
        return
    
    try:
        # 直接使用模块级别的 _DATA_BASE_PATH 变量
        base_root = _DATA_BASE_PATH or os.path.join(os.getcwd(), "data_output")
        preview_root = os.path.join(base_root, ad_type, ad_network)
        print(f"🔎 Scanning preview files under: {preview_root}")
        
        if not os.path.exists(preview_root):
            print(f"⚠️ Preview directory does not exist: {preview_root}")
            return
        
        # 查找所有 .preview 文件
        preview_files = []
        for root, dirs, files in os.walk(preview_root):
            for name in files:
                if name.endswith('.preview'):
                    preview_files.append(os.path.join(root, name))
        
        print(f"✅ Found {len(preview_files)} preview file(s)")
        
        # 预览每个文件
        for sample_file in preview_files:
            print(f"\n   Previewing: {sample_file}")
            try:
                # 手动逐行读取 JSONL，避免 Spark Arrow 类型转换问题
                records = []
                with open(sample_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                records.append(json.loads(line))
                            except json.JSONDecodeError as je:
                                print(f"   ⚠️  Skipping invalid JSON line: {je}")
                
                if records:
                    df = pandas.DataFrame(records)
                    try:
                        display(df.head(5))
                    except NameError:
                        print(df.head(5).to_string())
                    print(f"   Total rows: {len(df)}\n")
                else:
                    print(f"   ⚠️  No valid records found in preview file\n")
            except Exception as e:
                print(f"   ❌ Failed to read preview file: {e}")
    except Exception as e:
        print(f"❌ Preview scan error: {e}")
