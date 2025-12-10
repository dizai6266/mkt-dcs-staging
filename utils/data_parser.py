"""
数据格式自动识别与转换模块

支持的输入格式：
- CSV
- JSON Lines (JSONL)  
- JSON 数组 [...]
- 单个 JSON 对象 {...}
- API 响应包装格式 {"code": 200, "results": [...], "data": [...]}

所有格式统一转换为 JSONL 输出
"""

import io
import json
import logging
from enum import Enum
from typing import Iterator, Tuple, List, Any, Optional

import pandas


class DataFormat(Enum):
    """数据格式枚举"""
    CSV = "csv"
    JSONL = "jsonl"
    JSON_ARRAY = "json_array"
    JSON_OBJECT = "json_object"
    API_RESPONSE = "api_response"  # {"code": 200, "results": [...]}
    EMPTY = "empty"
    UNKNOWN = "unknown"


# ============================================================================
# 常见的 API 响应数据字段名（按优先级排序）
# ============================================================================
API_RESPONSE_DATA_KEYS = [
    'results',      # AppLovin: {"code": 200, "results": [...]}
    'data',         # 通用格式
    'items',        # 常见格式
    'records',      # 常见格式
    'rows',         # 常见格式
    'list',         # 常见格式
    'content',      # 某些 API 使用
]


# ============================================================================
# Pandas 兼容性处理
# ============================================================================

def _get_read_csv_kwargs() -> dict:
    """根据 Pandas 版本返回正确的错误处理参数"""
    try:
        pandas_version = tuple(map(int, pandas.__version__.split('.')[:2]))
        if pandas_version >= (1, 3):
            return {'on_bad_lines': 'skip'}
        else:
            return {'error_bad_lines': False}
    except:
        return {'on_bad_lines': 'skip'}


# ============================================================================
# 格式检测
# ============================================================================

def detect_format(text_data: str) -> DataFormat:
    """
    智能检测数据格式
    
    Args:
        text_data: 原始文本数据
        
    Returns:
        DataFormat: 检测到的数据格式
    """
    if not text_data or not text_data.strip():
        return DataFormat.EMPTY
    
    text_stripped = text_data.strip()
    
    # 1. 检查是否为 JSON Lines（每行一个 JSON 对象）
    first_line = text_stripped.split('\n')[0].strip()
    if first_line.startswith('{') and first_line.endswith('}'):
        try:
            json.loads(first_line)
            # 检查是否有多行，且每行都是有效 JSON
            lines = text_stripped.split('\n')
            if len(lines) > 1:
                valid_jsonl = True
                for line in lines[:5]:  # 只检查前 5 行
                    line = line.strip()
                    if line:
                        try:
                            obj = json.loads(line)
                            if not isinstance(obj, dict):
                                valid_jsonl = False
                                break
                        except json.JSONDecodeError:
                            valid_jsonl = False
                            break
                if valid_jsonl:
                    return DataFormat.JSONL
            else:
                # 只有一行，且是有效 JSON 对象
                return DataFormat.JSON_OBJECT
        except json.JSONDecodeError:
            pass
    
    # 2. 检查是否为 JSON 数组
    if text_stripped.startswith('['):
        try:
            data = json.loads(text_stripped)
            if isinstance(data, list):
                return DataFormat.JSON_ARRAY
        except json.JSONDecodeError:
            pass
    
    # 3. 检查是否为单个 JSON 对象（包括 API 响应）
    # 注意：即使是多行 JSON（格式化的），也应该尝试解析
    if text_stripped.startswith('{'):
        try:
            data = json.loads(text_stripped)
            if isinstance(data, dict):
                # 检查是否为 API 响应包装格式
                if _is_api_response(data):
                    return DataFormat.API_RESPONSE
                return DataFormat.JSON_OBJECT
        except json.JSONDecodeError:
            # 4. 【重要】启发式检测：如果是截断的 JSON，通过特征判断
            # 这对于流式处理非常重要，因为只读取了部分数据
            if _detect_api_response_heuristic(text_stripped):
                return DataFormat.API_RESPONSE
            # 检查是否像是单个 JSON 对象（被截断的）
            if _detect_json_object_heuristic(text_stripped):
                return DataFormat.JSON_OBJECT
    
    # 5. 更严格的 CSV 检测：检查是否有明确的 CSV 特征
    # - 第一行包含逗号分隔的字段名
    # - 不以 { 或 [ 开头
    if not text_stripped.startswith('{') and not text_stripped.startswith('['):
        lines = text_stripped.split('\n')
        if len(lines) >= 1:
            first_line = lines[0].strip()
            # 检查是否有逗号分隔的多个字段
            if ',' in first_line or '\t' in first_line:
                return DataFormat.CSV
    
    # 6. 无法识别的格式
    logging.warning(f"   ⚠️ Could not detect format. First 100 chars: {text_stripped[:100]}")
    return DataFormat.UNKNOWN


def _detect_api_response_heuristic(text_data: str) -> bool:
    """
    启发式检测：判断是否为 API 响应格式（即使 JSON 被截断）
    
    特征：
    - 以 { 开头
    - 包含 "code": 或 "status":
    - 包含 "results": 或 "data": 等数据字段
    """
    # 检查常见的 API 响应特征
    has_code = '"code"' in text_data or '"status"' in text_data
    has_data_field = any(f'"{key}"' in text_data for key in API_RESPONSE_DATA_KEYS)
    
    return has_code and has_data_field


def _detect_json_object_heuristic(text_data: str) -> bool:
    """
    启发式检测：判断是否为 JSON 对象格式（即使被截断）
    
    特征：
    - 以 { 开头
    - 包含 "key": 格式的键值对
    """
    import re
    # 检查是否有 JSON 键值对模式
    pattern = r'"[a-zA-Z_][a-zA-Z0-9_]*"\s*:'
    return bool(re.search(pattern, text_data))


def _is_api_response(data: dict) -> bool:
    """
    检测是否为 API 响应包装格式
    
    特征：
    - 包含常见的数据字段（results, data, items 等）
    - 该字段值为列表
    - 可能包含 code, status, message 等元数据字段
    """
    # 检查是否有常见的数据字段
    for key in API_RESPONSE_DATA_KEYS:
        if key in data and isinstance(data[key], list):
            return True
    
    # 检查是否有 code/status 字段 + 某个列表字段
    has_meta = any(k in data for k in ['code', 'status', 'success', 'message', 'msg'])
    has_list_field = any(isinstance(v, list) for v in data.values())
    
    return has_meta and has_list_field


def _extract_data_from_api_response(data: dict) -> Tuple[List[Any], str]:
    """
    从 API 响应中提取实际数据
    
    Args:
        data: API 响应字典
        
    Returns:
        Tuple[List[Any], str]: (提取的数据列表, 使用的字段名)
    """
    # 按优先级查找数据字段
    for key in API_RESPONSE_DATA_KEYS:
        if key in data and isinstance(data[key], list):
            return data[key], key
    
    # 如果没找到已知字段，找第一个列表字段
    for key, value in data.items():
        if isinstance(value, list):
            return value, key
    
    # 没有找到列表字段，返回空
    return [], ''


# ============================================================================
# 格式转换
# ============================================================================

def convert_to_jsonl(
    text_data: str, 
    data_format: DataFormat = None,
    date_columns: List[str] = None
) -> Tuple[str, int, DataFormat]:
    """
    将各种格式的数据转换为 JSON Lines
    
    Args:
        text_data: 原始文本数据
        data_format: 可选，指定数据格式（不指定则自动检测）
        date_columns: 需要转换为字符串的日期列名
        
    Returns:
        Tuple[str, int, DataFormat]: (JSONL 内容, 行数, 检测到的格式)
    """
    if not text_data or not text_data.strip():
        return '', 0, DataFormat.EMPTY
    
    # 自动检测格式
    if data_format is None:
        data_format = detect_format(text_data)
    
    print(f"   📋 Detected format: {data_format.value}")
    
    if date_columns is None:
        date_columns = ['date', 'report_date', 'start_ds', 'end_ds', 'exc_ds', 'day']
    
    if data_format == DataFormat.JSONL:
        return _convert_jsonl(text_data)
    
    elif data_format == DataFormat.JSON_ARRAY:
        return _convert_json_array(text_data)
    
    elif data_format == DataFormat.API_RESPONSE:
        return _convert_api_response(text_data)
    
    elif data_format == DataFormat.JSON_OBJECT:
        return _convert_json_object(text_data)
    
    elif data_format == DataFormat.CSV:
        return _convert_csv(text_data, date_columns)
    
    else:
        logging.warning(f"   ⚠️ Unknown format, returning as-is")
        return text_data, 0, DataFormat.UNKNOWN


def _convert_jsonl(text_data: str) -> Tuple[str, int, DataFormat]:
    """处理 JSON Lines 格式"""
    lines = []
    row_count = 0
    for line in text_data.strip().split('\n'):
        line = line.strip()
        if line:
            try:
                json.loads(line)  # 验证
                lines.append(line)
                row_count += 1
            except json.JSONDecodeError as e:
                logging.warning(f"   ⚠️ Skipping invalid JSON line: {str(e)[:50]}")
    return '\n'.join(lines), row_count, DataFormat.JSONL


def _convert_json_array(text_data: str) -> Tuple[str, int, DataFormat]:
    """处理 JSON 数组格式"""
    data = json.loads(text_data)
    lines = [json.dumps(item, ensure_ascii=False) for item in data]
    return '\n'.join(lines), len(lines), DataFormat.JSON_ARRAY


def _convert_api_response(text_data: str) -> Tuple[str, int, DataFormat]:
    """处理 API 响应包装格式"""
    data = json.loads(text_data)
    extracted_data, field_name = _extract_data_from_api_response(data)
    
    if not extracted_data:
        # 没有找到数据列表，当作普通 JSON 对象处理
        logging.warning(f"   ⚠️ No list data found in API response, treating as single object")
        return json.dumps(data, ensure_ascii=False), 1, DataFormat.JSON_OBJECT
    
    print(f"   📦 Extracted {len(extracted_data)} records from '{field_name}' field")
    lines = [json.dumps(item, ensure_ascii=False) for item in extracted_data]
    return '\n'.join(lines), len(lines), DataFormat.API_RESPONSE


def _convert_json_object(text_data: str) -> Tuple[str, int, DataFormat]:
    """处理单个 JSON 对象"""
    data = json.loads(text_data)
    return json.dumps(data, ensure_ascii=False), 1, DataFormat.JSON_OBJECT


def _convert_csv(text_data: str, date_columns: List[str]) -> Tuple[str, int, DataFormat]:
    """处理 CSV 格式"""
    csv_kwargs = _get_read_csv_kwargs()
    df = pandas.read_csv(io.StringIO(text_data), **csv_kwargs)
    
    # 处理日期列
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # 转换为 JSONL
    lines = []
    for _, row in df.iterrows():
        record = {col: (None if pandas.isna(val) else val) for col, val in row.items()}
        lines.append(json.dumps(record, ensure_ascii=False))
    
    return '\n'.join(lines), len(lines), DataFormat.CSV


# ============================================================================
# 流式解析器（用于大文件）
# ============================================================================

class StreamingParser:
    """
    流式数据解析器
    
    用于处理大文件，避免一次性加载到内存
    """
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.date_columns = ['date', 'report_date', 'start_ds', 'end_ds', 'exc_ds', 'day']
    
    def detect_format_from_file(self, file_obj) -> DataFormat:
        """
        从文件对象中检测格式
        
        会读取文件开头的一部分来检测格式，然后 seek 回开头
        """
        current_pos = file_obj.tell()
        
        # 读取开头 4KB 来检测格式
        sample = file_obj.read(4096)
        file_obj.seek(current_pos)
        
        if isinstance(sample, bytes):
            sample = sample.decode('utf-8', errors='ignore')
        
        return detect_format(sample)
    
    def parse_file(self, file_obj, data_format: DataFormat = None) -> Iterator[Tuple[List[dict], int]]:
        """
        流式解析文件
        
        Args:
            file_obj: 文件对象（需要支持 read/seek）
            data_format: 可选，指定格式
            
        Yields:
            Tuple[List[dict], int]: (记录列表, 当前批次大小)
        """
        if data_format is None:
            data_format = self.detect_format_from_file(file_obj)
        
        print(f"   📋 Detected format: {data_format.value}")
        
        if data_format == DataFormat.CSV:
            yield from self._parse_csv_streaming(file_obj)
        else:
            # 对于 JSON 格式，先读取全部内容再解析
            # （因为 JSON 格式不能真正流式解析）
            yield from self._parse_json_all(file_obj, data_format)
    
    def _parse_csv_streaming(self, file_obj) -> Iterator[Tuple[List[dict], int]]:
        """流式解析 CSV"""
        csv_kwargs = _get_read_csv_kwargs()
        csv_kwargs['chunksize'] = self.chunk_size
        
        for chunk_df in pandas.read_csv(file_obj, **csv_kwargs):
            # 处理日期列
            for col in self.date_columns:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str)
            
            # 转换为字典列表
            records = []
            for _, row in chunk_df.iterrows():
                record = {col: (None if pandas.isna(val) else val) for col, val in row.items()}
                records.append(record)
            
            yield records, len(records)
    
    def _parse_json_all(self, file_obj, data_format: DataFormat) -> Iterator[Tuple[List[dict], int]]:
        """解析 JSON 格式（非流式，但分批返回）"""
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        
        # 如果格式是 UNKNOWN，重新检测（因为现在有完整数据）
        if data_format == DataFormat.UNKNOWN:
            data_format = detect_format(content)
            print(f"   📋 Re-detected format with full content: {data_format.value}")
        
        # 转换为 JSONL
        jsonl_content, row_count, actual_format = convert_to_jsonl(content, data_format)
        
        if not jsonl_content:
            return
        
        # 分批返回
        lines = jsonl_content.split('\n')
        batch = []
        
        for line in lines:
            line = line.strip()
            if line:
                try:
                    batch.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
                
                if len(batch) >= self.chunk_size:
                    yield batch, len(batch)
                    batch = []
        
        # 返回剩余的
        if batch:
            yield batch, len(batch)


# ============================================================================
# 便捷函数
# ============================================================================

def parse_response_content(content: bytes, encoding: str = 'utf-8') -> Tuple[str, int, DataFormat]:
    """
    解析 HTTP 响应内容
    
    Args:
        content: 响应的字节内容
        encoding: 编码格式
        
    Returns:
        Tuple[str, int, DataFormat]: (JSONL 内容, 行数, 格式)
    """
    text_data = content.decode(encoding)
    return convert_to_jsonl(text_data)


def records_to_jsonl(records: List[dict]) -> str:
    """
    将记录列表转换为 JSONL 字符串
    
    Args:
        records: 字典列表
        
    Returns:
        str: JSONL 格式字符串
    """
    return '\n'.join(json.dumps(record, ensure_ascii=False) for record in records)


# ============================================================================
# AppLovin 专用转换器
# ============================================================================

def expand_applovin_max_ad_unit(ad_unit_data: dict) -> List[dict]:
    """
    展开 AppLovin MAX 广告单元数据中的 ad_network_settings
    
    将每个 network setting 展开为单独的行
    
    输入格式:
    {
        "id": "xxx",
        "name": "...",
        "platform": "ios",
        "ad_format": "INTER",
        "package_name": "com.xxx",
        "disabled": false,
        "ad_network_settings": {
            "UNITY_BIDDING": {"disabled": false, "ad_network_ad_unit_id": "xxx"},
            "ADMOB_BIDDING": {"disabled": false, "ad_network_ad_unit_id": "yyy"},
            ...
        }
    }
    
    输出格式 (每个 network 一行):
    {"id":"xxx","name":"...","platform":"ios","ad_format":"INTER","package_name":"com.xxx","disabled":false,"network":"UNITY_BIDDING","ad_network_ad_unit_id":"xxx"}
    
    Args:
        ad_unit_data: 广告单元数据字典
        
    Returns:
        List[dict]: 展开后的记录列表
    """
    # 基础字段
    base_fields = ['id', 'name', 'platform', 'ad_format', 'package_name', 'disabled', 'has_active_experiment']
    base_record = {k: ad_unit_data.get(k) for k in base_fields if k in ad_unit_data}
    
    # 获取 ad_network_settings
    ad_network_settings = ad_unit_data.get('ad_network_settings', {})
    
    # 如果没有 network settings，返回基础记录
    if not ad_network_settings:
        return [base_record]
    
    # 展开每个 network setting
    expanded_records = []
    
    # ad_network_settings 可能是 dict 或 list
    if isinstance(ad_network_settings, dict):
        for network_name, network_config in ad_network_settings.items():
            record = base_record.copy()
            record['network'] = network_name
            
            # 提取 network 配置中的字段
            if isinstance(network_config, dict):
                record['network_disabled'] = network_config.get('disabled', False)
                record['ad_network_ad_unit_id'] = network_config.get('ad_network_ad_unit_id', '')
                # 可能还有其他字段
                for key in ['cpm_floor', 'cpm_floor_value']:
                    if key in network_config:
                        record[key] = network_config[key]
            
            expanded_records.append(record)
    
    elif isinstance(ad_network_settings, list):
        # 如果是列表格式
        for network_config in ad_network_settings:
            if isinstance(network_config, dict):
                record = base_record.copy()
                # 尝试获取 network 名称
                network_name = network_config.get('network') or network_config.get('name') or 'unknown'
                record['network'] = network_name
                record['network_disabled'] = network_config.get('disabled', False)
                record['ad_network_ad_unit_id'] = network_config.get('ad_network_ad_unit_id', '')
                
                expanded_records.append(record)
    
    return expanded_records if expanded_records else [base_record]


def convert_applovin_max_config(text_data: str) -> Tuple[str, int]:
    """
    转换 AppLovin MAX 配置 API 响应
    
    自动处理：
    1. API 响应包装格式 {"code": 200, "results": [...]}
    2. 单个广告单元对象
    3. 展开 ad_network_settings
    
    Args:
        text_data: 原始 API 响应文本
        
    Returns:
        Tuple[str, int]: (JSONL 内容, 行数)
    """
    if not text_data or not text_data.strip():
        return '', 0
    
    text_stripped = text_data.strip()
    
    try:
        data = json.loads(text_stripped)
    except json.JSONDecodeError as e:
        logging.error(f"❌ Failed to parse JSON: {e}")
        return '', 0
    
    all_records = []
    
    # 如果是 API 响应包装格式
    if isinstance(data, dict) and _is_api_response(data):
        extracted_data, field_name = _extract_data_from_api_response(data)
        print(f"   📦 Extracted {len(extracted_data)} ad units from '{field_name}' field")
        
        for ad_unit in extracted_data:
            if isinstance(ad_unit, dict):
                all_records.extend(expand_applovin_max_ad_unit(ad_unit))
    
    # 如果是单个广告单元对象
    elif isinstance(data, dict):
        all_records.extend(expand_applovin_max_ad_unit(data))
    
    # 如果是广告单元列表
    elif isinstance(data, list):
        for ad_unit in data:
            if isinstance(ad_unit, dict):
                all_records.extend(expand_applovin_max_ad_unit(ad_unit))
    
    if not all_records:
        return '', 0
    
    print(f"   📊 Expanded to {len(all_records)} network records")
    lines = [json.dumps(record, ensure_ascii=False) for record in all_records]
    return '\n'.join(lines), len(lines)
