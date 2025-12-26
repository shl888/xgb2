# shared_data/config.py
"""
配置常量
"""

# 数据缓存配置
CACHE_CONFIG = {
    'step4_cleanup_interval': 3600,  # Step4缓存清理间隔（秒）
    'step4_max_cache_size': 1000,    # Step4最大缓存条目数
    'step5_cleanup_interval': 3600,  # Step5缓存清理间隔（秒）
    'step5_max_cache_size': 500,     # Step5最大缓存条目数
}

# 处理配置
PROCESSING_CONFIG = {
    'min_funding_rate_threshold': 0.0003,  # 高费率记录阈值
    'log_data_flow': False,                # 是否详细记录数据流
    'enable_validation': True,             # 是否启用数据验证
}

# 时间转换配置
TIME_CONFIG = {
    'beijing_offset_hours': 8,            # 北京时区偏移
    'timestamp_format': 'iso',            # 时间戳格式
}