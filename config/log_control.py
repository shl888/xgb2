"""
日志开关 - 极简版
只在这里改，重启生效
"""

# ============== 在这里改 ==============
# 全局级别："warning"=只显示错误, "info"=显示状态, "debug"=显示所有
GLOBAL_LOG_LEVEL = "info"  # ✅ 推荐保持"info"

# 高频数据流（每秒100次，务必保持False）
DATA_FLOW_LOGS = {
    "websocket_receive": False,   # WebSocket接收（最频繁）
    "data_store_update": False,   # 数据存储
    "pipeline_ingest": False,     # 流水线接收
    "step5": False,               # Step5结果（可设为True，查看最终结果）
}

# 服务状态（低频，建议保持True）
STATUS_LOGS = {
    "brain_init": True,           # 大脑启动
    "pipeline_batch": True,       # 批量处理完成
    "memory_warning": True,       # 内存警告
}
# ============== 别改下面 ==============

import logging
import sys

def setup_all_loggers():
    level = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING}
    logging.basicConfig(level=level[GLOBAL_LOG_LEVEL], format='%(asctime)s - %(name)s - %(message)s')
    
    # 高频模块设为ERROR（静默）
    for name in ["websocket_pool.pool_manager", "shared_data.data_store", "shared_data.pipeline_manager"]:
        logging.getLogger(name).setLevel(logging.ERROR)
    
    # 状态模块设为INFO（显示）
    if STATUS_LOGS["brain_init"]:
        logging.getLogger("brain_core").setLevel(logging.INFO)
    if STATUS_LOGS["pipeline_batch"]:
        logging.getLogger("shared_data.pipeline_manager").setLevel(logging.INFO)
    
    print(f"✅ 日志配置: {GLOBAL_LOG_LEVEL} | 数据流: {'开' if any(DATA_FLOW_LOGS.values()) else '关'}")

def should_log(name: str) -> bool:
    return DATA_FLOW_LOGS.get(name, False)
