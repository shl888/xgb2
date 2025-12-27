"""
日志开关 - 终极版
只在这里改，重启生效
"""

# ============== 在这里改 ==============
# 全局："warning"=只错误, "info"=显示状态, "debug"=全显示
GLOBAL_LOG_LEVEL = "info"

# 高频数据流（务必False）
DATA_FLOW_LOGS = {
    "websocket_receive": False,   # WebSocket接收
    "data_store_update": False,   # 数据存储
    "pipeline_ingest": False,     # 流水线接收
    "step5": False,               # Step5结果
}

# 流水线步骤（务必False，除非调试）
STEP_LOGS = {
    "step1": False,   # Step1提取
    "step2": False,   # Step2融合
    "step3": False,   # Step3对齐
    "step4": False,   # Step4计算
    "step5": False,   # Step5跨平台
}

# 服务状态（建议True）
STATUS_LOGS = {
    "brain_init": True,           # 大脑启动
    "pipeline_batch": True,       # 批量完成
    "memory_warning": True,       # 内存警告
}
# ============== 别改下面 ==============

import logging
import sys

def setup_all_loggers():
    level = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING}
    logging.basicConfig(level=level[GLOBAL_LOG_LEVEL], format='%(asctime)s - %(message)s')
    
    # 所有模块默认ERROR（静默）
    for name in [
        "brain_core",
        "websocket_pool.admin",
        "websocket_pool.pool_manager",
        "http_server.server",
        "shared_data.data_store",
        "shared_data.pipeline_manager",
        "shared_data.step1_filter",
        "shared_data.step2_fusion",
        "shared_data.step3_align",
        "shared_data.step4_calc",
        "shared_data.step5_cross_calc",
        "funding_settlement.api_routes",
        "aiohttp",
        "urllib3",
    ]:
        logging.getLogger(name).setLevel(logging.ERROR)
    
    # 状态日志设为INFO
    if STATUS_LOGS["brain_init"]:
        logging.getLogger("brain_core").setLevel(logging.INFO)
    if STATUS_LOGS["pipeline_batch"]:
        logging.getLogger("shared_data.pipeline_manager").setLevel(logging.INFO)
    
    print(f"✅ 日志: {GLOBAL_LOG_LEVEL} | 数据: {'开' if any(DATA_FLOW_LOGS.values()) else '关'}")

def should_log(name: str) -> bool:
    return DATA_FLOW_LOGS.get(name, False)
