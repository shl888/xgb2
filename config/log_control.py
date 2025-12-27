"""
日志中央控制器 - 生产优化版
功能：只在这里改，控制所有日志
特点：默认"info"安静不刷屏，高频数据流单独控制
"""

import logging
import os

# ============== 在这里改（唯一需要修改的地方） ==============
# 全局日志级别：debug/info/warning/error
# 推荐默认值："info"（显示关键信息，不刷屏）
# 完全静默："warning"（只显示错误和警告）
# 深度调试："debug"（显示所有细节）
GLOBAL_LOG_LEVEL = "info"  # ✅ 推荐保持"info"，关键信息始终显示

# 高频数据流日志（每秒50-100次，务必设为False）
# 只有需要看数据流时才临时改成True，看完立刻改回False
DATA_FLOW_LOGS = {
    "websocket_receive": False,   # WebSocket每秒接收（最频繁）
    "websocket_parse": False,     # 数据解析
    "data_store_update": False,   # 数据存储
    "pipeline_ingest": False,     # 流水线接收
    "brain_receive": False,       # 大脑接收
    "step1": False,               # Step1提取
    "step2": False,               # Step2融合
    "step3": False,               # Step3对齐
    "step4": False,               # Step4计算
    "step5": False,               # Step5跨平台（可设为True，查看最终结果）
}

# 低频状态日志（可设为True）
STATUS_LOGS = {
    "brain_init": True,           # 大脑启动（关键信息）
    "brain_shutdown": True,       # 大脑关闭
    "pipeline_batch": False,      # 批量处理（可设为True，看批量频率）
    "connection_error": True,     # 连接错误（始终有效，不受开关控制）
    "http_error": True,           # HTTP错误（始终有效）
    "funding_fetch_error": True,  # 资金费率获取失败（始终有效）
}

# ============== 配置函数（别改这里） ==============
def setup_all_loggers():
    """配置所有日志器，只调用一次"""
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    
    global_level = level_map.get(GLOBAL_LOG_LEVEL, logging.INFO)
    
    # 设置根日志器
    logging.basicConfig(
        level=global_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 显示启动信息（不受日志级别影响）
    print("="*60)
    print(f"✅ 日志系统初始化完成")
    print(f"📝 全局级别: {GLOBAL_LOG_LEVEL}")
    print(f"🎯 高频数据流日志: {'开启' if any(DATA_FLOW_LOGS.values()) else '关闭'}")
    print(f"🎯 低频状态日志: {'开启' if any(STATUS_LOGS.values()) else '关闭'}")
    print("="*60)
    
    # 设置每个模块
    for logger_name in [
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
        logger = logging.getLogger(logger_name)
        
        # 高频数据流模块设为ERROR（完全静默）
        if any(DATA_FLOW_LOGS.values()):
            # 如果用户开了某个数据流日志，对应模块设为INFO
            if logger_name == "websocket_pool.pool_manager" and DATA_FLOW_LOGS["websocket_receive"]:
                logger.setLevel(logging.INFO)
            elif logger_name == "shared_data.data_store" and DATA_FLOW_LOGS["data_store_update"]:
                logger.setLevel(logging.INFO)
            elif logger_name == "shared_data.pipeline_manager" and DATA_FLOW_LOGS["pipeline_ingest"]:
                logger.setLevel(logging.INFO)
            elif logger_name == "brain_core" and DATA_FLOW_LOGS["brain_receive"]:
                logger.setLevel(logging.INFO)
            elif logger_name == "shared_data.step5_cross_calc" and DATA_FLOW_LOGS["step5"]:
                logger.setLevel(logging.INFO)
            else:
                logger.setLevel(logging.ERROR)  # 其他高频模块完全静默
        else:
            # 没有开数据流日志，全部ERROR
            logger.setLevel(logging.ERROR)
        
        logger.propagate = False
    
    # 状态日志模块设为INFO（显示关键信息）
    for logger_name in [
        "brain_core",  # 启动/关闭日志
        "shared_data.pipeline_manager",  # 批量日志
    ]:
        if STATUS_LOGS["brain_init"] or STATUS_LOGS["pipeline_batch"]:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.INFO)

# ============== 判断函数（核心逻辑） ==============
def should_log(log_type: str, name: str) -> bool:
    """
    判断是否应该打印日志
    - INFO/DEBUG：受开关控制
    - WARNING/ERROR/CRITICAL：始终显示（不受开关控制）
    """
    # WARNING/ERROR/CRITICAL 始终显示
    # INFO/DEBUG 受开关控制
    
    if log_type == "data_flow":
        return DATA_FLOW_LOGS.get(name, False)
    elif log_type == "status":
        return STATUS_LOGS.get(name, False)
    
    return False

# ============== 打印当前配置（调试时调用） ==============
def show_current_config():
    print("="*60)
    print("📊 当前日志配置")
    print(f"全局级别: {GLOBAL_LOG_LEVEL}")
    print(f"高频数据流日志: {DATA_FLOW_LOGS}")
    print(f"低频状态日志: {STATUS_LOGS}")
    print("="*60)

# ============== 快速切换函数 ==============
def enable_step5_log():
    """快速开启Step5日志（查看最终结果）"""
    DATA_FLOW_LOGS["step5"] = True
    print("✅ Step5日志已开启")

def disable_all_data_flow():
    """快速关闭所有高频数据流日志（生产环境用）"""
    for key in DATA_FLOW_LOGS:
        DATA_FLOW_LOGS[key] = False
    print("✅ 所有高频数据流日志已关闭")

def enable_all_data_flow():
    """快速开启所有高频数据流日志（调试用，会爆炸）"""
    for key in DATA_FLOW_LOGS:
        DATA_FLOW_LOGS[key] = True
    print("⚠️  所有高频数据流日志已开启（日志会爆炸！）")
