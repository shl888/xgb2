"""
日志中央控制器
功能：只在这里改，控制所有日志
使用方法：直接改字典，重启程序生效
"""

import logging
import os

# ============== 在这里改（唯一需要修改的地方） ==============
# 全局日志级别：debug/info/warning/error
# 生产环境设 "warning"，调试设 "info"，深度调试设 "debug"
GLOBAL_LOG_LEVEL = "info"  # ✅ 改这里

# 各模块日志级别（想开哪个就把"warning"改成"info"或"debug"）
MODULE_LEVELS = {
    # 核心模块（生产环境保持warning）
    "brain_core": "info",
    "websocket_pool.admin": "warning",
    "websocket_pool.pool_manager": "warning",
    "http_server.server": "warning",
    
    # 流水线模块（想看哪个开哪个）
    "shared_data.pipeline_manager": "warning",  # ✅ 改成"info"看流水线
    "shared_data.step1_filter": "warning",      # ✅ 改成"info"看Step1
    "shared_data.step2_fusion": "warning",      # ✅ 改成"info"看Step2
    "shared_data.step3_align": "warning",       # ✅ 改成"info"看Step3
    "shared_data.step4_calc": "warning",        # ✅ 改成"info"看Step4
    "shared_data.step5_cross_calc": "info",  # ✅ 改成"info"看Step5
    
    # 数据存储（想看数据更新）
    "shared_data.data_store": "warning",        # ✅ 改成"info"看数据存储
    
    # 资金费率（可保留info）
    "funding_settlement.api_routes": "info",
    
    # 第三方库（全部error，最安静）
    "aiohttp": "error",
    "aiohttp.access": "error",  # 关闭HTTP访问日志
    "urllib3": "error",
    "asyncio": "error",
}

# ============== 配置函数（别改这里） ==============
def setup_all_loggers():
    """配置所有日志器，只调用一次"""
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    
    global_level = level_map.get(GLOBAL_LOG_LEVEL, logging.WARNING)
    
    # 设置根日志器
    logging.basicConfig(
        level=global_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 设置每个模块
    for logger_name, level_name in MODULE_LEVELS.items():
        level = level_map.get(level_name, logging.WARNING)
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.propagate = False  # 防止重复
    
    print(f"✅ 日志配置完成，{len(MODULE_LEVELS)} 个模块已设置")
    print(f"📝 全局级别: {GLOBAL_LOG_LEVEL}")
    
    # 显示哪些模块开了
    active = [name for name, level in MODULE_LEVELS.items() if level in ["info", "debug"]]
    if active:
        print(f"🎯 已开启的模块: {', '.join(active)}")
    else:
        print("🎯 所有模块处于静默模式（只显示警告和错误）")
