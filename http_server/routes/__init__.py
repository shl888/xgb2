"""
HTTP路由聚合模块
集中管理所有路由的导入和注册
"""
from aiohttp import web
import logging
import datetime
import sys
import os
from typing import Dict, Any

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# ============ 共享依赖（所有route模块可用） ============
from shared_data.data_store import data_store  # noqa: E402
from ..exchange_api import ExchangeAPI         # noqa: E402
from ..auth import require_auth                # noqa: E402

logger = logging.getLogger(__name__)

# ============ 导入各模块路由 ============
from .main import setup_main_routes
from .debug import setup_debug_routes
from .trade import setup_trade_routes
from .account import setup_account_routes
from .monitor import setup_monitor_routes


def setup_routes(app: web.Application):
    """
    主路由设置函数 - 聚合所有模块
    保持与原始routes.py完全兼容的接口
    """
    logger.info("开始加载路由模块...")
    
    # 基础路由（根路径、健康检查等）
    setup_main_routes(app)
    
    # 调试接口路由
    setup_debug_routes(app)
    
    # 交易接口路由
    setup_trade_routes(app)
    
    # 账户与市场数据路由
    setup_account_routes(app)
    
    # 系统监控路由
    setup_monitor_routes(app)
    
    logger.info("=" * 60)
    logger.info("✅ 所有路由模块加载完成")
    logger.info("📊 路由统计:")
    logger.info(f"   - 总路由数: {len(app.router.routes())}")
    logger.info(f"   - 调试接口: /api/debug/* (4个)")
    logger.info(f"   - 交易接口: /api/trade/* (5个)")
    logger.info(f"   - 账户接口: /api/account/* (2个)")
    logger.info(f"   - 市场数据: /api/market/*, /api/data/* (3个)")
    logger.info(f"   - 监控接口: /api/monitor/* (3个)")
    logger.info(f"   - 基础接口: /, /health, /public/ping (3个)")
    logger.info("=" * 60)


# ============ 公共辅助函数（所有模块可用） ============
def _calculate_data_age(timestamp_str: str) -> float:
    """计算数据年龄（秒）- 从原始routes.py提取"""
    if not timestamp_str:
        return float('inf')
    
    try:
        if 'T' in timestamp_str:
            # ISO格式
            try:
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                data_time = datetime.datetime.fromisoformat(timestamp_str)
            except ValueError:
                try:
                    if '.' in timestamp_str:
                        timestamp_str = timestamp_str.split('.')[0]
                    data_time = datetime.datetime.fromisoformat(timestamp_str)
                except:
                    return float('inf')
        else:
            try:
                ts = float(timestamp_str)
                if ts > 1e12:
                    ts = ts / 1000
                data_time = datetime.datetime.fromtimestamp(ts)
            except:
                return float('inf')
        
        now = datetime.datetime.now(datetime.timezone.utc)
        if data_time.tzinfo is None:
            data_time = data_time.replace(tzinfo=datetime.timezone.utc)
        
        return (now - data_time).total_seconds()
    except Exception:
        return float('inf')
