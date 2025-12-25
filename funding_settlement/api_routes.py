"""
资金费率结算HTTP接口 - 新增公共调试接口
"""
from aiohttp import web
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any

# 设置导入路径
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from .manager import FundingSettlementManager
from .templates import get_html_page, get_public_html_page
from http_server.auth import require_auth

logger = logging.getLogger(__name__)

# 创建管理器实例
_manager = FundingSettlementManager()


# ============ ✅ 新增：公共API（无需密码） ============
async def get_settlement_public(request: web.Request) -> web.Response:
    """
    公共API：获取所有资金费率结算数据（无需密码）
    GET /api/funding/settlement/public
    """
    try:
        # 从共享数据模块获取数据
        from shared_data.data_store import data_store
        
        funding_data = data_store.funding_settlement.get('binance', {})
        
        # 格式化为详细数据
        formatted_data = []
        for symbol, data in funding_data.items():
            formatted_data.append({
                "交易所": "币安",
                "符号": symbol,
                "data_type": "资金费率",
                "funding_rate": data.get('funding_rate'),
                "funding_time": data.get('funding_time'),
                "next_funding_time": data.get('next_funding_time'),
                "timestamp": datetime.now().isoformat(),
                "source": "api"
            })
        
        return web.json_response({
            "success": True,
            "count": len(formatted_data),
            "data": formatted_data
        })
        
    except Exception as e:
        logger.error(f"公共API错误: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "data": []
        })


async def get_settlement_public_page(request: web.Request) -> web.Response:
    """
    公共HTML页面（无需密码）
    GET /funding/settlement/public
    """
    try:
        from shared_data.data_store import data_store
        
        contracts = data_store.funding_settlement.get('binance', {})
        
        # 生成HTML表格
        html_content = get_public_html_page(contracts)
        return web.Response(text=html_content, content_type='text/html')
        
    except Exception as e:
        logger.error(f"生成公共页面失败: {e}")
        return web.Response(text=f"页面生成错误: {e}", status=500)


# ============ ✅ 原有：需要密码的接口 ============
@require_auth
async def get_settlement_status(request: web.Request) -> web.Response:
    """查看状态（需要密码）"""
    try:
        status = _manager.get_status()
        from shared_data.data_store import data_store
        
        contracts = data_store.funding_settlement.get('binance', {})
        sample_contracts = list(contracts.keys())[:5] if contracts else []
        
        return web.json_response({
            "success": True,
            "status": status,
            "sample_contracts": sample_contracts,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, status=500)


@require_auth
async def post_fetch_settlement(request: web.Request) -> web.Response:
    """手动触发获取（需要密码）"""
    try:
        result = await _manager.manual_fetch()
        return web.json_response(result)
        
    except Exception as e:
        logger.error(f"手动获取失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, status=500)


async def get_settlement_page(request: web.Request) -> web.Response:
    """HTML管理页面（需要密码的原有页面）"""
    try:
        from shared_data.data_store import data_store
        
        contracts = data_store.funding_settlement.get('binance', {})
        html_content = get_html_page(_manager)
        return web.Response(text=html_content, content_type='text/html')
        
    except Exception as e:
        logger.error(f"生成页面失败: {e}")
        return web.Response(text=f"页面生成错误: {e}", status=500)


def setup_funding_settlement_routes(app: web.Application):
    """
    设置资金费率结算路由
    """
    # ✅ 公共接口（无需密码）
    app.router.add_get('/api/funding/settlement/public', get_settlement_public)
    app.router.add_get('/funding/settlement/public', get_settlement_public_page)
    
    # ✅ 受保护接口（需要密码）
    app.router.add_get('/api/funding/settlement/status', get_settlement_status)
    app.router.add_post('/api/funding/settlement/fetch', post_fetch_settlement)
    app.router.add_get('/funding/settlement', get_settlement_page)
    
    logger.info("✅ 资金费率结算路由已加载:")
    logger.info("   - GET  /api/funding/settlement/public (🔓 公共)")
    logger.info("   - GET  /funding/settlement/public (🔓 公共)")
    logger.info("   - GET  /api/funding/settlement/status (🔐 需密码)")
    logger.info("   - POST /api/funding/settlement/fetch (🔐 需密码)")
    logger.info("   - GET  /funding/settlement (🔐 需密码)")
