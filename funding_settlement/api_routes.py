"""
资金费率结算HTTP接口
提供手动触发和状态查看功能
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
from .templates import get_html_page
from http_server.auth import require_auth

logger = logging.getLogger(__name__)

# 创建管理器实例
_manager = FundingSettlementManager()


@require_auth
async def get_settlement_status(request: web.Request) -> web.Response:
    """
    查看资金费率结算数据状态
    GET /api/funding/settlement/status
    """
    try:
        status = _manager.get_status()
        
        # 获取一些统计信息
        from shared_data.data_store import data_store
        
        contracts = data_store.funding_settlement.get('binance', {})
        sample_contracts = list(contracts.keys())[:5] if contracts else []
        
        return dict({
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
    """
    手动触发获取资金费率结算数据
    POST /api/funding/settlement/fetch
    """
    try:
        # 检查认证
        provided_password = request.headers.get('X-Access-Password')
        if not provided_password:
            return web.json_response({
                "success": False,
                "error": "缺少访问密码。请在请求头中使用: X-Access-Password"
            }, status=401)
        
        # 执行手动获取
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
    """
    资金费率结算管理页面
    GET /funding/settlement
    """
    try:
        html_content = get_html_page(_manager)
        return web.Response(text=html_content, content_type='text/html')
        
    except Exception as e:
        logger.error(f"生成页面失败: {e}")
        return web.Response(text=f"页面生成错误: {e}", status=500)


def setup_funding_settlement_routes(app: web.Application):
    """
    设置资金费率结算路由
    """
    # 状态查看接口
    app.router.add_get('/api/funding/settlement/status', get_settlement_status)
    
    # 手动触发接口
    app.router.add_post('/api/funding/settlement/fetch', post_fetch_settlement)
    
    # HTML管理页面（公开路径，但数据需要认证）
    app.router.add_get('/funding/settlement', get_settlement_page)
    
    logger.info("✅ 资金费率结算路由已加载:")
    logger.info("   - GET  /api/funding/settlement/status")
    logger.info("   - POST /api/funding/settlement/fetch")
    logger.info("   - GET  /funding/settlement")
