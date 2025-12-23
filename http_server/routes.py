"""
HTTP API路由定义
"""
from aiohttp import web
import json
import sys
import os
from typing import Dict, Any
import logging
import datetime

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # brain_core目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store  # 改为绝对导入
from .exchange_api import ExchangeAPI
from .auth import require_auth  # 新增导入

logger = logging.getLogger(__name__)

# ============ 【新增调试接口】============
async def get_all_websocket_data(request):
    """
    【核心调试接口】查看WebSocket获取的所有市场数据
    地址：GET /api/debug/all_websocket_data
    修复版：显示所有数据类型，不覆盖
    """
    try:
        # 获取查询参数
        query = request.query
        show_all = query.get('show_all', '').lower() == 'true'
        show_types = query.get('show_types', '').lower() == 'true'  # 新增：是否显示所有数据类型
        sample_size = min(int(query.get('sample', 3)), 10)  # 默认只显示3条，最多10条
        
        # 1. 从共享存储中获取数据
        # 使用新的get_market_data方法，get_latest=False获取所有数据类型
        binance_all_data = await data_store.get_market_data("binance", get_latest=False)
        okx_all_data = await data_store.get_market_data("okx", get_latest=False)
        
        # 2. 统计不同类型的数据量
        binance_stats = _count_data_types(binance_all_data)
        okx_stats = _count_data_types(okx_all_data)
        
        # 3. 准备返回的数据
        response_data = {
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "summary": {
                "binance_symbols_count": len(binance_all_data),
                "okx_symbols_count": len(okx_all_data),
                "total_symbols": len(binance_all_data) + len(okx_all_data),
                "data_type_stats": {
                    "binance": binance_stats,
                    "okx": okx_stats
                }
            }
        }
        
        if show_all:
            # 显示全部数据（可能很大）
            response_data['data'] = {
                "binance": binance_all_data,
                "okx": okx_all_data
            }
        else:
            # 显示抽样数据
            response_data['sample'] = {
                "binance": _get_sample_data(binance_all_data, sample_size, show_types),
                "okx": _get_sample_data(okx_all_data, sample_size, show_types)
            }
            
            # 动态提示
            hints = []
            hints.append("如需查看全部数据，请添加参数 ?show_all=true")
            if not show_types:
                hints.append("如需查看所有数据类型，请添加参数 ?show_types=true")
            hints.append(f"当前显示抽样数量: {sample_size} (可调整: ?sample=5)")
            
            response_data['hint'] = " | ".join(hints)
        
        return web.json_response(response_data)
        
    except Exception as e:
        logger.error(f"获取WebSocket数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }, status=500)

def _count_data_types(exchange_data: Dict) -> Dict[str, int]:
    """统计数据类型数量"""
    stats = {
        "total_symbols": 0,
        "ticker": 0,
        "funding_rate": 0,
        "mark_price": 0,
        "other": 0
    }
    
    if not exchange_data:
        return stats
    
    stats["total_symbols"] = len(exchange_data)
    
    for symbol, data_dict in exchange_data.items():
        if isinstance(data_dict, dict):
            for data_type in data_dict:
                if data_type in stats:
                    stats[data_type] += 1
                elif data_type not in ['latest', 'store_timestamp']:
                    stats["other"] += 1
    
    return stats

def _get_sample_data(exchange_data: Dict, sample_size: int, show_types: bool = False) -> Dict:
    """获取抽样数据"""
    if not exchange_data:
        return {}
    
    sample = {}
    count = 0
    
    for symbol, data_dict in exchange_data.items():
        if count >= sample_size:
            break
            
        if not show_types and isinstance(data_dict, dict) and 'latest' in data_dict:
            # 只显示最新数据
            latest_type = data_dict['latest']
            if latest_type in data_dict and latest_type != 'latest':
                sample[symbol] = data_dict[latest_type]
                count += 1
        else:
            # 显示所有数据类型
            sample[symbol] = data_dict
            count += 1
    
    return sample

async def get_symbol_detail(request):
    """
    【调试接口】查看指定交易对的详细数据
    地址：GET /api/debug/symbol/{exchange}/{symbol}
    增强版：显示所有数据类型
    """
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.match_info.get('symbol', '').upper()
        show_all_types = request.query.get('show_all_types', '').lower() == 'true'
        
        if exchange not in ['binance', 'okx']:
            return web.json_response({
                "success": False,
                "error": f"不支持的交易所: {exchange}"
            }, status=400)
        
        # 获取指定交易对数据（所有数据类型）
        data = await data_store.get_market_data(exchange, symbol, get_latest=False)
        
        if not data:
            return web.json_response({
                "success": False,
                "error": f"未找到数据: {exchange} {symbol}",
                "hint": "可能是: 1. 交易对名称错误 2. 该交易对未被订阅 3. 数据尚未到达"
            }, status=404)
        
        # 计算数据年龄
        for data_type, data_content in data.items():
            if isinstance(data_content, dict) and 'timestamp' in data_content:
                timestamp = data_content['timestamp']
                age_seconds = _calculate_data_age(timestamp)
                data_content['age_seconds'] = age_seconds
        
        response = {
            "success": True,
            "exchange": exchange,
            "symbol": symbol,
            "data_types_count": len(data),
            "data_types": list(data.keys()),
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        if show_all_types or len(data) <= 3:
            # 显示所有数据类型
            response['data'] = data
        else:
            # 默认只显示最新数据
            if 'latest' in data and data['latest'] in data:
                latest_type = data['latest']
                response['data'] = {latest_type: data[latest_type]}
                response['hint'] = f"当前显示最新数据类型: {latest_type}，如需查看所有类型请添加参数 ?show_all_types=true"
            else:
                response['data'] = data
        
        return web.json_response(response)
        
    except Exception as e:
        logger.error(f"获取交易对数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)

def _calculate_data_age(timestamp_str: str) -> float:
    """计算数据年龄（秒）"""
    if not timestamp_str:
        return float('inf')
    
    try:
        if 'T' in timestamp_str:
            # ISO格式
            try:
                # 尝试解析ISO格式
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                data_time = datetime.datetime.fromisoformat(timestamp_str)
            except ValueError:
                # 如果ISO解析失败，尝试其他格式
                try:
                    # 去掉毫秒部分再尝试
                    if '.' in timestamp_str:
                        timestamp_str = timestamp_str.split('.')[0]
                    data_time = datetime.datetime.fromisoformat(timestamp_str)
                except:
                    return float('inf')
        else:
            try:
                ts = float(timestamp_str)
                if ts > 1e12:  # 毫秒时间戳
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

async def get_funding_rates(request):
    """
    【新增接口】获取所有资金费率数据
    地址：GET /api/debug/funding_rates
    """
    try:
        # 获取查询参数
        query = request.query
        exchange = query.get('exchange', '').lower() or None
        min_rate = float(query.get('min_rate', 0)) if query.get('min_rate') else None
        max_rate = float(query.get('max_rate', 0)) if query.get('max_rate') else None
        show_all = query.get('show_all', '').lower() == 'true'
        sort_by = query.get('sort_by', 'rate')  # rate, abs_rate, symbol
        
        # 获取资金费率数据
        funding_rates = await data_store.get_funding_rates(
            exchange=exchange,
            min_rate=min_rate,
            max_rate=max_rate
        )
        
        # 如果有排序要求
        if sort_by and funding_rates:
            for exch, data in funding_rates.items():
                if 'data' in data:
                    sorted_data = dict(sorted(
                        data['data'].items(),
                        key=lambda x: _get_sort_key(x[1], sort_by)
                    ))
                    data['data'] = sorted_data
        
        # 准备响应
        response = {
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "query": {
                "exchange": exchange or "all",
                "min_rate": min_rate,
                "max_rate": max_rate,
                "sort_by": sort_by
            },
            "funding_rates": funding_rates
        }
        
        # 添加统计信息
        total_symbols = 0
        for exch, data in funding_rates.items():
            total_symbols += data.get('count', 0)
        
        response['summary'] = {
            "total_exchanges": len(funding_rates),
            "total_symbols": total_symbols,
            "exchanges": list(funding_rates.keys())
        }
        
        # 添加提示
        if not show_all and total_symbols > 50:
            response['hint'] = f"找到 {total_symbols} 个资金费率数据，只显示前50个。如需查看全部，请添加参数 ?show_all=true"
            # 限制返回数量
            for exch, data in funding_rates.items():
                if 'data' in data and len(data['data']) > 50:
                    data['data'] = dict(list(data['data'].items())[:50])
                    data['count'] = len(data['data'])
        
        return web.json_response(response)
        
    except Exception as e:
        logger.error(f"获取资金费率数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }, status=500)

def _get_sort_key(data_item: Dict, sort_by: str) -> Any:
    """获取排序键"""
    if sort_by == 'rate':
        return data_item.get('funding_rate', 0)
    elif sort_by == 'abs_rate':
        return abs(data_item.get('funding_rate', 0))
    elif sort_by == 'symbol':
        return data_item.get('symbol', '')
    elif sort_by == 'age':
        return data_item.get('age_seconds', float('inf'))
    else:
        return 0

async def get_websocket_status(request):
    """
    【调试接口】查看WebSocket连接池状态
    地址：GET /api/debug/websocket_status
    """
    try:
        # 获取连接状态
        connection_status = await data_store.get_connection_status()
        
        # 获取数据存储统计
        data_stats = data_store.get_market_data_stats()
        
        # 统计信息
        stats = {
            "total_exchanges": len(connection_status),
            "exchanges": list(connection_status.keys()),
            "data_statistics": data_stats
        }
        
        return web.json_response({
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "stats": stats,
            "connection_status": connection_status
        })
        
    except Exception as e:
        logger.error(f"获取WebSocket状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)



async def root_handler(request):
    """根路径处理器 - 返回友好的欢迎页面"""
    from .welcome_page import get_welcome_page
    
    html_content = get_welcome_page()
    return web.Response(text=html_content, content_type='text/html')

async def funding_history_test_page(request):
    """历史资金费率测试页面"""
    from .welcome_page import get_funding_history_test_page
    
    html_content = get_funding_history_test_page()
    return web.Response(text=html_content, content_type='text/html')

async def public_ping(request):
    """
    完全公开的健康检查接口 - 用于外部监控网站保持服务器活跃
    只返回最简单的状态，不含任何敏感信息
    """
    data = {
        "status": "alive",
        "timestamp": datetime.datetime.now().isoformat()
    }
    return web.json_response(data, status=200)

async def health_check(request):
    """健康检查 - Render优化版"""
    # 检查HTTP服务状态
    http_ready = data_store.is_http_server_ready()
    
    status_info = {
        "status": "ok" if http_ready else "starting",
        "service": "brain-core-trading",
        "http_server_ready": http_ready,
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    # 如果HTTP服务已就绪，返回200，否则返回202（已接受，但还在处理）
    status_code = 200 if http_ready else 202
    return web.json_response(status_info, status=status_code)

@require_auth
async def get_market_data(request):
    """获取市场数据"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        
        market_data = await data_store.get_market_data(exchange, symbol)
        
        return web.json_response({
            "exchange": exchange,
            "symbol": symbol or "all",
            "data": market_data,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取市场数据失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def get_account_balance(request):
    """获取账户余额"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        
        # 初始化交易所API
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        balance = await api.fetch_account_balance()
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "balance": balance,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取账户余额失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def get_positions(request):
    """获取持仓"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        positions = await api.fetch_positions()
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "positions": positions,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def create_order(request):
    """创建订单"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        data = await request.json()
        
        # 验证必要参数
        required = ['symbol', 'type', 'side', 'amount']
        for field in required:
            if field not in data:
                return web.json_response({"error": f"缺少必要参数: {field}"}, status=400)
        
        symbol = data['symbol']
        order_type = data['type']
        side = data['side']
        amount = float(data['amount'])
        price = float(data.get('price', 0))
        params = data.get('params', {})
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        order = await api.create_order(symbol, order_type, side, amount, price, params)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "order": order,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"创建订单失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def cancel_order(request):
    """取消订单"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        data = await request.json()
        
        if 'symbol' not in data or 'order_id' not in data:
            return web.json_response({"error": "缺少symbol或order_id参数"}, status=400)
        
        symbol = data['symbol']
        order_id = data['order_id']
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        result = await api.cancel_order(symbol, order_id)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "result": result,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"取消订单失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def get_open_orders(request):
    """获取未成交订单"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        orders = await api.fetch_open_orders(symbol)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "open_orders": orders,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取未成交订单失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def get_order_history(request):
    """获取订单历史"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        limit = int(request.query.get('limit', 100))
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        orders = await api.fetch_order_history(symbol, limit=limit)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "order_history": orders,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取订单历史失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def set_leverage(request):
    """设置杠杆"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        data = await request.json()
        
        if 'symbol' not in data or 'leverage' not in data:
            return web.json_response({"error": "缺少symbol或leverage参数"}, status=400)
        
        symbol = data['symbol']
        leverage = int(data['leverage'])
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        result = await api.set_leverage(symbol, leverage)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "result": result,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"设置杠杆失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def get_connection_status(request):
    """获取连接状态"""
    try:
        exchange = request.match_info.get('exchange', '')
        
        connection_status = await data_store.get_connection_status(exchange or None)
        
        return web.json_response({
            "connection_status": connection_status,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取连接状态失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

@require_auth
async def get_ticker(request):
    """获取ticker数据"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        
        if not symbol:
            return web.json_response({"error": "缺少symbol参数"}, status=400)
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        ticker = await api.fetch_ticker(symbol)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "ticker": ticker,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取ticker失败: {e}")
        return web.json_response({"error": str(e)}, status=500)

# ============ 【系统监控相关函数】============
async def system_monitor_placeholder(request):
    """系统监控占位函数"""
    return web.json_response({
        "error": "系统监控模块未安装",
        "hint": "请安装psutil包并确保system_monitor模块存在"
    }, status=501)

async def get_system_health(request):
    """获取系统健康状态（公开访问）"""
    try:
        from system_monitor.collector import SystemMonitor
        monitor = SystemMonitor()
        data = monitor.check_health()
        # 只返回基本信息，不暴露敏感数据
        safe_data = {
            "healthy": data.get("healthy", False),
            "timestamp": data.get("timestamp")
        }
        return web.json_response({
            "success": True,
            "data": safe_data
        })
    except ImportError:
        return system_monitor_placeholder(request)
    except Exception as e:
        logger.error(f"获取系统健康状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)

@require_auth
async def get_system_metrics(request):
    """获取系统核心指标（需要密码）"""
    try:
        from system_monitor.collector import SystemMonitor
        monitor = SystemMonitor()
        data = monitor.collect_light()
        return web.json_response({
            "success": True,
            "data": data
        })
    except ImportError:
        return system_monitor_placeholder(request)
    except Exception as e:
        logger.error(f"获取系统指标失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)

@require_auth
async def get_system_status(request):
    """获取完整系统状态（需要密码）"""
    try:
        from system_monitor.collector import SystemMonitor
        monitor = SystemMonitor()
        data = monitor.collect_all()
        return web.json_response({
            "success": True,
            "data": data
        })
    except ImportError:
        return system_monitor_placeholder(request)
    except Exception as e:
        logger.error(f"获取系统状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)
        
# ============ 【系统监控相关函数结束】============

def setup_routes(app):
    """设置路由"""
    # 根路径
    app.router.add_get('/', root_handler)
    app.router.add_get('/test/funding_history', funding_history_test_page)
    
    # 公开健康检查（不需要认证）
    app.router.add_get('/public/ping', public_ping)
    app.router.add_get('/health', health_check)
    
    # 市场数据
    app.router.add_get('/api/market/{exchange}', get_market_data)
    app.router.add_get('/api/market/{exchange}/{symbol}', get_market_data)
    
    # 账户和交易
    app.router.add_get('/api/account/{exchange}/balance', get_account_balance)
    app.router.add_get('/api/account/{exchange}/positions', get_positions)
    
    # 订单操作
    app.router.add_post('/api/trade/{exchange}/order', create_order)
    app.router.add_post('/api/trade/{exchange}/cancel', cancel_order)
    app.router.add_get('/api/trade/{exchange}/open-orders', get_open_orders)
    app.router.add_get('/api/trade/{exchange}/order-history', get_order_history)
    
    # 杠杆设置
    app.router.add_post('/api/trade/{exchange}/leverage', set_leverage)
    
    # 连接状态
    app.router.add_get('/api/status/connections', get_connection_status)
    app.router.add_get('/api/status/connections/{exchange}', get_connection_status)
    
    # 实时数据
    app.router.add_get('/api/data/{exchange}/ticker', get_ticker)
    
    # ============ 【系统监控路由】============
    app.router.add_get('/api/monitor/health', get_system_health)
    app.router.add_get('/api/monitor/metrics', get_system_metrics)
    app.router.add_get('/api/monitor/status', get_system_status)
    logger.info("✅ 系统监控路由已添加")
    # ============ 【系统监控路由结束】============

    # ============ 【新增调试接口路由】============
    app.router.add_get('/api/debug/all_websocket_data', get_all_websocket_data)
    app.router.add_get('/api/debug/symbol/{exchange}/{symbol}', get_symbol_detail)
    app.router.add_get('/api/debug/websocket_status', get_websocket_status)
    app.router.add_get('/api/debug/funding_rates', get_funding_rates)  #
    logger.info("✅ 调试接口路由已添加")
# ============ 【新增调试接口路由结束】============



