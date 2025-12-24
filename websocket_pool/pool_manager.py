# websocket_pool/pool_manager.py
"""
WebSocket连接池总管理器 - 日志规范版
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any, List, Optional
import ccxt.async_support as ccxt_async

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .exchange_pool import ExchangeWebSocketPool
from .config import EXCHANGE_CONFIGS
from .static_symbols import STATIC_SYMBOLS

logger = logging.getLogger(__name__)

# 默认数据回调函数
async def default_data_callback(data):
    try:
        if not data:
            return
            
        exchange = data.get("exchange", "")
        symbol = data.get("symbol", "")
        
        if not exchange or not symbol:
            logger.warning(f"[管理器] 数据缺少字段: exchange={exchange}, symbol={symbol}")
            return
        
        await data_store.update_market_data(exchange, symbol, data)
        
        # 计数日志（每100条）
        default_data_callback.counter = getattr(default_data_callback, 'counter', 0) + 1
        if default_data_callback.counter % 100 == 0:
            logger.info(f"[管理器] 已处理 {default_data_callback.counter} 条数据")
            
    except Exception as e:
        logger.error(f"[管理器] 数据回调错误: {e}")

class WebSocketPoolManager:
    """WebSocket连接池管理器"""
    
    def __init__(self, data_callback=None):
        self.data_callback = data_callback or default_data_callback
        self.exchange_pools = {}
        self.initialized = False
        self._initializing = False
        self._shutting_down = False
        
        logger.info("[管理器] WebSocketPoolManager 初始化完成")
        
    async def initialize(self):
        """初始化所有交易所连接池"""
        if self.initialized or self._initializing:
            logger.warning("[管理器] 连接池已在初始化或已初始化，跳过")
            return
        
        self._initializing = True
        logger.info(f"{'=' * 60}")
        logger.info("[管理器] 正在初始化所有交易所连接池...")
        logger.info(f"{'=' * 60}")
        
        try:
            # 并发初始化所有交易所
            exchange_tasks = []
            exchange_names = []
            for exchange_name in ["binance", "okx"]:
                if exchange_name in EXCHANGE_CONFIGS:
                    task = asyncio.create_task(
                        self._setup_exchange_pool(exchange_name),
                        name=f"init_{exchange_name}"
                    )
                    exchange_tasks.append(task)
                    exchange_names.append(exchange_name)
            
            if exchange_tasks:
                results = await asyncio.gather(*exchange_tasks, return_exceptions=True)
                
                # 🎯 为每个交易所输出独立的成功/失败日志
                for exchange_name, result in zip(exchange_names, results):
                    if isinstance(result, Exception):
                        logger.error(f"[管理器] ❌ {exchange_name} 初始化失败: {result}")
                    else:
                        # 🎯 获取该交易所的统计信息
                        pool = self.exchange_pools.get(exchange_name)
                        if pool:
                            masters = len(pool.master_connections)
                            standbys = len(pool.warm_standby_connections)
                            logger.info(f"[管理器] ✅ {exchange_name} 初始化成功 ({masters}主/{standbys}备)")
            
            self.initialized = True
            logger.info("[管理器] 所有交易所连接池初始化完成！")
            
        except Exception as e:
            logger.error(f"[管理器] 初始化异常: {e}")
        finally:
            self._initializing = False
        
        logger.info(f"{'=' * 60}")

    async def _setup_exchange_pool(self, exchange_name: str):
        """设置单个交易所连接池"""
        logger.info(f"[管理器] [{exchange_name}] 开始设置连接池...")
        
        try:
            symbols = await self._fetch_exchange_symbols(exchange_name)
            
            if not symbols:
                logger.warning(f"[管理器] [{exchange_name}] API获取失败，使用静态合约列表")
                symbols = self._get_static_symbols(exchange_name)
            
            if not symbols:
                logger.error(f"[管理器] [{exchange_name}] 无法获取任何合约，跳过")
                return
            
            logger.info(f"[管理器] [{exchange_name}] 成功获取 {len(symbols)} 个合约")
            
            # 限制合约数量
            active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
            symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
            max_symbols = symbols_per_conn * active_connections
            
            if len(symbols) > max_symbols:
                logger.info(f"[管理器] [{exchange_name}] 合约数量超限 {len(symbols)} > {max_symbols}，裁剪")
                symbols = symbols[:max_symbols]
            
            # 初始化连接池
            pool = ExchangeWebSocketPool(exchange_name, self.data_callback)
            await pool.initialize(symbols)
            self.exchange_pools[exchange_name] = pool
            
            logger.info(f"[管理器] ✅ [{exchange_name}] 连接池设置成功")
            
        except Exception as e:
            logger.error(f"[管理器] [{exchange_name}] 设置失败: {e}")
            raise

    # ==================== 以下方法保持不变 ====================

    async def _fetch_exchange_symbols(self, exchange_name: str) -> List[str]:
        """获取交易所合约列表 - 稳健版"""
        symbols = await self._fetch_symbols_via_api(exchange_name)
        if symbols:
            logger.info(f"[管理器] ✅ [{exchange_name}] API获取成功: {len(symbols)} 个合约")
            return symbols
        
        logger.warning(f"[管理器] [{exchange_name}] API失败，使用静态列表")
        return self._get_static_symbols(exchange_name)
    
    async def _fetch_symbols_via_api(self, exchange_name: str) -> List[str]:
        """通过API获取合约"""
        exchange = None
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                config = self._get_exchange_config(exchange_name)
                exchange_class = getattr(ccxt_async, exchange_name)
                exchange = exchange_class(config)
                
                logger.info(f"[管理器] [{exchange_name}] 加载市场数据... (尝试 {attempt}/{max_retries})")
                
                if exchange_name == "okx":
                    markets = await exchange.fetch_markets(params={'instType': 'SWAP'})
                    markets_dict = {}
                    for market in markets:
                        symbol = market.get('symbol', '')
                        if symbol:
                            markets_dict[symbol.upper()] = market
                    markets = markets_dict
                else:
                    markets = await exchange.load_markets()
                    markets = {k.upper(): v for k, v in markets.items()}
                
                logger.info(f"[管理器] [{exchange_name}] 市场数据加载完成: {len(markets)} 个")
                
                filtered_symbols = self._filter_and_format_symbols(exchange_name, markets)
                
                await exchange.close()
                return filtered_symbols
                
            except Exception as e:
                error_detail = str(e) if e else '未知错误'
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f'[管理器] [{exchange_name}] 第{attempt}次失败，{wait_time}秒后重试: {error_detail}')
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f'[管理器] [{exchange_name}] 所有{max_retries}次尝试均失败: {error_detail}')
                    if exchange:
                        await exchange.close()
                    return []

    def _get_exchange_config(self, exchange_name: str) -> dict:
        """获取交易所配置"""
        base_config = {
            'apiKey': '',
            'secret': '',
            'enableRateLimit': True,
            'timeout': 30000,
        }
        
        if exchange_name == "okx":
            base_config.update({
                'options': {
                    'defaultType': 'swap',
                    'fetchMarketDataRateLimit': 2000,
                }
            })
        elif exchange_name == "binance":
            base_config.update({
                'options': {
                    'defaultType': 'future',
                    'warnOnFetchOHLCVLimitArgument': False,
                }
            })
        
        return base_config

    def _filter_and_format_symbols(self, exchange_name: str, markets: dict) -> List[str]:
        """筛选和格式化合约"""
        all_usdt_symbols = []
        
        for symbol, market in markets.items():
            try:
                symbol_upper = symbol.upper()
                
                if exchange_name == "binance":
                    is_perpetual = market.get('swap', False) or market.get('linear', False)
                    is_active = market.get('active', False)
                    is_usdt = '/USDT' in symbol_upper
                    
                    if is_perpetual and is_active and is_usdt:
                        parts = symbol_upper.split('/')
                        if len(parts) >= 2:
                            base_symbol = parts[0]
                            if ':USDT' in base_symbol:
                                base_symbol = base_symbol.split(':')[0]
                            
                            clean_symbol = f"{base_symbol}USDT"
                            if clean_symbol.endswith('USDTUSDT'):
                                clean_symbol = clean_symbol[:-4]
                            
                            all_usdt_symbols.append(clean_symbol)
                
                elif exchange_name == "okx":
                    market_type = market.get('type', '').upper()
                    quote = market.get('quote', '').upper()
                    
                    is_swap = market_type == 'SWAP' or market.get('swap', False)
                    is_usdt_quote = quote == 'USDT' or '-USDT-' in symbol_upper
                    
                    if is_swap and is_usdt_quote:
                        if '-USDT-SWAP' in symbol_upper:
                            clean_symbol = symbol.upper()
                        elif '/USDT:USDT' in symbol_upper:
                            clean_symbol = symbol.replace('/USDT:USDT', '-USDT-SWAP').upper()
                        else:
                            inst_id = market.get('info', {}).get('instId', '')
                            if inst_id and '-USDT-SWAP' in inst_id.upper():
                                clean_symbol = inst_id.upper()
                            else:
                                continue
                        
                        all_usdt_symbols.append(clean_symbol)
                
            except Exception as e:
                logger.debug(f"[管理器] [{exchange_name}] 处理市场 {symbol} 跳过: {e}")
                continue
        
        symbols = sorted(list(set(all_usdt_symbols)))
        
        if symbols:
            logger.info(f"[管理器] ✅ [{exchange_name}] 找到 {len(symbols)} 个USDT永续合约")
            logger.info(f"[管理器] [{exchange_name}] 前10个示例: {symbols[:10]}")
        else:
            logger.warning(f"[管理器] [{exchange_name}] 未找到USDT永续合约")
        
        return symbols
    
    def _get_static_symbols(self, exchange_name: str) -> List[str]:
        """获取静态合约列表"""
        return STATIC_SYMBOLS.get(exchange_name, [])
    
    async def get_all_status(self) -> Dict[str, Any]:
        """获取所有交易所连接状态"""
        status = {}
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                pool_status = await pool.get_status()
                status[exchange_name] = pool_status
            except Exception as e:
                logger.error(f"[管理器] [{exchange_name}] 获取状态错误: {e}")
                status[exchange_name] = {"error": str(e)}
        
        return status
    
    async def shutdown(self):
        """关闭所有连接池 - 防重入版"""
        if self._shutting_down:
            logger.info("[管理器] 连接池已在关闭中，跳过")
            return
        
        self._shutting_down = True
        logger.info("[管理器] 正在关闭所有WebSocket连接池...")
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                await pool.shutdown()
                logger.info(f"[管理器] ✅ [{exchange_name}] 连接池已关闭")
            except Exception as e:
                logger.error(f"[管理器] [{exchange_name}] 关闭时出错: {e}")
        
        logger.info("[管理器] 所有WebSocket连接池已关闭")
