"""
共享内存数据存储
WebSocket数据直接存储在这里，大脑直接从这里读取
修复版：支持按数据类型存储，避免覆盖
"""
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

class DataStore:
    """共享数据存储，线程安全 - 修复版"""
    
    def __init__(self):
        # 交易所实时数据 - 新的数据结构
        # 格式：market_data[exchange][symbol][data_type] = data
        self.market_data = {}
        
        
        # 资金费率结算数据
        # 结构: {"binance": {"BTCUSDT": {"funding_rate": 0.0001, "funding_time": 1234567890000, ...}}}
        self.funding_settlement = {
            "binance": {}
}
        
        # 账户数据
        self.account_data = {}
        # 订单数据
        self.order_data = {}
        # 连接状态
        self.connection_status = {}
        # 新增：HTTP服务就绪状态
        self._http_server_ready = False
        
        # 新增：大脑成品数据回调函数
        self.brain_callback = None
        
        # 锁，确保线程安全
        self.locks = {
            'market_data': asyncio.Lock(),
            'account_data': asyncio.Lock(),
            'order_data': asyncio.Lock(),
            'connection_status': asyncio.Lock(),
        }
    
    # 新增：设置大脑回调函数
    def set_brain_callback(self, callback):
        """设置大脑回调函数（接收成品数据）"""
        self.brain_callback = callback
        logger.info("🧠 数据存储模块：大脑回调已设置")
    
    # 新增：大脑数据推送方法
    async def _push_to_brain(self, processed_data: Dict[str, Any]):
        """推送成品数据给大脑"""
        try:
            if self.brain_callback:
                await self.brain_callback(processed_data)
        except Exception as e:
            logger.error(f"推送数据给大脑失败: {e}")
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """
        更新市场数据 - 修复版
        
        修复问题：相同symbol的ticker数据和资金费率数据相互覆盖
        新结构：market_data[exchange][symbol][data_type] = data
        """
        async with self.locks['market_data']:
            # 初始化数据结构
            if exchange not in self.market_data:
                self.market_data[exchange] = {}
            if symbol not in self.market_data[exchange]:
                self.market_data[exchange][symbol] = {}
            
            # 获取数据类型（ticker, funding_rate, mark_price等）
            data_type = data.get("data_type", "unknown")
            
            # 存储数据，按数据类型分类
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),  # 存储时间
                'source': 'websocket'
            }
            
            # 同时存储一份最新数据引用（便于快速访问）
            self.market_data[exchange][symbol]['latest'] = data_type
            
            # 调试日志：记录不同类型的数据
            if data_type in ['funding_rate', 'mark_price']:
                funding_rate = data.get('funding_rate', 0)
                logger.debug(f"[DataStore] 存储 {exchange} {symbol} {data_type} = {funding_rate:.6f}")
        
        # 新增：异步过滤并推送给大脑
        try:
            # 导入过滤器（避免循环导入）
            from .filter import data_filter
            
            # 使用过滤器处理数据
            processed_data = data_filter.filter_and_process(data)
            
            # 如果过滤通过，推送给大脑
            if processed_data and self.brain_callback:
                # 异步推送，不阻塞主流程
                asyncio.create_task(self._push_to_brain(processed_data))
                
        except ImportError:
            logger.warning("数据过滤器未找到，跳过过滤")
        except Exception as e:
            logger.error(f"过滤推送数据失败: {e}")
    
    async def get_market_data(self, exchange: str, symbol: str = None, 
                             data_type: str = None, get_latest: bool = False) -> Dict[str, Any]:
        """
        获取市场数据 - 增强版
        
        参数：
            exchange: 交易所名称
            symbol: 交易对名称，为None时返回整个交易所数据
            data_type: 数据类型（ticker, funding_rate, mark_price），为None时返回所有类型
            get_latest: 是否只获取最新的一条数据（兼容旧接口）
        """
        async with self.locks['market_data']:
            if exchange not in self.market_data:
                return {}
            
            # 情况1：获取整个交易所数据
            if not symbol:
                result = {}
                for sym, data_dict in self.market_data[exchange].items():
                    if get_latest and 'latest' in data_dict:
                        latest_type = data_dict['latest']
                        result[sym] = data_dict.get(latest_type, {})
                    else:
                        # 移除内部字段
                        clean_dict = {k: v for k, v in data_dict.items() 
                                    if k not in ['latest', 'store_timestamp']}
                        result[sym] = clean_dict
                return result
            
            # 情况2：获取指定symbol的数据
            if symbol not in self.market_data[exchange]:
                return {}
            
            symbol_data = self.market_data[exchange][symbol]
            
            # 情况2.1：获取指定数据类型
            if data_type:
                return symbol_data.get(data_type, {})
            
            # 情况2.2：获取最新数据（兼容旧接口）
            if get_latest and 'latest' in symbol_data:
                latest_type = symbol_data['latest']
                return symbol_data.get(latest_type, {})
            
            # 情况2.3：获取该symbol的所有数据（排除内部字段）
            return {k: v for k, v in symbol_data.items() 
                   if k not in ['latest', 'store_timestamp']}
    
    async def get_funding_rates(self, exchange: str = None, 
                               min_rate: float = None, max_rate: float = None) -> Dict[str, Any]:
        """
        获取资金费率数据 - 专用方法
        
        参数：
            exchange: 交易所名称，为None时返回所有交易所
            min_rate: 最小资金费率（绝对值）
            max_rate: 最大资金费率（绝对值）
        """
        async with self.locks['market_data']:
            result = {}
            
            # 确定要查询的交易所列表
            exchanges = [exchange] if exchange else self.market_data.keys()
            
            for exch in exchanges:
                if exch not in self.market_data:
                    continue
                
                exchange_rates = {}
                for symbol, data_dict in self.market_data[exch].items():
                    # 查找资金费率数据
                    for data_type in ['funding_rate', 'mark_price']:
                        if data_type in data_dict:
                            data = data_dict[data_type]
                            if 'funding_rate' in data:
                                rate = data['funding_rate']
                                
                                # 费率筛选
                                if min_rate is not None and abs(rate) < min_rate:
                                    continue
                                if max_rate is not None and abs(rate) > max_rate:
                                    continue
                                
                                exchange_rates[symbol] = {
                                    'funding_rate': rate,
                                    'next_funding_time': data.get('next_funding_time'),
                                    'mark_price': data.get('mark_price'),
                                    'timestamp': data.get('timestamp'),
                                    'data_type': data_type,
                                    'store_time': data_dict.get('store_timestamp'),
                                    'age_seconds': self._calculate_data_age(data.get('timestamp'))
                                }
                                break  # 找到就跳出
                
                if exchange_rates:
                    result[exch] = {
                        'count': len(exchange_rates),
                        'data': exchange_rates
                    }
            
            return result
    
    def _calculate_data_age(self, timestamp_str: str) -> float:
        """计算数据年龄（秒）"""
        if not timestamp_str:
            return float('inf')
        
        try:
            # 处理各种时间格式
            if 'T' in timestamp_str:
                # ISO格式
                data_time = datetime.fromisoformat(
                    timestamp_str.replace('Z', '+00:00').split('.')[0]
                )
            else:
                # 时间戳格式
                try:
                    ts = float(timestamp_str)
                    if ts > 1e12:  # 毫秒时间戳
                        ts = ts / 1000
                    data_time = datetime.fromtimestamp(ts)
                except:
                    return float('inf')
            
            now = datetime.now()
            return (now - data_time).total_seconds()
        except:
            return float('inf')
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """更新账户数据"""
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_account_data(self, exchange: str) -> Dict[str, Any]:
        """获取账户数据"""
        async with self.locks['account_data']:
            return self.account_data.get(exchange, {}).copy()
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """更新订单数据"""
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
    
    async def get_order_data(self, exchange: str, order_id: str = None) -> Dict[str, Any]:
        """获取订单数据"""
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                return {}
            if order_id:
                return self.order_data[exchange].get(order_id, {})
            return self.order_data[exchange].copy()
    
    async def update_connection_status(self, exchange: str, connection_type: str, status: Dict[str, Any]):
        """更新连接状态"""
        async with self.locks['connection_status']:
            if exchange not in self.connection_status:
                self.connection_status[exchange] = {}
            self.connection_status[exchange][connection_type] = {
                **status,
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_connection_status(self, exchange: str = None) -> Dict[str, Any]:
        """获取连接状态"""
        async with self.locks['connection_status']:
            if exchange:
                return self.connection_status.get(exchange, {}).copy()
            return self.connection_status.copy()
    
    async def get_all_market_data(self) -> Dict[str, Any]:
        """获取所有市场数据（兼容旧代码）"""
        return await self.get_market_data(None)  # 传递None以获取所有交易所
    
    def get_market_data_stats(self) -> Dict[str, Any]:
        """获取数据存储统计信息"""
        stats = {
            'exchanges': {},
            'total_symbols': 0,
            'total_data_types': 0
        }
        
        for exchange, symbols in self.market_data.items():
            symbol_count = len(symbols)
            data_type_count = 0
            
            for symbol, data_dict in symbols.items():
                # 排除内部字段
                valid_types = [k for k in data_dict.keys() 
                             if k not in ['latest', 'store_timestamp']]
                data_type_count += len(valid_types)
            
            stats['exchanges'][exchange] = {
                'symbols': symbol_count,
                'data_types': data_type_count
            }
            stats['total_symbols'] += symbol_count
            stats['total_data_types'] += data_type_count
        
        return stats
    
    # 新增：HTTP服务状态管理
    def set_http_server_ready(self, ready: bool):
        """设置HTTP服务就绪状态"""
        self._http_server_ready = ready
    
    def is_http_server_ready(self) -> bool:
        """检查HTTP服务是否就绪"""
        return self._http_server_ready

# 全局数据存储实例
data_store = DataStore()

# 配置日志
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())  # 避免无配置时的错误
