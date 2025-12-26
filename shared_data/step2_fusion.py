# shared_data/step2_fusion.py
"""
第二步：融合数据源
将5种数据源融合成统一格式
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

class Step2Fusion:
    """第二步：数据融合"""
    
    def __init__(self):
        # 临时存储，等待数据齐全
        self.temp_storage = defaultdict(lambda: defaultdict(dict))
        logger.info("✅ Step2Fusion 初始化完成")
    
    async def process(self, filtered_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        处理过滤后的数据，按symbol融合
        
        输入：单个过滤后的数据
        输出：融合后的数据列表（可能为None，表示还在等待数据）
        """
        try:
            exchange = filtered_data.get("exchange")
            symbol = filtered_data.get("symbol")
            data_type = filtered_data.get("data_type")
            
            if not all([exchange, symbol, data_type]):
                return None
            
            # 存储到临时缓存
            self.temp_storage[symbol][exchange][data_type] = filtered_data
            
            # 检查是否有完整的数据可以融合
            return await self._check_and_fuse(symbol)
            
        except Exception as e:
            logger.error(f"Step2 处理失败: {e}, 数据: {filtered_data}")
            return None
    
    async def _check_and_fuse(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """检查并融合一个symbol的所有数据"""
        symbol_data = self.temp_storage[symbol]
        
        results = []
        
        # 处理每个交易所的数据
        for exchange, data_dict in symbol_data.items():
            fused = self._fuse_exchange_data(exchange, symbol, data_dict)
            if fused:
                results.append(fused)
        
        # 如果有融合结果，清理临时数据
        if results:
            del self.temp_storage[symbol]
            return results
        
        return None
    
    def _fuse_exchange_data(self, exchange: str, symbol: str, data_dict: Dict) -> Optional[Dict]:
        """融合单个交易所的数据"""
        try:
            fused_data = {
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "source": "step2"
            }
            
            # 提取各类型数据
            ticker_data = data_dict.get("ticker")
            funding_data = data_dict.get("funding_rate")
            mark_price_data = data_dict.get("mark_price")
            historical_data = data_dict.get("historical_funding")
            
            # 融合价格数据
            if ticker_data:
                fused_data["price"] = ticker_data.get("last_price")
                fused_data["price_timestamp"] = ticker_data.get("timestamp")
            
            # 融合资金费率数据
            funding_sources = []
            if funding_data:
                funding_sources.append(funding_data)
            if mark_price_data:
                funding_sources.append(mark_price_data)  # mark_price也是资金费率数据
            
            if funding_sources:
                # 取最新的资金费率数据
                latest_funding = max(funding_sources, key=lambda x: x.get("timestamp", ""))
                fused_data["funding_rate"] = latest_funding.get("funding_rate")
                fused_data["current_settlement_time"] = latest_funding.get("current_settlement_time")
                fused_data["next_settlement_time"] = latest_funding.get("next_settlement_time")
                fused_data["funding_timestamp"] = latest_funding.get("timestamp")
            
            # 融合历史数据（币安专用）
            if historical_data and exchange == EXCHANGE_BINANCE:
                fused_data["last_settlement_time"] = historical_data.get("last_settlement_time")
                fused_data["historical_funding_rate"] = historical_data.get("funding_rate")
                fused_data["historical_timestamp"] = historical_data.get("timestamp")
            
            # 必须有价格或费率数据才算有效
            if "price" in fused_data or "funding_rate" in fused_data:
                return fused_data
            
            return None
            
        except Exception as e:
            logger.error(f"融合交易所数据失败 {exchange}/{symbol}: {e}")
            return None
    
    async def cleanup_old_data(self, max_age_seconds: int = 300):
        """清理过期的临时数据"""
        current_time = datetime.now()
        expired_symbols = []
        
        for symbol, symbol_data in self.temp_storage.items():
            # 检查数据是否过期（需要更精确的时间戳检查）
            # 这里简化处理，只检查条目数量
            if len(symbol_data) == 0:
                expired_symbols.append(symbol)
        
        for symbol in expired_symbols:
            del self.temp_storage[symbol]
            logger.debug(f"Step2 清理过期数据: {symbol}")