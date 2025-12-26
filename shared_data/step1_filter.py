# shared_data/step1_filter.py
"""
第一步：过滤提取关键项
从原始数据中提取关键字段，去除非必要数据
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# 🚨 修复：定义常量
DATA_TYPE_TICKER = "ticker"
DATA_TYPE_FUNDING_RATE = "funding_rate"
DATA_TYPE_MARK_PRICE = "mark_price"
DATA_TYPE_HISTORICAL_FUNDING = "historical_funding"

EXCHANGE_BINANCE = "binance"
EXCHANGE_OKX = "okx"

class Step1Filter:
    """第一步：数据过滤提取"""
    
    def __init__(self):
        logger.info("✅ Step1Filter 初始化完成")
    
    async def process(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理原始数据，提取关键字段
        
        输入：原始完整数据
        输出：精简的关键字段数据
        """
        try:
            exchange = raw_data.get("exchange", "")
            data_type = raw_data.get("data_type", "")
            symbol = raw_data.get("symbol", "")
            
            if not all([exchange, data_type, symbol]):
                logger.warning(f"Step1: 数据缺少必要字段: {raw_data}")
                return None
            
            # 根据数据类型进行不同的提取
            if data_type == DATA_TYPE_TICKER:
                return await self._process_ticker(raw_data, exchange, symbol)
            elif data_type == DATA_TYPE_FUNDING_RATE:
                return await self._process_funding_rate(raw_data, exchange, symbol)
            elif data_type == DATA_TYPE_MARK_PRICE:
                return await self._process_mark_price(raw_data, exchange, symbol)
            elif data_type == DATA_TYPE_HISTORICAL_FUNDING:
                return await self._process_historical_funding(raw_data, exchange, symbol)
            else:
                # 其他数据类型直接传递
                return raw_data
                
        except Exception as e:
            logger.error(f"Step1 处理失败: {e}, 数据: {raw_data}")
            return None
    
    async def _process_ticker(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """处理ticker数据"""
        # 从raw_data中提取价格
        raw_data_content = raw_data.get("raw_data", {})
        
        if exchange == EXCHANGE_BINANCE:
            last_price = float(raw_data_content.get("c", 0))
        elif exchange == EXCHANGE_OKX:
            data_list = raw_data_content.get("data", [{}])
            if data_list:
                last_price = float(data_list[0].get("last", 0))
            else:
                last_price = 0
        else:
            last_price = 0
        
        return {
            "exchange": exchange,
            "symbol": symbol,
            "data_type": DATA_TYPE_TICKER,
            "last_price": last_price,
            "timestamp": raw_data.get("timestamp"),
            "source": "step1"
        }
    
    async def _process_funding_rate(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """处理实时资金费率数据"""
        raw_data_content = raw_data.get("raw_data", {})
        funding_rate = 0.0
        current_time = None
        next_time = None
        
        if exchange == EXCHANGE_BINANCE:
            funding_rate = float(raw_data_content.get("r", 0))
            current_time = raw_data_content.get("T")  # 本次结算时间
        elif exchange == EXCHANGE_OKX:
            data_list = raw_data_content.get("data", [{}])
            if data_list:
                funding_rate = float(data_list[0].get("fundingRate", 0))
                current_time = data_list[0].get("fundingTime")  # 本次结算时间
                next_time = data_list[0].get("nextFundingTime")  # 下次结算时间
        
        return {
            "exchange": exchange,
            "symbol": symbol,
            "data_type": DATA_TYPE_FUNDING_RATE,
            "funding_rate": funding_rate,
            "current_settlement_time": current_time,
            "next_settlement_time": next_time,
            "timestamp": raw_data.get("timestamp"),
            "source": "step1"
        }
    
    async def _process_mark_price(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """处理标记价格数据（币安的资金费率在markPrice里）"""
        raw_data_content = raw_data.get("raw_data", {})
        
        funding_rate = float(raw_data_content.get("r", 0))
        current_time = raw_data_content.get("T")  # 本次结算时间
        
        return {
            "exchange": exchange,
            "symbol": symbol,
            "data_type": DATA_TYPE_FUNDING_RATE,  # 转换为funding_rate类型
            "funding_rate": funding_rate,
            "current_settlement_time": current_time,
            "timestamp": raw_data.get("timestamp"),
            "source": "step1"
        }
    
    async def _process_historical_funding(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """处理历史资金费率数据（币安专用）"""
        funding_rate = raw_data.get("funding_rate", 0)
        funding_time = raw_data.get("funding_time")  # 上次结算时间
        
        return {
            "exchange": exchange,
            "symbol": symbol,
            "data_type": DATA_TYPE_HISTORICAL_FUNDING,
            "funding_rate": funding_rate,
            "last_settlement_time": funding_time,  # 注意字段名
            "timestamp": raw_data.get("timestamp"),
            "source": "step1"
        }
        