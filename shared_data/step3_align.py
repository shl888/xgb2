# shared_data/step3_align.py
"""
第三步：筛选孤立合约 + 时间转换
只保留双平台都有的合约，将UTC时间转换为北京时间
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class Step3Align:
    """第三步：筛选对齐和时间转换"""
    
    def __init__(self):
        # 存储各交易所的合约数据
        self.binance_symbols = {}
        self.okx_symbols = {}
        
        # 北京时间偏移
        self.beijing_offset = timedelta(hours=8)
        logger.info("✅ Step3Align 初始化完成")
    
    async def process(self, fused_data_list: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        处理融合后的数据
        
        输入：融合后的数据列表
        输出：筛选对齐后的数据列表（双平台都有的合约）
        """
        try:
            if not fused_data_list:
                return None
            
            results = []
            
            for fused_data in fused_data_list:
                exchange = fused_data.get("exchange")
                symbol = fused_data.get("symbol")
                
                if not exchange or not symbol:
                    continue
                
                # 存储到对应的交易所缓存
                if exchange == "binance":
                    self.binance_symbols[symbol] = fused_data
                elif exchange == "okx":
                    self.okx_symbols[symbol] = fused_data
                
                # 检查是否有对应的另一平台数据
                aligned_data = await self._check_and_align(symbol)
                if aligned_data:
                    results.extend(aligned_data)
            
            return results if results else None
            
        except Exception as e:
            logger.error(f"Step3 处理失败: {e}")
            return None
    
    async def _check_and_align(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """检查并对齐一个symbol的双平台数据"""
        binance_data = self.binance_symbols.get(symbol)
        okx_data = self.okx_symbols.get(symbol)
        
        if not binance_data or not okx_data:
            return None
        
        # 转换时间格式并添加标记
        processed_binance = await self._convert_time_and_mark(binance_data, "binance")
        processed_okx = await self._convert_time_and_mark(okx_data, "okx")
        
        # 清理缓存
        del self.binance_symbols[symbol]
        del self.okx_symbols[symbol]
        
        return [processed_binance, processed_okx]
    
    async def _convert_time_and_mark(self, data: Dict[str, Any], exchange: str) -> Dict[str, Any]:
        """转换时间格式并添加对齐标记"""
        result = data.copy()
        
        # 添加对齐标记
        result["aligned"] = True
        result["aligned_at"] = datetime.now().isoformat()
        
        # 转换时间字段（如果存在）
        time_fields = [
            "current_settlement_time",
            "next_settlement_time", 
            "last_settlement_time",
            "price_timestamp",
            "funding_timestamp",
            "historical_timestamp"
        ]
        
        for field in time_fields:
            if field in result and result[field]:
                beijing_time = self._convert_to_beijing(result[field])
                if beijing_time:
                    result[f"{field}_beijing"] = beijing_time
                    # 保留原始时间戳
                    result[f"{field}_original"] = result[field]
        
        return result
    
    def _convert_to_beijing(self, timestamp) -> Optional[str]:
        """将时间戳转换为北京时间"""
        try:
            if isinstance(timestamp, (int, float)):
                # 毫秒时间戳
                if timestamp > 1e12:
                    timestamp = timestamp / 1000
                
                utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                beijing_time = utc_time + self.beijing_offset
                return beijing_time.isoformat()
            
            elif isinstance(timestamp, str):
                # ISO格式字符串
                if 'Z' in timestamp:
                    timestamp = timestamp.replace('Z', '+00:00')
                
                try:
                    utc_time = datetime.fromisoformat(timestamp)
                    if utc_time.tzinfo is None:
                        utc_time = utc_time.replace(tzinfo=timezone.utc)
                    
                    beijing_time = utc_time + self.beijing_offset
                    return beijing_time.isoformat()
                except:
                    # 如果不是ISO格式，尝试其他解析
                    pass
            
            return None
            
        except Exception as e:
            logger.debug(f"时间转换失败: {e}, timestamp: {timestamp}")
            return None
    
    async def cleanup_stale_data(self, max_age_seconds: int = 600):
        """清理过期的孤立数据"""
        current_time = datetime.now()
        
        # 检查币安数据
        stale_binance = []
        for symbol, data in self.binance_symbols.items():
            timestamp_str = data.get("timestamp")
            if timestamp_str and self._is_data_stale(timestamp_str, max_age_seconds):
                stale_binance.append(symbol)
        
        # 检查欧意数据
        stale_okx = []
        for symbol, data in self.okx_symbols.items():
            timestamp_str = data.get("timestamp")
            if timestamp_str and self._is_data_stale(timestamp_str, max_age_seconds):
                stale_okx.append(symbol)
        
        # 清理过期数据
        for symbol in stale_binance:
            del self.binance_symbols[symbol]
            logger.debug(f"Step3 清理过期币安数据: {symbol}")
        
        for symbol in stale_okx:
            del self.okx_symbols[symbol]
            logger.debug(f"Step3 清理过期欧意数据: {symbol}")
    
    def _is_data_stale(self, timestamp_str: str, max_age_seconds: int) -> bool:
        """检查数据是否过期"""
        try:
            data_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            age = (datetime.now() - data_time).total_seconds()
            return age > max_age_seconds
        except:
            return True