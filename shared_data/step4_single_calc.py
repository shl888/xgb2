# shared_data/step4_single_calc.py
"""
第四步：单平台计算
处理单个交易所的数据，重点是币安的结算时间滚动逻辑
"""

import logging
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

class Step4SingleCalc:
    """第四步：单平台计算"""
    
    def __init__(self):
        # 核心缓存：每个symbol的结算时间数据
        self.settlement_cache = defaultdict(lambda: {
            "binance": {
                "last_settlement_time": None,
                "current_settlement_time": None,
                "last_funding_rate": None,
                "last_update": None
            },
            "okx": {
                "current_settlement_time": None,
                "next_settlement_time": None,
                "last_funding_rate": None,
                "last_update": None
            }
        })
        
        # 数据缓存：完整的单平台数据
        self.data_cache = {}
        
        logger.info("✅ Step4SingleCalc 初始化完成 - 包含币安T2→T1逻辑")
    
    async def process(self, aligned_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理对齐后的单平台数据
        
        输入：单个交易所的对齐数据
        输出：计算后的单平台数据
        """
        try:
            exchange = aligned_data.get("exchange")
            symbol = aligned_data.get("symbol")
            
            if not exchange or not symbol:
                logger.warning(f"Step4: 数据缺少必要字段: {aligned_data}")
                return None
            
            # 获取当前结算时间
            current_time = aligned_data.get("current_settlement_time")
            funding_rate = aligned_data.get("funding_rate")
            
            if not current_time or funding_rate is None:
                logger.debug(f"Step4: 数据缺少结算时间或费率: {symbol}")
                return None
            
            # 执行核心逻辑：结算时间更新和周期计算
            calculation_result = await self._update_settlement_and_calculate(
                exchange, symbol, current_time, funding_rate
            )
            
            if not calculation_result:
                return None
            
            # 构建完整结果
            result = aligned_data.copy()
            
            # 添加计算结果
            result["calculation"] = calculation_result
            
            # 添加缓存时间
            result["cached_at"] = datetime.now().isoformat()
            
            # 存储到数据缓存
            cache_key = f"{exchange}_{symbol}"
            self.data_cache[cache_key] = {
                "data": result,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.debug(f"Step4 处理完成: {exchange}/{symbol}, "
                        f"费率: {funding_rate:.6f}, "
                        f"周期: {calculation_result.get('period_seconds')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Step4 处理失败: {e}, 数据: {aligned_data}")
            return None
    
    async def _update_settlement_and_calculate(self, exchange: str, symbol: str, 
                                             new_current_time, new_funding_rate):
        """
        核心逻辑：更新结算时间并计算周期
        
        严格按照：T2→T1, T3→T2 的逻辑
        """
        try:
            # 获取当前缓存状态
            cache_key = symbol
            exchange_cache = self.settlement_cache[cache_key][exchange]
            
            last_time = exchange_cache["last_settlement_time"]
            current_time = exchange_cache["current_settlement_time"]
            
            result = {
                "exchange": exchange,
                "symbol": symbol,
                "action": "unknown",
                "period_seconds": None,
                "time_updated": False,
                "old_state": {
                    "last": last_time,
                    "current": current_time,
                    "rate": exchange_cache.get("last_funding_rate")
                },
                "new_state": {
                    "last": None,
                    "current": None,
                    "rate": new_funding_rate
                }
            }
            
            # 情况1：首次设置（缓存为空）
            if current_time is None:
                exchange_cache["current_settlement_time"] = new_current_time
                exchange_cache["last_funding_rate"] = new_funding_rate
                exchange_cache["last_update"] = datetime.now().isoformat()
                
                result["action"] = "initialized"
                result["new_state"]["current"] = new_current_time
                result["message"] = "首次设置结算时间"
                return result
            
            # 情况2：新的结算时间到来（T3 ≠ T2）
            if new_current_time != current_time:
                # 核心逻辑：T2 → T1, T3 → T2
                new_last_time = current_time  # T2变成新的T1
                new_current_time = new_current_time  # T3变成新的T2
                
                # 更新缓存
                exchange_cache["last_settlement_time"] = new_last_time
                exchange_cache["current_settlement_time"] = new_current_time
                exchange_cache["last_funding_rate"] = new_funding_rate
                exchange_cache["last_update"] = datetime.now().isoformat()
                
                # 计算周期（如果有足够的时间点）
                period_seconds = None
                if new_last_time and new_current_time:
                    period_ms = new_current_time - new_last_time
                    period_seconds = period_ms / 1000
                
                result["action"] = "time_advanced"
                result["time_updated"] = True
                result["period_seconds"] = period_seconds
                result["new_state"]["last"] = new_last_time
                result["new_state"]["current"] = new_current_time
                result["message"] = f"结算时间前进: {current_time}→last, {new_current_time}→current"
                
                # 记录时间变化
                if period_seconds:
                    hours = period_seconds / 3600
                    logger.debug(f"Step4 {exchange}/{symbol} 结算周期: {hours:.1f}小时")
                
                return result
            
            # 情况3：同一结算时间，费率更新
            else:
                # 只更新费率，不改变时间
                exchange_cache["last_funding_rate"] = new_funding_rate
                exchange_cache["last_update"] = datetime.now().isoformat()
                
                # 计算周期（基于已有时间）
                period_seconds = None
                if last_time and current_time:
                    period_ms = current_time - last_time
                    period_seconds = period_ms / 1000
                
                result["action"] = "rate_updated"
                result["time_updated"] = False
                result["period_seconds"] = period_seconds
                result["new_state"]["last"] = last_time
                result["new_state"]["current"] = current_time
                result["message"] = "同一结算时间内费率更新"
                
                return result
            
        except Exception as e:
            logger.error(f"结算时间更新失败 {exchange}/{symbol}: {e}")
            return None
    
    async def get_cached_data(self, exchange: str = None, symbol: str = None) -> Dict:
        """获取缓存数据"""
        if exchange and symbol:
            cache_key = f"{exchange}_{symbol}"
            return self.data_cache.get(cache_key, {})
        elif exchange:
            return {k: v for k, v in self.data_cache.items() if k.startswith(f"{exchange}_")}
        else:
            return self.data_cache.copy()
    
    async def get_settlement_state(self, symbol: str) -> Dict:
        """获取结算时间状态"""
        return dict(self.settlement_cache.get(symbol, {}))
    
    async def cleanup_old_cache(self, max_age_hours: int = 24):
        """清理旧缓存"""
        current_time = datetime.now()
        expired_keys = []
        
        for key, cache_item in self.data_cache.items():
            cache_time_str = cache_item.get("timestamp")
            if cache_time_str:
                try:
                    cache_time = datetime.fromisoformat(cache_time_str)
                    age_hours = (current_time - cache_time).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        expired_keys.append(key)
                except:
                    expired_keys.append(key)
        
        for key in expired_keys:
            del self.data_cache[key]
        
        # 清理settlement_cache中没有对应data_cache的条目
        symbols_in_data = {k.split('_')[1] for k in self.data_cache.keys() if '_' in k}
        symbols_to_remove = set(self.settlement_cache.keys()) - symbols_in_data
        
        for symbol in symbols_to_remove:
            del self.settlement_cache[symbol]
        
        if expired_keys or symbols_to_remove:
            logger.info(f"Step4 缓存清理完成: {len(expired_keys)}数据, {len(symbols_to_remove)}结算状态")
            