# shared_data/step5_cross_calc.py
"""
第五步：跨平台计算 + 推送
只计算价格差、费率差、倒计时，生成成品数据
"""

import logging
import asyncio
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class Step5CrossCalc:
    """第五步：跨平台计算和推送（简化版）"""
    
    def __init__(self):
        # 缓存各交易所的完整数据
        self.exchange_cache = {
            "binance": {},  # symbol -> 完整数据
            "okx": {}       # symbol -> 完整数据
        }
        
        # 大脑回调函数
        self.brain_callback = None
        
        # 统计
        self.stats = {
            "processed": 0,
            "pushed": 0,
            "errors": 0
        }
        
        logger.info("✅ Step5CrossCalc 初始化完成（简化版）")
    
    def set_brain_callback(self, callback: Callable):
        """设置大脑回调函数"""
        self.brain_callback = callback
        logger.info("🧠 Step5: 大脑回调已设置")
    
    async def process(self, single_calc_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单平台计算后的数据
        
        输入：单个交易所的计算后数据
        输出：跨平台计算结果（成品数据）
        """
        try:
            exchange = single_calc_data.get("exchange")
            symbol = single_calc_data.get("symbol")
            
            if not exchange or not symbol:
                return None
            
            # 存储到交易所缓存
            self.exchange_cache[exchange][symbol] = single_calc_data
            
            # 检查是否有对应的另一平台数据
            final_result = await self._check_and_calculate(symbol)
            
            if final_result:
                # 推送给大脑
                await self._push_to_brain(final_result)
                
                # 更新统计
                self.stats["processed"] += 1
                self.stats["pushed"] += 1
                
                return final_result
            
            return None
            
        except Exception as e:
            logger.error(f"Step5 处理失败: {e}")
            self.stats["errors"] += 1
            return None
    
    async def process_special_data(self, special_data: Dict[str, Any]) -> None:
        """
        处理特殊数据（交易/账户数据），直接推送给大脑
        """
        try:
            if not self.brain_callback:
                return
            
            await self.brain_callback(special_data)
            
        except Exception as e:
            logger.error(f"Step5 推送特殊数据失败: {e}")
    
    async def _check_and_calculate(self, symbol: str) -> Optional[Dict[str, Any]]:
        """检查并计算跨平台数据"""
        binance_data = self.exchange_cache["binance"].get(symbol)
        okx_data = self.exchange_cache["okx"].get(symbol)
        
        if not binance_data or not okx_data:
            return None
        
        try:
            # 计算最终成品数据
            final_data = await self._calculate_final_data(symbol, binance_data, okx_data)
            
            # 清理缓存
            del self.exchange_cache["binance"][symbol]
            del self.exchange_cache["okx"][symbol]
            
            return final_data
            
        except Exception as e:
            logger.error(f"计算最终数据失败 {symbol}: {e}")
            return None
    
    async def _calculate_final_data(self, symbol: str, 
                                  binance_data: Dict, 
                                  okx_data: Dict) -> Dict[str, Any]:
        """计算最终成品数据"""
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        
        # 提取币安数据
        binance_price = binance_data.get("price")
        binance_rate = binance_data.get("funding_rate", 0)
        binance_current = binance_data.get("current_settlement_time")
        
        # 币安周期（从Step4的计算结果中获取）
        binance_period = None
        binance_calc = binance_data.get("calculation", {})
        if binance_calc:
            binance_period = binance_calc.get("period_seconds")
        
        # 计算币安倒计时（秒）
        binance_countdown = None
        if binance_current:
            binance_countdown = max(0, (binance_current - now_ms) / 1000)
        
        # 提取欧意数据
        okx_price = okx_data.get("price")
        okx_rate = okx_data.get("funding_rate", 0)
        okx_current = okx_data.get("current_settlement_time")
        okx_next = okx_data.get("next_settlement_time")
        
        # 欧意周期
        okx_period = None
        if okx_current and okx_next:
            okx_period = (okx_next - okx_current) / 1000
        
        # 计算欧意倒计时（秒）
        okx_countdown = None
        if okx_current:
            okx_countdown = max(0, (okx_current - now_ms) / 1000)
        
        # 计算价格差
        price_diff = None
        if binance_price is not None and okx_price is not None:
            price_diff = binance_price - okx_price
        
        # 计算费率差
        rate_diff = None
        if binance_rate is not None and okx_rate is not None:
            rate_diff = binance_rate - okx_rate
        
        # 构建最终成品数据
        final_data = {
            # 基本信息
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "data_type": "arbitrage_opportunity",
            "source": "step5",
            
            # 币安数据
            "binance": {
                "price": binance_price,
                "funding_rate": binance_rate,
                "period_seconds": binance_period,
                "current_settlement_time": binance_current,
                "countdown_seconds": binance_countdown
            },
            
            # 欧意数据
            "okx": {
                "price": okx_price,
                "funding_rate": okx_rate,
                "period_seconds": okx_period,
                "current_settlement_time": okx_current,
                "next_settlement_time": okx_next,
                "countdown_seconds": okx_countdown
            },
            
            # 计算结果
            "calculations": {
                "price_diff": price_diff,
                "rate_diff": rate_diff,
                "rate_diff_abs": abs(rate_diff) if rate_diff is not None else None
            }
        }
        
        # 记录重要数据
        if rate_diff is not None and abs(rate_diff) > 0.0001:
            logger.info(f"Step5 生成成品: {symbol}, "
                       f"价格差: {price_diff:.2f}, "
                       f"费率差: {rate_diff:.6f}")
        
        return final_data
    
    async def _push_to_brain(self, final_data: Dict[str, Any]) -> None:
        """推送给大脑"""
        if not self.brain_callback:
            logger.warning("Step5: 大脑回调未设置，无法推送")
            return
        
        try:
            # 添加推送标记
            final_data["pushed_at"] = datetime.now().isoformat()
            
            # 调用大脑回调
            await self.brain_callback(final_data)
            
            logger.debug(f"Step5 推送成品: {final_data.get('symbol')}")
            
        except Exception as e:
            logger.error(f"Step5 推送失败: {e}")
            raise
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        cache_sizes = {
            "binance": len(self.exchange_cache["binance"]),
            "okx": len(self.exchange_cache["okx"])
        }
        
        return {
            **self.stats,
            "cache_sizes": cache_sizes,
            "timestamp": datetime.now().isoformat()
        }
    
    async def cleanup_old_cache(self, max_age_minutes: int = 10):
        """清理旧缓存"""
        now = datetime.now()
        
        # 清理交易所缓存
        for exchange in ["binance", "okx"]:
            expired_symbols = []
            
            for symbol, data in self.exchange_cache[exchange].items():
                timestamp_str = data.get("timestamp")
                if timestamp_str:
                    try:
                        data_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        age_minutes = (now - data_time).total_seconds() / 60
                        if age_minutes > max_age_minutes:
                            expired_symbols.append(symbol)
                    except:
                        expired_symbols.append(symbol)
            
            for symbol in expired_symbols:
                del self.exchange_cache[exchange][symbol]
        
        if expired_symbols:
            logger.debug(f"Step5 清理过期缓存: {len(expired_symbols)}个合约")
            