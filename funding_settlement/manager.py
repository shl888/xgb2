"""
资金费率结算管理器
核心功能：从币安获取最近结算周期的资金费率
"""
import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import aiohttp

# 设置导入路径
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store

logger = logging.getLogger(__name__)


class FundingSettlementManager:
    """
    资金费率结算管理器
    1. 启动时自动获取一次（带重试）
    2. 支持手动触发获取
    3. 限制手动触发频率（1小时最多3次）
    """
    
    # 币安API配置
    BINANCE_FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
    API_WEIGHT_PER_REQUEST = 10  # 批量获取固定权重
    
    def __init__(self):
        self.last_fetch_time: Optional[float] = None
        self.manual_fetch_count: int = 0
        self.last_manual_fetch_hour: Optional[int] = None
        self.is_auto_fetched: bool = False
        
        # 初始化data_store存储结构
        if not hasattr(data_store, 'funding_settlement'):
            data_store.funding_settlement = {}
        if 'binance' not in data_store.funding_settlement:
            data_store.funding_settlement['binance'] = {}
    
    async def fetch_funding_settlement(self, max_retries: int = 3) -> Dict[str, Any]:
        """
        获取币安最近结算周期的资金费率
        :param max_retries: 最大重试次数
        :return: 结果字典
        """
        logger.info("=" * 60)
        logger.info("🎯 开始获取币安资金费率结算数据...")
        logger.info(f"API端点: {self.BINANCE_FUNDING_RATE_URL}")
        logger.info(f"API权重消耗（固定）: {self.API_WEIGHT_PER_REQUEST}")
        logger.info("=" * 60)
        
        for attempt in range(max_retries):
            try:
                # 构建请求参数：不传symbol，不传时间，limit=1（每个symbol只返回最新结算）
                params = {
                    "limit": 1  # 只获取最近1次结算数据
                }
                
                logger.info(f"📡 API请求: limit=1（最近结算周期）")
                logger.info(f"尝试次数: {attempt + 1}/{max_retries}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.BINANCE_FUNDING_RATE_URL,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        
                        # 检查HTTP状态
                        if response.status != 200:
                            error_text = await response.text()
                            raise Exception(f"HTTP {response.status}: {error_text}")
                        
                        # 解析响应
                        data = await response.json()
                        
                        # 计算权重消耗（币安批量获取固定权重=10）
                        weight_used = self.API_WEIGHT_PER_REQUEST
                        
                        logger.info(f"✅ API响应成功！状态码: {response.status}")
                        logger.info(f"📊 返回合约数量: {len(data)} 个")
                        logger.info(f"⚖️  权重消耗: {weight_used}（固定值）")
                        
                        # 过滤USDT永续合约
                        filtered_data = self._filter_usdt_perpetual(data)
                        
                        logger.info(f"🔍 过滤后USDT永续合约: {len(filtered_data)} 个")
                        logger.info(f"USDT合约: {list(filtered_data.keys())[:10]}{'...' if len(filtered_data) > 10 else ''}")
                        
                        # 推送到共享数据模块
                        await self._push_to_data_store(filtered_data)
                        
                        # 更新状态
                        self.last_fetch_time = time.time()
                        self.is_auto_fetched = True
                        
                        result = {
                            "success": True,
                            "contract_count": len(data),
                            "filtered_count": len(filtered_data),
                            "weight_used": weight_used,
                            "timestamp": datetime.now().isoformat(),
                            "contracts": list(filtered_data.keys())
                        }
                        
                        logger.info("=" * 60)
                        logger.info(f"🎉 资金费率结算数据获取成功！")
                        logger.info(f"   总合约: {len(data)}, USDT永续: {len(filtered_data)}")
                        logger.info(f"   权重消耗: {weight_used}")
                        logger.info("=" * 60)
                        
                        return result
                
            except Exception as e:
                logger.error(f"❌ 第 {attempt + 1} 次尝试失败: {e}")
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("=" * 60)
                    logger.error("💥 所有重试次数已用完，获取失败！")
                    logger.error("=" * 60)
        
        return {
            "success": False,
            "error": f"获取失败，已重试 {max_retries} 次",
            "timestamp": datetime.now().isoformat()
        }
    
    def _filter_usdt_perpetual(self, api_response: List[Dict]) -> Dict[str, Dict]:
        """
        过滤USDT永续合约
        :param api_response: 币安API原始响应
        :return: 过滤后的字典 {symbol: data}
        """
        filtered = {}
        
        for item in api_response:
            symbol = item.get('symbol', '')
            
            if (symbol.endswith('USDT') and 
                not symbol.startswith('1000') and 
                ':' not in symbol):
                
                processed = {
                    "symbol": symbol,
                    "funding_rate": float(item.get('fundingRate', 0)),
                    "funding_time": item.get('fundingTime'),
                    "next_funding_time": item.get('nextFundingTime'),
                    "raw_data": item
                }
                
                filtered[symbol] = processed
        
        return dict(sorted(filtered.items()))
    
    async def _push_to_data_store(self, filtered_data: Dict[str, Dict]):
        """
        推送到共享数据模块
        """
        try:
            data_store.funding_settlement['bin'].clear()
            for symbol, data in filtered_data.items():
                data_store.funding_settlement['binance'][symbol] = data
            logger.info(f"📤 成功推送到data_store.funding_settlement['binance']")
        except Exception as e:
            logger.error(f"推送到data_store失败: {e}")
            raise
    
    def can_manually_fetch(self) -> tuple[bool, Optional[str]]:
        """
        检查是否可以手动触发获取
        """
        current_hour = datetime.now().hour
        
        if self.last_manual_fetch_hour != current_hour:
            self.manual_fetch_count = 0
            self.last_manual_fetch_hour = current_hour
        
        if self.manual_fetch_count >= 3:
            return False, f"1小时内最多获取3次（已使用: {self.manual_fetch_count}/3）"
        
        return True, None
    
    async def manual_fetch(self) -> Dict[str, Any]:
        """
        手动触发获取
        """
        can_fetch, reason = self.can_manually_fetch()
        
        if not can_fetch:
            logger.warning(f"⏸️  手动获取被拒绝: {reason}")
            return {
                "success": False,
                "error": reason,
                "timestamp": datetime.now().isoformat()
            }
        
        logger.info("=" * 60)
        logger.info("🖱️  收到手动触发请求...")
        logger.info("=" * 60)
        
        self.manual_fetch_count += 1
        result = await self.fetch_funding_settlement()
        result['triggered_by'] = 'manual'
        result['manual_fetch_count'] = f"{self.manual_fetch_count}/3"
        
        return result
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取模块状态
        """
        current_hour = datetime.now().hour
        
        if self.last_manual_fetch_hour != current_hour:
            manual_count_str = "0/3"
        else:
            manual_count_str = f"{self.manual_fetch_count}/3"
        
        return {
            "last_fetch_time": datetime.fromtimestamp(self.last_fetch_time).isoformat() if self.last_fetch_time else None,
            "is_auto_fetched": self.is_auto_fetched,
            "manual_fetch_count": manual_count_str,
            "usdt_contracts_count": len(data_store.funding_settlement.get('binance', {})),
            "api_weight_per_request": self.API_WEIGHT_PER_REQUEST
        }
