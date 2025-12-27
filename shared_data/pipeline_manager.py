"""
PipelineManager 降压修正版 - 保留Step4缓存
功能：单条流式处理 + 仅保留Step4必需缓存
"""

import asyncio
from enum import Enum
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import logging
import time
from dataclasses import dataclass

# 5个步骤
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc  # 它自带缓存
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

class DataType(Enum):
    """极简数据类型分类"""
    MARKET = "market"
    ACCOUNT = "account"

@dataclass
class PipelineConfig:
    """流水线配置（降压版）"""
    queue_max_size: int = 500
    processing_timeout: float = 1.0
    log_interval: int = 60

class PipelineManager:
    """降压修正版 - 仅保留Step4必需缓存"""
    
    _instance: Optional['PipelineManager'] = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def instance(cls) -> 'PipelineManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self, brain_callback: Optional[Callable] = None, 
                 config: Optional[PipelineConfig] = None):
        
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.config = config or PipelineConfig()
        self.brain_callback = brain_callback
        
        # 5个步骤实例
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()  # ✅ 保留内部缓存
        self.step5 = Step5CrossCalc()
        
        self.processing_lock = asyncio.Lock()
        
        self.counters = {
            'market_processed': 0,
            'account_processed': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        self.running = False
        self.queue = asyncio.Queue(maxsize=self.config.queue_max_size)
        
        # ✅ 新增：Step4缓存监控（仅监控，不额外存储）
        self.step4_cache_size = 0
        
        logger.info(f"✅ 降压修正版PipelineManager初始化完成")
        self._initialized = True
    
    def _update_step4_cache_monitor(self):
        """更新Step4缓存大小监控"""
        try:
            self.step4_cache_size = len(self.step4.binance_cache)
            if self.step4_cache_size > 1000:  # 警告阈值
                logger.warning(f"⚠️ Step4缓存异常({self.step4_cache_size}个合约)")
        except:
            pass  # 即使监控失败也不影响主流程
    
    async def start(self):
        """启动消费者循环"""
        if self.running:
            return
        
        logger.info("🚀 降压修正版PipelineManager启动...")
        self.running = True
        
        asyncio.create_task(self._consumer_loop())
        asyncio.create_task(self._cache_monitor_loop())  # ✅ 启动缓存监控
        
        logger.info("✅ 消费者循环已启动")
    
    async def stop(self):
        """立即关闭"""
        logger.info("🛑 PipelineManager停止中...")
        self.running = False
        
        await asyncio.sleep(1)
        
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except:
                break
        
        logger.info("✅ PipelineManager已停止")
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """数据入口（带背压控制）"""
        try:
            data_type = data.get("data_type", "")
            if data_type.startswith(("ticker", "funding_rate", "mark_price",
                                   "okx_", "binance_")):
                category = DataType.MARKET
            elif data_type.startswith(("account", "position", "order", "trade")):
                category = DataType.ACCOUNT
            else:
                category = DataType.MARKET
            
            queue_item = {
                "category": category,
                "data": data,
                "timestamp": time.time()
            }
            
            self.queue.put_nowait(queue_item)
            return True
            
        except asyncio.QueueFull:
            logger.warning(f"⚠️ 队列已满（>{self.config.queue_max_size}），数据被拒绝")
            return False
        except Exception as e:
            logger.error(f"入队失败: {e}")
            return False
    
    async def _consumer_loop(self):
        """单条流式处理循环"""
        logger.info("🔄 消费者循环启动（单条流式）...")
        
        while self.running:
            try:
                queue_item = await asyncio.wait_for(
                    self.queue.get(), 
                    timeout=self.config.processing_timeout
                )
                await self._process_single_item(queue_item)
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"循环异常: {e}")
                self.counters['errors'] += 1
                await asyncio.sleep(0.1)
    
    async def _cache_monitor_loop(self):
        """Step4缓存监控循环（每30秒检查）"""
        while self.running:
            try:
                await asyncio.sleep(30)
                self._update_step4_cache_monitor()
                
                # 打印缓存状态（调试用）
                logger.debug(f"Step4缓存: {self.step4_cache_size} 个合约")
                
            except Exception as e:
                logger.error(f"缓存监控异常: {e}")
    
    async def _process_single_item(self, item: Dict[str, Any]):
        """单条数据处理"""
        category = item["category"]
        raw_data = item["data"]
        
        async with self.processing_lock:
            try:
                if category == DataType.MARKET:
                    await self._process_market_data(raw_data)
                elif category == DataType.ACCOUNT:
                    await self._process_account_data(raw_data)
                
            except Exception as e:
                logger.error(f"处理失败: {raw_data.get('symbol', 'N/A')} - {e}")
                self.counters['errors'] += 1
    
    async def _process_market_data(self, data: Dict[str, Any]):
        """市场数据处理：完整5步流水线"""
        # Step1: 提取
        step1_results = self.step1.process([data])
        if not step1_results:
            return
        
        # Step2: 融合
        step2_results = self.step2.process(step1_results)
        if not step2_results:
            return
        
        # Step3: 对齐
        step3_results = self.step3.process(step2_results)
        if not step3_results:
            return
        
        # Step4: 计算（内部缓存自动工作）
        step4_results = self.step4.process(step3_results)
        if not step4_results:
            return
        
        # Step5: 跨平台计算
        final_results = self.step5.process(step4_results)
        if not final_results:
            return
        
        # 推送大脑
        if self.brain_callback:
            for result in final_results:
                await self.brain_callback(result.__dict__)
        
        self.counters['market_processed'] += 1
        logger.debug(f"📊 处理完成: {data.get('symbol', 'N/A')}")
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据：直连大脑"""
        if self.brain_callback:
            await self.brain_callback(data)
        
        self.counters['account_processed'] += 1
        logger.debug(f"💰 账户数据直达: {data.get('exchange', 'N/A')}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取当前状态（包含Step4缓存监控）"""
        uptime = time.time() - self.counters['start_time']
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "errors": self.counters['errors'],
            "queue_size": self.queue.qsize(),
            "step4_cache_size": self.step4_cache_size  # ✅ 增加缓存监控
        }
