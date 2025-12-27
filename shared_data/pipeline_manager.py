"""
PipelineManager 调优版 - 批量处理 + 背压控制
功能：隔10条批量跑一次Step2-5，大幅降低CPU开销
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
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

class DataType(Enum):
    MARKET = "market"
    ACCOUNT = "account"

@dataclass
class PipelineConfig:
    """调优版配置"""
    queue_max_size: int = 1000           # ✅ 增大到1000（内存仍然安全）
    processing_timeout: float = 1.0
    batch_size: int = 10                 # ✅ 新增：每10条批量处理一次
    log_interval: int = 60

class PipelineManager:
    """调优版 - 批量处理 + 保留Step4缓存"""
    
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
        
        # 5个步骤
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()
        self.step5 = Step5CrossCalc()
        
        self.processing_lock = asyncio.Lock()
        
        self.counters = {
            'market_processed': 0,
            'account_processed': 0,
            'errors': 0,
            'batches_processed': 0,  # ✅ 新增：批量计数
            'start_time': time.time()
        }
        
        self.running = False
        self.queue = asyncio.Queue(maxsize=self.config.queue_max_size)
        
        # ✅ 新增：Step1临时缓存（批量处理用）
        self._step1_buffer: List[Any] = []
        
        logger.info(f"✅ 调优版PipelineManager初始化完成 (队列: {self.config.queue_max_size}, 批量: {self.config.batch_size})")
        self._initialized = True
    
    async def start(self):
        if self.running:
            return
        
        logger.info("🚀 调优版PipelineManager启动...")
        self.running = True
        
        asyncio.create_task(self._consumer_loop())
        logger.info("✅ 消费者循环已启动")
    
    async def stop(self):
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
        logger.info("🔄 消费者循环启动（批量处理模式）...")
        
        while self.running:
            try:
                queue_item = await asyncio.wait_for(
                    self.queue.get(), 
                    timeout=self.config.processing_timeout
                )
                await self._process_single_item(queue_item)
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                # ✅ 超时后检查是否需要刷新缓冲区
                if len(self._step1_buffer) > 0:
                    await self._flush_buffer()
                continue
            except Exception as e:
                logger.error(f"循环异常: {e}")
                self.counters['errors'] += 1
                await asyncio.sleep(0.1)
    
    async def _process_single_item(self, item: Dict[str, Any]):
        category = item["category"]
        raw_data = item["data"]
        
        async with self.processing_lock:
            try:
                if category == DataType.MARKET:
                    # ✅ 先缓存到Step1缓冲区
                    self._step1_buffer.append(raw_data)
                    
                    # ✅ 达到批量大小再处理
                    if len(self._step1_buffer) >= self.config.batch_size:
                        await self._flush_buffer()
                    
                elif category == DataType.ACCOUNT:
                    await self._process_account_data(raw_data)
                
            except Exception as e:
                logger.error(f"处理失败: {raw_data.get('symbol', 'N/A')} - {e}")
                self.counters['errors'] += 1
    
    async def _flush_buffer(self):
        """批量刷新缓冲区"""
        if not self._step1_buffer:
            return
        
        try:
            logger.debug(f"批量处理 {len(self._step1_buffer)} 条数据...")
            
            # Step1: 批量提取
            step1_results = self.step1.process(self._step1_buffer)
            self._step1_buffer.clear()  # ✅ 立即清空缓冲区
            
            if not step1_results:
                return
            
            # Step2-5: 继续批量处理
            step2_results = self.step2.process(step1_results)
            if not step2_results:
                return
            
            step3_results = self.step3.process(step2_results)
            if not step3_results:
                return
            
            step4_results = self.step4.process(step3_results)
            if not step4_results:
                return
            
            final_results = self.step5.process(step4_results)
            if not final_results:
                return
            
            # 推送大脑
            if self.brain_callback:
                for result in final_results:
                    await self.brain_callback(result.__dict__)
            
            self.counters['batches_processed'] += 1
            self.counters['market_processed'] += len(final_results)
            
        except Exception as e:
            logger.error(f"批量处理失败: {e}")
            self.counters['errors'] += 1
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据：直连大脑"""
        if self.brain_callback:
            await self.brain_callback(data)
        
        self.counters['account_processed'] += 1
        logger.debug(f"💰 账户数据直达: {data.get('exchange', 'N/A')}")
    
    def get_status(self) -> Dict[str, Any]:
        uptime = time.time() - self.counters['start_time']
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "batches_processed": self.counters['batches_processed'],  # ✅ 批量计数
            "errors": self.counters['errors'],
            "queue_size": self.queue.qsize(),
            "buffer_size": len(self._step1_buffer),  # ✅ 缓冲区当前大小
            "step4_cache_size": len(self.step4.binance_cache) if hasattr(self.step4, 'binance_cache') else 0
        }
