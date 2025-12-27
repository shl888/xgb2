"""
PipelineManager 智能版 - 完整实现
包含：批量处理 + 内存监控 + Step4缓存监控
"""

import asyncio
from enum import Enum
from typing import Dict, Any, Optional, Callable
import logging
import time
from dataclasses import dataclass

# 5个步骤 + 内存监控
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

try:
    import psutil  # 可选依赖，用于内存监控
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)

class DataType(Enum):
    MARKET = "market"
    ACCOUNT = "account"

@dataclass
class PipelineConfig:
    """智能版配置"""
    queue_max_size: int = 5000           # 上限5000（约300MB）
    processing_timeout: float = 1.0
    batch_size: int = 10
    log_interval: int = 60
    memory_safe_threshold: float = 70.0  # 内存安全阈值70%

class PipelineManager:
    """智能版 - 完整实现"""
    
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
            'batches_processed': 0,
            'dropped_due_to_memory': 0,
            'start_time': time.time()
        }
        
        self.running = False
        self.queue = asyncio.Queue(maxsize=self.config.queue_max_size)
        self._step1_buffer: List[Any] = []
        
        logger.info(f"✅ 智能版PipelineManager初始化完成 (队列上限: {self.config.queue_max_size})")
        self._initialized = True
    
    def _get_memory_usage_percent(self) -> float:
        """获取当前内存使用率"""
        if psutil is None:
            return 0.0
        try:
            return psutil.virtual_memory().percent
        except:
            return 0.0
    
    def _is_memory_safe(self) -> bool:
        """检查内存是否安全"""
        usage = self._get_memory_usage_percent()
        return usage < self.config.memory_safe_threshold
    
    async def start(self):
        """启动所有后台任务"""
        if self.running:
            return
        
        logger.info("🚀 智能版PipelineManager启动...")
        self.running = True
        
        # ✅ 启动3个后台任务
        asyncio.create_task(self._consumer_loop())
        asyncio.create_task(self._memory_monitor_loop())
        asyncio.create_task(self._periodic_log_loop())
        
        logger.info("✅ 消费者循环 + 监控任务已启动")
    
    async def stop(self):
        """优雅停止"""
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
        """智能入队（带内存感知）"""
        try:
            # 分类
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
            
            # 尝试直接入队
            try:
                self.queue.put_nowait(queue_item)
                return True
            except asyncio.QueueFull:
                pass
            
            # 内存检查
            if not self._is_memory_safe():
                logger.warning(f"⚠️ 内存危险({self._get_memory_usage_percent():.1f}%)，拒绝数据")
                self.counters['dropped_due_to_memory'] += 1
                return False
            
            # 丢弃老数据后入队
            try:
                self.queue.get_nowait()  # 丢弃最老一条
                self.queue.put_nowait(queue_item)
                logger.debug(f"🔄 队列满，丢弃老数据后入队: {data.get('symbol', 'N/A')}")
                return True
            except:
                return False
            
        except Exception as e:
            logger.error(f"入队失败: {e}")
            return False
    
    async def _consumer_loop(self):
        """核心消费者循环"""
        logger.info("🔄 消费者循环启动（批量处理 + 内存感知）...")
        
        while self.running:
            try:
                queue_item = await asyncio.wait_for(
                    self.queue.get(), 
                    timeout=self.config.processing_timeout
                )
                await self._process_single_item(queue_item)
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                if len(self._step1_buffer) > 0:
                    await self._flush_buffer()
                continue
            except Exception as e:
                logger.error(f"循环异常: {e}")
                self.counters['errors'] += 1
                await asyncio.sleep(0.1)
    
    async def _process_single_item(self, item: Dict[str, Any]):
        """单条数据处理"""
        category = item["category"]
        raw_data = item["data"]
        
        async with self.processing_lock:
            try:
                if category == DataType.MARKET:
                    self._step1_buffer.append(raw_data)
                    
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
            
            step1_results = self.step1.process(self._step1_buffer)
            self._step1_buffer.clear()
            
            if not step1_results:
                return
            
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
            
            if self.brain_callback:
                for result in final_results:
                    await self.brain_callback(result.__dict__)
            
            self.counters['batches_processed'] += 1
            self.counters['market_processed'] += len(final_results)
            
        except Exception as e:
            logger.error(f"批量处理失败: {e}")
            self.counters['errors'] += 1
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据直连大脑"""
        if self.brain_callback:
            await self.brain_callback(data)
        
        self.counters['account_processed'] += 1
        logger.debug(f"💰 账户数据直达: {data.get('exchange', 'N/A')}")
    
    async def _memory_monitor_loop(self):
        """内存监控循环（每10秒）"""
        while self.running:
            try:
                await asyncio.sleep(10)
                
                mem_usage = self._get_memory_usage_percent()
                queue_size = self.queue.qsize()
                
                if mem_usage > self.config.memory_safe_threshold:
                    logger.warning(f"⚠️ 内存压力高: {mem_usage:.1f}% | 队列: {queue_size}")
                
                if queue_size > self.config.queue_max_size * 0.8:
                    logger.warning(f"⚠️ 队列堆积: {queue_size}/{self.config.queue_max_size}")
                
            except Exception as e:
                logger.error(f"内存监控异常: {e}")
    
    async def _periodic_log_loop(self):
        """定期日志循环"""
        while self.running:
            try:
                await asyncio.sleep(self.config.log_interval)
                
                status = self.get_status()
                logger.info("="*60)
                logger.info(f"📊 流水线运行报告（运行: {int(status['uptime_seconds'])}秒）")
                logger.info(f"处理量: 市场={status['market_processed']} | 账户={status['account_processed']}")
                logger.info(f"批量数: {status['batches_processed']} | 错误: {status['errors']}")
                logger.info(f"队列: {status['queue_size']}/{self.config.queue_max_size} | 内存: {status['memory_usage_percent']:.1f}%")
                logger.info("="*60)
                
            except Exception as e:
                logger.error(f"日志循环异常: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取完整状态"""
        uptime = time.time() - self.counters['start_time']
        mem_usage = self._get_memory_usage_percent()
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "batches_processed": self.counters['batches_processed'],
            "errors": self.counters['errors'],
            "queue_size": self.queue.qsize(),
            "buffer_size": len(self._step1_buffer),
            "memory_usage_percent": mem_usage,
            "dropped_due_to_memory": self.counters['dropped_due_to_memory'],
            "memory_safe": self._is_memory_safe()
        }
