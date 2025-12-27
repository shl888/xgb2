"""
PipelineManager 降压版 - 内存优化型
功能：协调5步流水线，单条流式处理，零缓存，低内存
"""

import asyncio
from enum import Enum
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import logging
import time

# 5个步骤（仅导入需要的）
from shared_data.step1_filter import Step1Filter, ExtractedData
from shared_data.step2_fusion import Step2Fusion, FusedData
from shared_data.step3_align import Step3Align, AlignedData
from shared_data.step4_calc import Step4Calc, PlatformData
from shared_data.step5_cross_calc import Step5CrossCalc, CrossPlatformData

logger = logging.getLogger(__name__)

class DataType(Enum):
    """极简数据类型分类"""
    MARKET = "market"
    ACCOUNT = "account"

class PipelineManager:
    """降压版管理员 - 单条流式 + 零缓存"""
    
    # ✅ 新增：单例模式（内存开销<1KB）
    _instance: Optional['PipelineManager'] = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    # ✅ 新增：获取单例实例
    @classmethod
    def instance(cls) -> 'PipelineManager':
        """获取单例实例（如果未初始化则创建默认实例）"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self, brain_callback: Optional[Callable] = None):
        # **防止重复初始化**
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        # 核心组件（轻量级）
        self.brain_callback = brain_callback
        
        # 5个步骤实例（无状态）
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()
        self.step5 = Step5CrossCalc()
        
        # **单条处理锁（核心保留）**
        self.processing_lock = asyncio.Lock()
        
        # **简单计数器（无历史记录）**
        self.counters = {
            'market_processed': 0,
            'account_processed': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # **状态标志**
        self.running = False
        
        # **数据队列（限制大小防内存爆）**
        self.queue = asyncio.Queue(maxsize=500)
        
        logger.info("✅ 降压版PipelineManager初始化完成")
        self._initialized = True
    
    async def start(self):
        """启动消费者循环"""
        if self.running:
            return
        
        logger.info("🚀 降压版PipelineManager启动...")
        self.running = True
        
        asyncio.create_task(self._consumer_loop())
        logger.info("✅ 消费者循环已启动")
    
    async def stop(self):
        """立即关闭"""
        logger.info("🛑 PipelineManager停止中...")
        self.running = False
        
        await asyncio.sleep(1)
        
        # 清空队列（释放内存）
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except:
                break
        
        logger.info("✅ PipelineManager已停止")
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """
        数据入口（带背压控制）
        """
        try:
            # 快速分类
            data_type = data.get("data_type", "")
            if data_type.startswith(("ticker", "funding_rate", "mark_price",
                                   "okx_", "binance_")):
                category = DataType.MARKET
            elif data_type.startswith(("account", "position", "order", "trade")):
                category = DataType.ACCOUNT
            else:
                category = DataType.MARKET
            
            # 打包入队
            queue_item = {
                "category": category,
                "data": data,
                "timestamp": time.time()
            }
            
            self.queue.put_nowait(queue_item)
            return True
            
        except asyncio.QueueFull:
            logger.warning(f"⚠️ 队列已满（>{self.queue.maxsize}），数据被拒绝")
            return False
        except Exception as e:
            logger.error(f"入队失败: {e}")
            return False
    
    async def _consumer_loop(self):
        """单条流式处理循环"""
        logger.info("🔄 消费者循环启动（单条流式）...")
        
        while self.running:
            try:
                queue_item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._process_single_item(queue_item)
                self.queue.task_done()
                
            except asyncio.TimeoutError:
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
        
        # Step4: 计算
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
        """获取当前状态"""
        uptime = time.time() - self.counters['start_time']
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "errors": self.counters['errors'],
            "queue_size": self.queue.qsize()
        }
