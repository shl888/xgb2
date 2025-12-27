"""
PipelineManager 顶配版 - 数据总指挥官
功能：协调5步流水线 + 账户数据直连 + 智能监控 + 自动容错
设计原则：生产级健壮性、可观测性、高吞吐、零阻塞
"""

import asyncio
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import time
from collections import deque
from enum import Enum

# 导入5个步骤
from step1_filter import Step1Filter, ExtractedData
from step2_fusion import Step2Fusion, FusedData
from step3_align import Step3Align, AlignedData
from step4_calc import Step4Calc, PlatformData
from step5_cross_calc import Step5CrossCalc, CrossPlatformData

logger = logging.getLogger(__name__)

class DataType(Enum):
    """数据类型枚举（严格分类）"""
    # 市场数据（走完整流水线）
    MARKET_TICKER = "ticker"
    MARKET_FUNDING_RATE = "funding_rate"
    MARKET_MARK_PRICE = "mark_price"
    MARKET_OKX_TICKER = "okx_ticker"
    MARKET_OKX_FUNDING = "okx_funding_rate"
    MARKET_BINANCE_TICKER = "binance_ticker"
    MARKET_BINANCE_MARK = "binance_mark_price"
    MARKET_BINANCE_SETTLEMENT = "binance_funding_settlement"
    
    # 账户数据（直连大脑）
    ACCOUNT_BALANCE = "account"
    ACCOUNT_POSITION = "position"
    ACCOUNT_ORDER = "order"
    ACCOUNT_TRADE = "trade"
    
    # 系统数据（特殊处理）
    SYSTEM_STATUS = "connection_status"

@dataclass
class PipelineConfig:
    """流水线配置（可热更新）"""
    # 批处理阈值
    step1_batch_size: int = 15      # Step1缓存15条处理一次
    step2_batch_size: int = 30      # Step2缓存30条处理一次
    step3_batch_size: int = 30      # Step3缓存30条处理一次
    step4_batch_size: int = 50      # Step4缓存50条处理一次
    
    # 性能调优
    max_queue_size: int = 10000     # 队列上限（防内存爆）
    cleanup_interval: int = 300     # 清理间隔（秒）
    metrics_report_interval: int = 60  # 指标报告间隔（秒）
    
    # 容错配置
    max_retry_attempts: int = 3     # 单条数据最大重试
    dead_letter_enabled: bool = True  # 是否启用死信队列

@dataclass
class PipelineMetrics:
    """运行指标（实时更新）"""
    # 处理量
    step1_processed: int = 0
    step2_processed: int = 0
    step3_processed: int = 0
    step4_processed: int = 0
    step5_processed: int = 0
    direct_processed: int = 0  # 直连大脑的数据量
    
    # 错误统计
    step1_errors: int = 0
    step2_errors: int = 0
    step3_errors: int = 0
    step4_errors: int = 0
    step5_errors: int = 0
    
    # 性能指标
    avg_latency_ms: Dict[str, float] = field(default_factory=dict)  # 各步骤平均延迟
    queue_size_history: deque = field(default_factory=lambda: deque(maxlen=100))  # 队列历史
    last_update: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "processed": {
                "step1": self.step1_processed,
                "step2": self.step2_processed,
                "step3": self.step3_processed,
                "step4": self.step4_processed,
                "step5": self.step5_processed,
                "direct": self.direct_processed
            },
            "errors": {
                "step1": self.step1_errors,
                "step2": self.step2_errors,
                "step3": self.step3_errors,
                "step4": self.step4_errors,
                "step5": self.step5_errors,
            },
            "performance": self.avg_latency_ms,
            "total_processed": sum([self.step1_processed, self.direct_processed]),
            "error_rate": sum([self.step1_errors]) / max(1, self.step1_processed),
            "last_update": self.last_update.isoformat() if self.last_update else None
        }

class PipelineManager:
    """顶配版流水线管理员"""
    
    # 类级别单例（可选）
    _instance: Optional['PipelineManager'] = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, brain_callback: Optional[Callable[[Dict], asyncio.Future]] = None, 
                 config: Optional[PipelineConfig] = None):
        
        # 防止重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        # 核心组件
        self.config = config or PipelineConfig()
        self.metrics = PipelineMetrics()
        self.brain_callback = brain_callback
        
        # 5个步骤实例（带独立锁）
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()
        self.step5 = Step5CrossCalc()
        
        # 步骤锁（防止并发处理同类型数据）
        self.step_locks = {
            'step1': asyncio.Lock(),
            'step2': asyncio.Lock(),
            'step3': asyncio.Lock(),
            'step4': asyncio.Lock(),
            'step5': asyncio.Lock(),
        }
        
        # 数据队列（使用优先级队列）
        self.data_queue = asyncio.PriorityQueue(maxsize=self.config.max_queue_size)
        
        # 缓存（LRU风格，带过期时间）
        self._step1_cache: List[ExtractedData] = []
        self._step2_cache: List[FusedData] = []
        self._step3_cache: List[AlignedData] = []
        self._step4_cache: List[PlatformData] = []
        
        # 死信队列（记录处理失败的数据）
        self.dead_letter_queue: List[Dict[str, Any]] = []
        
        # 延迟记录（用于计算avg_latency）
        self._latencies = defaultdict(list)
        
        # 运行状态
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # 初始化时间
        self._init_time = datetime.now()
        
        logger.info(f"🎛️ 流水线管理员初始化完成 (批处理配置: {self.config})")
        self._initialized = True
    
    async def start(self):
        """启动管理员（启动所有后台任务）"""
        if self._running:
            logger.warning("⚠️ 管理员已在运行中")
            return
        
        logger.info("🚀 流水线管理员启动...")
        self._running = True
        
        # 启动核心处理循环
        self._tasks = [
            asyncio.create_task(self._data_consumer_loop(), name="consumer"),
            asyncio.create_task(self._metrics_reporter(), name="metrics"),
            asyncio.create_task(self._cleanup_task(), name="cleanup"),
            asyncio.create_task(self._health_check(), name="health")
        ]
        
        logger.info("✅ 所有后台任务启动完成")
    
    async def stop(self, timeout: int = 30):
        """优雅停止（等待所有数据处理完成）"""
        logger.info(f"🛑 管理员停止中（超时: {timeout}s）...")
        self._running = False
        
        # 1. 停止接收新数据
        await self.data_queue.put((999, {"type": "poison"}))  # 毒丸
        
        # 2. 等待所有任务完成
        done, pending = await asyncio.wait(self._tasks, timeout=timeout)
        
        if pending:
            logger.warning(f"⚠️  {len(pending)} 个任务超时，强制取消")
            for task in pending:
                task.cancel()
        
        # 3. 清理缓存
        self._flush_all_cache()
        
        logger.info("✅ 管理员已停止")
    
    async def ingest_data(self, data: Dict[str, Any], priority: int = 5) -> bool:
        """
        接收原始数据入口
        priority: 优先级(0-10, 0最高)
        """
        try:
            # 1. 数据分类
            data_type = data.get("data_type", "")
            classified_type = self._classify_data_type(data_type)
            
            if classified_type is None:
                logger.warning(f"⚠️ 无法分类的数据类型: {data_type}，直接传递")
                if self.brain_callback:
                    await self.brain_callback(data)
                return True
            
            # 2. 根据类型决定路径
            if classified_type == "market":
                # 市场数据 → 入队等待流水线处理
                await self.data_queue.put((priority, {
                    "type": "market",
                    "data": data,
                    "retry_count": 0,
                    "first_seen": time.time()
                }))
                logger.debug(f"📥 市场数据入队: {data_type} {data.get('symbol')}")
                
            elif classified_type == "account":
                # 账户数据 → 直连大脑
                if self.brain_callback:
                    await self.brain_callback(data)
                self.metrics.direct_processed += 1
                logger.debug(f"📤 账户数据直连大脑: {data_type}")
            
            elif classified_type == "system":
                # 系统数据 → 特殊处理
                await self._handle_system_data(data)
            
            self.metrics.last_update = datetime.now()
            return True
            
        except Exception as e:
            logger.error(f"数据入队失败: {e}")
            return False
    
    def _classify_data_type(self, data_type: str) -> Optional[str]:
        """严格分类数据类型"""
        market_prefixes = ["ticker", "funding_rate", "mark_price", 
                          "okx_", "binance_"]
        account_prefixes = ["account", "position", "order", "trade"]
        
        # 市场数据
        if any(data_type.startswith(p) for p in market_prefixes):
            return "market"
        
        # 账户数据
        if any(data_type.startswith(p) for p in account_prefixes):
            return "account"
        
        # 系统数据
        if data_type in ["connection_status", "system"]:
            return "system"
        
        return None
    
    async def _data_consumer_loop(self):
        """核心消费者循环（顺序执行流水线）"""
        logger.info("🔄 消费者循环启动...")
        
        while self._running:
            try:
                # 取数据（带超时）
                priority, item = await asyncio.wait_for(
                    self.data_queue.get(), timeout=1.0
                )
                
                # 毒丸检查（停止信号）
                if item.get("type") == "poison":
                    break
                
                # 记录等待时间
                wait_time = time.time() - item["first_seen"]
                self._latencies['queue_wait'].append(wait_time * 1000)
                
                # 处理单条数据
                await self._process_single_item(item)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"消费者循环异常: {e}")
                await asyncio.sleep(0.1)
        
        logger.info("🔄 消费者循环停止")
    
    async def _process_single_item(self, item: Dict[str, Any]):
        """处理单条数据（完整流水线）"""
        raw_data = item["data"]
        retry_count = item["retry_count"]
        
        start_time = time.time()
        
        try:
            # Step1: 过滤提取
            async with self.step_locks['step1']:
                step1_results = self.step1.process([raw_data])
                self.metrics.step1_processed += len(step1_results)
            
            if not step1_results:
                return
            
            # 缓存并检查批量条件
            self._step1_cache.extend(step1_results)
            
            # Step2: 融合（批量）
            if len(self._step1_cache) >= self.config.step1_batch_size:
                await self._run_step2()
            
            # Step3: 对齐（批量）
            if len(self._step2_cache) >= self.config.step2_batch_size:
                await self._run_step3()
            
            # Step4: 计算（批量）
            if len(self._step3_cache) >= self.config.step3_batch_size:
                await self._run_step4()
            
            # Step5: 跨平台计算（批量）
            if len(self._step4_cache) >= self.config.step4_batch_size:
                await self._run_step5()
            
            # 记录成功延迟
            self._record_latency('total', start_time)
            
        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            self._record_error(raw_data, e, retry_count)
    
    async def _run_step2(self):
        """执行Step2（带锁和错误处理）"""
        async with self.step_locks['step2']:
            try:
                step2_results = self.step2.process(self._step1_cache)
                self.metrics.step2_processed += len(step2_results)
                self._step2_cache.extend(step2_results)
                self._step1_cache.clear()
                logger.debug(f"Step2批量完成: {len(step2_results)} 条")
            except Exception as e:
                self.metrics.step2_errors += 1
                logger.error(f"Step2批量失败: {e}")
                raise
    
    async def _run_step3(self):
        """执行Step3"""
        async with self.step_locks['step3']:
            try:
                step3_results = self.step3.process(self._step2_cache)
                self.metrics.step3_processed += len(step3_results)
                self._step3_cache.extend(step3_results)
                self._step2_cache.clear()
                logger.debug(f"Step3批量完成: {len(step3_results)} 条")
            except Exception as e:
                self.metrics.step3_errors += 1
                logger.error(f"Step3批量失败: {e}")
                raise
    
    async def _run_step4(self):
        """执行Step4"""
        async with self.step_locks['step4']:
            try:
                step4_results = self.step4.process(self._step3_cache)
                self.metrics.step4_processed += len(step4_results)
                self._step4_cache.extend(step4_results)
                self._step3_cache.clear()
                logger.debug(f"Step4批量完成: {len(step4_results)} 条")
            except Exception as e:
                self.metrics.step4_errors += 1
                logger.error(f"Step4批量失败: {e}")
                raise
    
    async def _run_step5(self):
        """执行Step5（推送给大脑）"""
        async with self.step_locks['step5']:
            try:
                final_results = self.step5.process(self._step4_cache)
                self.metrics.step5_processed += len(final_results)
                
                # **推送给大脑**
                if self.brain_callback and final_results:
                    for result in final_results:
                        await self.brain_callback(result.__dict__)
                
                self._step4_cache.clear()
                logger.debug(f"Step5批量完成: {len(final_results)} 条 → 大脑")
            except Exception as e:
                self.metrics.step5_errors += 1
                logger.error(f"Step5批量失败: {e}")
                raise
    
    def _record_latency(self, step: str, start_time: float):
        """记录延迟"""
        latency_ms = (time.time() - start_time) * 1000
        self._latencies[step].append(latency_ms)
        
        # 计算移动平均
        if len(self._latencies[step]) > 100:
            self._latencies[step].pop(0)
        
        self.metrics.avg_latency_ms[step] = sum(self._latencies[step]) / len(self._latencies[step])
    
    def _record_error(self, data: Dict, error: Exception, retry_count: int):
        """记录错误（支持重试）"""
        if retry_count < self.config.max_retry_attempts:
            # 重新入队（带延迟）
            asyncio.create_task(self._retry_with_delay(data, retry_count + 1))
        else:
            # 进入死信队列
            if self.config.dead_letter_enabled:
                self.dead_letter_queue.append({
                    "data": data,
                    "error": str(error),
                    "timestamp": datetime.now().isoformat()
                })
                logger.error(f"数据进入死信队列: {data.get('symbol')} - {error}")
    
    async def _retry_with_delay(self, data: Dict, retry_count: int):
        """延迟重试（指数退避）"""
        delay = 2 ** retry_count  # 2, 4, 8秒
        logger.warning(f"⏳ {data.get('symbol')} 将在 {delay}s 后重试（第 {retry_count} 次）")
        await asyncio.sleep(delay)
        await self.ingest_data(data, priority=1)  # 重试数据优先级设为1
    
    async def _handle_system_data(self, data: Dict[str, Any]):
        """处理系统数据（如连接状态）"""
        # 可以在这里记录系统状态到监控面板
        logger.info(f"📡 系统数据: {data.get('data_type')} = {data.get('status')}")
    
    async def _metrics_reporter(self):
        """指标报告任务（定时打印）"""
        while self._running:
            await asyncio.sleep(self.config.metrics_report_interval)
            
            metrics = self.metrics.to_dict()
            logger.info("=" * 60)
            logger.info(f"📊 流水线运行报告（运行: {datetime.now() - self._init_time})")
            logger.info(f"处理量: {metrics['processed']} | 错误数: {sum(metrics['errors'].values())}")
            logger.info(f"延迟: {self.metrics.avg_latency_ms.get('total', 0):.2f}ms")
            logger.info(f"死信队列: {len(self.dead_letter_queue)}")
            logger.info("=" * 60)
    
    async def _cleanup_task(self):
        """清理任务（回收内存、检查泄漏）"""
        while self._running:
            await asyncio.sleep(self.config.cleanup_interval)
            
            # 1. 检查缓存泄漏
            total_cached = sum([
                len(self._step1_cache),
                len(self._step2_cache),
                len(self._step3_cache),
                len(self._step4_cache)
            ])
            
            if total_cached > 10000:
                logger.warning(f"⚠️  缓存总量过大({total_cached})，可能泄漏")
                self._flush_all_cache()
            
            # 2. 压缩延迟记录
            for key in self._latencies:
                if len(self._latencies[key]) > 1000:
                    self._latencies[key] = self._latencies[key][-100:]
            
            logger.debug(f"🧹 清理完成 | 缓存: {total_cached} | 死信: {len(self.dead_letter_queue)}")
    
    async def _health_check(self):
        """健康检查任务"""
        while self._running:
            await asyncio.sleep(30)
            
            # 检查队列堆积
            queue_size = self.data_queue.qsize()
            self.metrics.queue_size_history.append(queue_size)
            
            if queue_size > self.config.max_queue_size * 0.8:
                logger.warning(f"⚠️  队列堆积严重: {queue_size}/{self.config.max_queue_size}")
            
            # 检查错误率
            error_rate = self.metrics.step1_errors / max(1, self.metrics.step1_processed)
            if error_rate > 0.1:  # 错误率超过10%
                logger.error(f"🔥 错误率过高: {error_rate*100:.1f}%")
    
    def _flush_all_cache(self):
        """强制清空所有缓存"""
        logger.warning("🚨 强制清空缓存！")
        self._step1_cache.clear()
        self._step2_cache.clear()
        self._step3_cache.clear()
        self._step4_cache.clear()
    
    def get_status(self) -> Dict[str, Any]:
        """获取完整状态（用于监控面板）"""
        return {
            "running": self._running,
            "uptime_seconds": (datetime.now() - self._init_time).total_seconds(),
            "metrics": self.metrics.to_dict(),
            "pipeline_status": {
                "step1_cache": len(self._step1_cache),
                "step2_cache": len(self._step2_cache),
                "step3_cache": len(self._step3_cache),
                "step4_cache": len(self._step4_cache),
                "queue_size": self.data_queue.qsize(),
                "dead_letter": len(self.dead_letter_queue)
            },
            "config": self.config.__dict__,
            "health": {
                "queue_full_ratio": self.data_queue.qsize() / self.config.max_queue_size,
                "error_rate": self.metrics.step1_errors / max(1, self.metrics.step1_processed)
            }
        }
    
    def get_dead_letters(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取死信队列（用于调试）"""
        return self.dead_letter_queue[-limit:]

# 使用示例
if __name__ == "__main__":
    
    async def brain_receive(data: Dict[str, Any]):
        """大脑接收函数"""
        print(f"🧠 收到数据: {data.get('symbol', 'N/A')} | 类型: {data.get('data_type', 'N/A')}")
    
    async def main():
        # 1. 创建管理员（单例）
        config = PipelineConfig(step1_batch_size=20, cleanup_interval=600)
        manager = PipelineManager(brain_callback=brain_receive, config=config)
        
        # 2. 启动
        await manager.start()
        
        # 3. 模拟数据流入
        test_data = [
            {"exchange": "binance", "symbol": "BTCUSDT", "data_type": "funding_rate", "raw_data": {"r": 0.0001, "T": 1234567890000}},
            {"exchange": "okx", "symbol": "BTCUSDT", "data_type": "funding_rate", "raw_data": {"instId": "BTC-USDT-SWAP", "fundingRate": 0.0002}},
            {"exchange": "binance", "data_type": "account", "balance": 10000}  # 这条会直连大脑
        ]
        
        for data in test_data:
            await manager.ingest_data(data)
        
        # 4. 运行一段时间
        await asyncio.sleep(10)
        
        # 5. 获取状态
        print("\n" + "="*60)
        print("最终状态:", manager.get_status())
        print("="*60)
        
        # 6. 优雅停止
        await manager.stop()
    
    asyncio.run(main())
