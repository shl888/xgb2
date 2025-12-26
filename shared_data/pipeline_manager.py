# shared_data/pipeline_manager.py
"""
流水线管理员 - 轻量级核心调度
负责启动流水线和管理数据流通，确保数据按顺序处理
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

# 🚨 修复：直接在文件中定义常量，避免循环导入
DATA_TYPE_TICKER = "ticker"
DATA_TYPE_FUNDING_RATE = "funding_rate"
DATA_TYPE_MARK_PRICE = "mark_price"
DATA_TYPE_HISTORICAL_FUNDING = "historical_funding"
DATA_TYPE_TRADE = "trade"
DATA_TYPE_ACCOUNT = "account"
DATA_TYPE_ORDER = "order"

EXCHANGE_BINANCE = "binance"
EXCHANGE_OKX = "okx"

STEP_STATUS_PENDING = "pending"
STEP_STATUS_PROCESSING = "processing"
STEP_STATUS_COMPLETED = "completed"
STEP_STATUS_FAILED = "failed"

# 导入各个步骤
from .step1_filter import Step1Filter
from .step2_fusion import Step2Fusion
from .step3_align import Step3Align
from .step4_single_calc import Step4SingleCalc
from .step5_cross_calc import Step5CrossCalc

class PipelineManager:
    """流水线管理员 - 轻量级核心调度"""
    
    def __init__(self):
        # 初始化5个处理步骤
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4SingleCalc()
        self.step5 = Step5CrossCalc()
        
        # 数据队列（确保顺序处理）
        self.queues = {
            'step1_input': asyncio.Queue(maxsize=1000),
            'step1_output': asyncio.Queue(maxsize=1000),
            'step2_output': asyncio.Queue(maxsize=1000),
            'step3_output': asyncio.Queue(maxsize=1000),
            'step4_output': asyncio.Queue(maxsize=1000),
            'step5_input_special': asyncio.Queue(maxsize=100),  # 特殊数据通道
        }
        
        # 处理任务
        self.process_tasks = []
        self.running = False
        
        # 状态监控
        self.stats = {
            'processed_counts': {
                'total_input': 0,
                'step1': 0, 'step2': 0, 'step3': 0, 'step4': 0, 'step5': 0,
                'special_direct': 0, 'filtered_out': 0
            },
            'queue_sizes': {},
            'last_activity': {},
            'throughput_per_minute': 0,
            'error_counts': {f'step{i}': 0 for i in range(1, 6)}
        }
        
        # 性能监控
        self.performance = {
            'start_time': None,
            'total_processing_time': 0,
            'avg_processing_time': 0
        }
        
        logger.info("✅ PipelineManager 初始化完成 - 等待启动")
    
    def set_brain_callback(self, callback):
        """设置大脑回调（传递给Step5）"""
        self.step5.set_brain_callback(callback)
        logger.info("🧠 PipelineManager: 大脑回调已设置")
    
    async def start_pipeline(self):
        """启动整个流水线"""
        if self.running:
            logger.warning("流水线已在运行中")
            return
        
        logger.info("🚀 启动数据处理流水线...")
        self.running = True
        self.performance['start_time'] = datetime.now()
        
        # 启动每个步骤的处理任务
        self.process_tasks = [
            asyncio.create_task(self._run_step1(), name="step1_processor"),
            asyncio.create_task(self._run_step2(), name="step2_processor"),
            asyncio.create_task(self._run_step3(), name="step3_processor"),
            asyncio.create_task(self._run_step4(), name="step4_processor"),
            asyncio.create_task(self._run_step5(), name="step5_processor"),
            asyncio.create_task(self._monitor_loop(), name="pipeline_monitor"),
            asyncio.create_task(self._cleanup_loop(), name="pipeline_cleanup"),
        ]
        
        logger.info("✅ 数据处理流水线已启动（5个步骤 + 监控 + 清理）")
    
    async def route_data(self, raw_data: Dict[str, Any]):
        """
        路由数据到正确的处理流程
        这是主要的入口函数，被data_store调用
        """
        if not self.running:
            logger.warning("流水线未运行，数据被丢弃")
            return
        
        try:
            data_type = raw_data.get("data_type", "")
            
            # 特殊数据：交易/账户数据，直接到Step5
            if data_type in [DATA_TYPE_TRADE, DATA_TYPE_ACCOUNT, DATA_TYPE_ORDER]:
                await self.queues['step5_input_special'].put(raw_data)
                self.stats['processed_counts']['special_direct'] += 1
                self.stats['last_activity']['special_direct'] = datetime.now().isoformat()
                
                if self.stats['processed_counts']['special_direct'] % 10 == 0:
                    logger.debug(f"[管理员] 已处理 {self.stats['processed_counts']['special_direct']} 条特殊数据")
            
            # 市场数据：正常5步流水线
            else:
                await self.queues['step1_input'].put(raw_data)
                self.stats['processed_counts']['total_input'] += 1
                self.stats['last_activity']['step1_input'] = datetime.now().isoformat()
            
        except asyncio.QueueFull:
            logger.warning(f"队列已满，数据被丢弃: {raw_data.get('data_type')}")
        except Exception as e:
            logger.error(f"路由数据失败: {e}")
    
    # ========== 各个步骤的运行函数 ==========
    
    async def _run_step1(self):
        """运行Step1处理"""
        logger.info("▶️ Step1 处理器启动")
        
        while self.running:
            try:
                # 从队列获取数据
                raw_data = await self.queues['step1_input'].get()
                
                # 记录开始时间
                start_time = datetime.now()
                
                # 执行处理
                result = await self.step1.process(raw_data)
                
                # 记录处理时间
                processing_time = (datetime.now() - start_time).total_seconds()
                self.performance['total_processing_time'] += processing_time
                
                # 传递给下一步
                if result:
                    await self.queues['step1_output'].put(result)
                    self.stats['processed_counts']['step1'] += 1
                    self.stats['last_activity']['step1_output'] = datetime.now().isoformat()
                else:
                    self.stats['processed_counts']['filtered_out'] += 1
                
                # 标记任务完成
                self.queues['step1_input'].task_done()
                
                # 控制处理速度
                if processing_time < 0.001:
                    await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Step1] 处理失败: {e}")
                self.stats['error_counts']['step1'] += 1
                try:
                    self.queues['step1_input'].task_done()
                except:
                    pass
        
        logger.info("🛑 Step1 处理器停止")
    
    async def _run_step2(self):
        """运行Step2处理"""
        logger.info("▶️ Step2 处理器启动")
        
        while self.running:
            try:
                data = await self.queues['step1_output'].get()
                
                start_time = datetime.now()
                result = await self.step2.process(data)
                processing_time = (datetime.now() - start_time).total_seconds()
                
                if result:
                    # Step2可能返回多个结果
                    if isinstance(result, list):
                        for item in result:
                            await self.queues['step2_output'].put(item)
                        self.stats['processed_counts']['step2'] += len(result)
                    else:
                        await self.queues['step2_output'].put(result)
                        self.stats['processed_counts']['step2'] += 1
                    
                    self.stats['last_activity']['step2_output'] = datetime.now().isoformat()
                
                self.queues['step1_output'].task_done()
                
                if processing_time < 0.001:
                    await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Step2] 处理失败: {e}")
                self.stats['error_counts']['step2'] += 1
                try:
                    self.queues['step1_output'].task_done()
                except:
                    pass
        
        logger.info("🛑 Step2 处理器停止")
    
    async def _run_step3(self):
        """运行Step3处理"""
        logger.info("▶️ Step3 处理器启动")
        
        while self.running:
            try:
                data = await self.queues['step2_output'].get()
                
                start_time = datetime.now()
                result = await self.step3.process(data)
                processing_time = (datetime.now() - start_time).total_seconds()
                
                if result:
                    # Step3返回对齐后的双平台数据列表
                    for item in result:
                        await self.queues['step3_output'].put(item)
                    self.stats['processed_counts']['step3'] += len(result)
                    self.stats['last_activity']['step3_output'] = datetime.now().isoformat()
                
                self.queues['step2_output'].task_done()
                
                if processing_time < 0.001:
                    await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Step3] 处理失败: {e}")
                self.stats['error_counts']['step3'] += 1
                try:
                    self.queues['step2_output'].task_done()
                except:
                    pass
        
        logger.info("🛑 Step3 处理器停止")
    
    async def _run_step4(self):
        """运行Step4处理"""
        logger.info("▶️ Step4 处理器启动")
        
        while self.running:
            try:
                data = await self.queues['step3_output'].get()
                
                start_time = datetime.now()
                result = await self.step4.process(data)
                processing_time = (datetime.now() - start_time).total_seconds()
                
                if result:
                    await self.queues['step4_output'].put(result)
                    self.stats['processed_counts']['step4'] += 1
                    self.stats['last_activity']['step4_output'] = datetime.now().isoformat()
                
                self.queues['step3_output'].task_done()
                
                if processing_time < 0.001:
                    await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Step4] 处理失败: {e}")
                self.stats['error_counts']['step4'] += 1
                try:
                    self.queues['step3_output'].task_done()
                except:
                    pass
        
        logger.info("🛑 Step4 处理器停止")
    
    async def _run_step5(self):
        """运行Step5处理 - 从两个通道接收数据"""
        logger.info("▶️ Step5 处理器启动")
        
        while self.running:
            try:
                # 处理正常流水线数据（优先级高）
                if not self.queues['step4_output'].empty():
                    data = await self.queues['step4_output'].get()
                    
                    start_time = datetime.now()
                    result = await self.step5.process(data)
                    processing_time = (datetime.now() - start_time).total_seconds()
                    
                    self.stats['processed_counts']['step5'] += 1
                    self.stats['last_activity']['step5_output'] = datetime.now().isoformat()
                    self.queues['step4_output'].task_done()
                    
                    if processing_time < 0.001:
                        await asyncio.sleep(0.001)
                
                # 处理特殊数据（非阻塞检查）
                try:
                    special_data = self.queues['step5_input_special'].get_nowait()
                    await self.step5.process_special_data(special_data)
                    self.queues['step5_input_special'].task_done()
                except asyncio.QueueEmpty:
                    pass
                
                # 短暂休眠避免CPU占用过高
                await asyncio.sleep(0.0005)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Step5] 处理失败: {e}")
                self.stats['error_counts']['step5'] += 1
                try:
                    self.queues['step4_output'].task_done()
                except:
                    pass
        
        logger.info("🛑 Step5 处理器停止")
    
    # ========== 监控和清理函数 ==========
    
    async def _monitor_loop(self):
        """监控循环"""
        logger.info("👁️ 流水线监控启动")
        
        last_minute_count = 0
        last_minute_time = datetime.now()
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # 更新队列状态
                for name, queue in self.queues.items():
                    self.stats['queue_sizes'][name] = queue.qsize()
                
                # 计算每分钟吞吐量
                if (current_time - last_minute_time).total_seconds() >= 60:
                    self.stats['throughput_per_minute'] = self.stats['processed_counts']['step5'] - last_minute_count
                    last_minute_count = self.stats['processed_counts']['step5']
                    last_minute_time = current_time
                    
                    # 记录吞吐量
                    if self.stats['throughput_per_minute'] > 0:
                        logger.debug(f"[监控] 吞吐量: {self.stats['throughput_per_minute']}/分钟")
                
                # 定期状态报告（每30秒）
                if int(current_time.timestamp()) % 30 == 0:
                    self._log_status_summary()
                
                # 检查队列积压
                self._check_queue_backlog()
                
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[监控] 错误: {e}")
                await asyncio.sleep(5)
        
        logger.info("🛑 流水线监控停止")
    
    async def _cleanup_loop(self):
        """清理循环"""
        logger.info("🧹 流水线清理启动")
        
        while self.running:
            try:
                # 每5分钟清理一次
                await asyncio.sleep(300)
                
                # 清理各步骤的旧数据
                await self.step2.cleanup_old_data(max_age_seconds=300)
                await self.step3.cleanup_stale_data(max_age_seconds=600)
                await self.step4.cleanup_old_cache(max_age_hours=24)
                await self.step5.cleanup_old_cache(max_age_hours=6)
                
                logger.debug("流水线清理完成")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[清理] 错误: {e}")
                await asyncio.sleep(60)
        
        logger.info("🛑 流水线清理停止")
    
    def _log_status_summary(self):
        """记录状态摘要"""
        total_processed = sum(self.stats['processed_counts'].values())
        total_errors = sum(self.stats['error_counts'].values())
        
        status = {
            "时间": datetime.now().strftime("%H:%M:%S"),
            "运行时间": str(datetime.now() - self.performance['start_time']).split('.')[0],
            "处理总数": total_processed,
            "错误总数": total_errors,
            "吞吐量/分钟": self.stats['throughput_per_minute'],
            "队列积压": {k: v for k, v in self.stats['queue_sizes'].items() if v > 0},
            "各步骤处理量": {k: v for k, v in self.stats['processed_counts'].items() if v > 0}
        }
        
        logger.info(f"[流水线状态] {status}")
    
    def _check_queue_backlog(self):
        """检查队列积压"""
        for name, size in self.stats['queue_sizes'].items():
            if size > 500:
                logger.warning(f"[队列告警] {name} 积压: {size} 条数据")
    
    # ========== 公共接口 ==========
    
    async def get_status(self) -> Dict[str, Any]:
        """获取流水线状态"""
        if self.performance['start_time']:
            uptime = str(datetime.now() - self.performance['start_time'])
        else:
            uptime = "未启动"
        
        # 计算平均处理时间
        total_processed = self.stats['processed_counts']['step5']
        if total_processed > 0:
            avg_time = self.performance['total_processing_time'] / total_processed
        else:
            avg_time = 0
        
        return {
            "running": self.running,
            "uptime": uptime,
            "stats": self.stats.copy(),
            "performance": {
                "avg_processing_time_ms": round(avg_time * 1000, 2),
                "total_processed": total_processed
            },
            "queues": self.stats['queue_sizes'].copy(),
            "timestamp": datetime.now().isoformat()
        }
    
    async def stop_pipeline(self):
        """停止流水线"""
        if not self.running:
            logger.info("流水线未在运行")
            return
        
        logger.info("🛑 正在停止数据处理流水线...")
        self.running = False
        
        # 取消所有任务
        for task in self.process_tasks:
            task.cancel()
        
        # 等待任务完成
        try:
            await asyncio.gather(*self.process_tasks, return_exceptions=True)
        except:
            pass
        
        # 清空队列
        for queue in self.queues.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except:
                    pass
        
        logger.info("✅ 数据处理流水线已完全停止")
        
        # 记录最终统计
        total_processed = sum(self.stats['processed_counts'].values())
        logger.info(f"最终统计: 共处理 {total_processed} 条数据")
        