# brain_core/pipeline_starter.py
"""
数据处理流水线独立启动器
"""

import asyncio
import logging
from shared_data import data_store
from shared_data.pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)

class PipelineStarter:
    def __init__(self, brain_callback):
        self.brain_callback = brain_callback
        self.pipeline_manager = None
        self.running = False
    
    async def start(self):
        """启动流水线"""
        try:
            logger.info("🚀 启动数据处理流水线...")
            
            # 1. 创建流水线管理员
            self.pipeline_manager = PipelineManager()
            
            # 2. 连接数据存储
            data_store.set_pipeline_manager(self.pipeline_manager)
            
            # 3. 设置大脑回调
            self.pipeline_manager.set_brain_callback(self.brain_callback)
            
            # 4. 启动
            await self.pipeline_manager.start_pipeline()
            
            self.running = True
            logger.info("✅ 数据处理流水线已启动")
            return True
            
        except Exception as e:
            logger.error(f"启动流水线失败: {e}")
            return False
    
    async def stop(self):
        """停止流水线"""
        if self.pipeline_manager and self.running:
            await self.pipeline_manager.stop_pipeline()
            self.running = False
            logger.info("🛑 数据处理流水线已停止")