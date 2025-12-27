#!/usr/bin/env python3
"""
大脑核心主控 - PipelineManager集成版
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime

# 设置路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store
from shared_data.pipeline_manager import PipelineManager, PipelineConfig  # 新增导入

logger = logging.getLogger(__name__)


def start_keep_alive_background():
    """启动保活服务（后台线程）"""
    try:
        from keep_alive import start_with_http_check
        import threading
        
        def run_keeper():
            try:
                start_with_http_check()
            except Exception as e:
                logger.error(f"保活服务异常: {e}")
        
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logger.info("✅ 保活服务已启动")
    except:
        logger.warning("⚠️  保活服务未启动，但继续运行")


class BrainCore:
    def __init__(self):
        async def direct_to_datastore(data: dict):
            """WebSocket回调，直接对接data_store"""
            try:
                exchange = data.get("exchange")
                symbol = data.get("symbol")
                if exchange and symbol:
                    await data_store.update_market_data(exchange, symbol, data)
            except Exception as e:
                logger.error(f"回调错误: {e}")
        
        self.ws_admin = WebSocketAdmin(direct_to_datastore)
        self.http_server = None
        self.http_runner = None
        self.running = False
        
        # 流水线管理员
        self.pipeline_manager = None
        
        # 资金费率管理器
        self.funding_manager = None
        
        # 注册脑回调到data_store
        data_store.set_brain_callback(self.receive_processed_data)
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def receive_processed_data(self, processed_data: dict):
        """接收流水线处理后的成品数据（增强版）"""
        try:
            data_type = processed_data.get('data_type', 'unknown')
            exchange = processed_data.get('exchange', 'unknown')
            symbol = processed_data.get('symbol', 'unknown')
            
            # 打印接收到的数据摘要
            if data_type.startswith('cross_platform'):
                # 跨平台套利数据
                logger.info(f"🎯 套利数据: {exchange}:{symbol} | 价差: {processed_data.get('price_diff', 0):.6f}")
                # 在这里可以触发交易逻辑
                
            elif data_type.startswith('account_'):
                # 账户数据
                logger.info(f"💰 账户更新: {exchange} | 类型: {data_type}")
                # 在这里更新账户状态
                
            elif data_type == 'order':
                # 订单数据
                status = processed_data.get('payload', {}).get('status', 'unknown')
                logger.info(f"📝 订单更新: {exchange}:{symbol} | 状态: {status}")
                # 在这里更新订单状态
                
            elif data_type in ['ticker', 'funding_rate', 'mark_price']:
                # 市场数据（显示但不频繁打印）
                price = processed_data.get('payload', {}).get('latest_price', 0)
                if symbol in ['BTCUSDT', 'ETHUSDT']:  # 只打印重要币种
                    logger.debug(f"📊 市场数据: {exchange}:{symbol} | 价格: {price}")
                    
            else:
                logger.debug(f"📨 收到数据: {exchange}:{symbol} | 类型: {data_type}")
                
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
            logger.debug(f"错误数据: {processed_data}")
    
    async def initialize(self):
        """初始化（PipelineManager集成版）"""
        logger.info("=" * 60)
        logger.info("大脑核心启动中...")
        logger.info("=" * 60)
        
        try:
            # 1. 创建HTTP服务器
            port = int(os.getenv('PORT', 10000))
            logger.info(f"【1️⃣】创建HTTP服务器 (端口: {port})...")
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # 2. 注册路由
            logger.info("【2️⃣】注册所有路由...")
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            # 3. 启动HTTP服务器
            logger.info("【3️⃣】启动HTTP服务器...")
            await self.start_http_server()
            
            # 4. 标记HTTP就绪
            data_store.set_http_server_ready(True)
            logger.info("✅ HTTP服务已就绪！")
            
            # 5. **初始化并启动流水线管理员（新增）**
            logger.info("【4️⃣】初始化流水线管理员...")
            config = PipelineConfig(
                step1_batch_size=20,
                step2_batch_size=30,
                step3_batch_size=30,
                step4_batch_size=50,
                enable_monitoring=True  # 启用监控
            )
            
            self.pipeline_manager = PipelineManager(
                brain_callback=self.receive_processed_data,  # 设置回调
                config=config
            )
            
            # 启动流水线
            await self.pipeline_manager.start()
            logger.info("✅ 流水线管理员启动完成！")
            
            # 6. **让data_store引用流水线管理员（双向连接）**
            data_store.pipeline_manager = self.pipeline_manager
            logger.info("✅ DataStore ↔ PipelineManager 连接建立")
            
            # 7. 初始化资金费率管理器
            logger.info("【5️⃣】初始化资金费率管理器...")
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            # 8. 后台启动保活服务
            logger.info("【6️⃣】后台启动保活服务...")
            start_keep_alive_background()
            
            # 9. 启动后台任务（延迟执行）
            asyncio.create_task(self._delayed_ws_init())
            asyncio.create_task(self._delayed_funding_fetch())
            asyncio.create_task(self._monitor_pipeline())  # 新增：监控流水线
            
            self.running = True
            logger.info("=" * 60)
            logger.info("🚀 大脑核心启动完成！")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"🚨 初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _monitor_pipeline(self):
        """监控流水线状态"""
        await asyncio.sleep(15)  # 等待流水线稳定
        
        while self.running:
            try:
                if self.pipeline_manager:
                    # 获取流水线状态报告
                    report = self.pipeline_manager.get_pipeline_report()
                    
                    # 每30秒打印一次摘要
                    logger.info(f"📈 流水线状态: {report.get('total_processed', 0)}条已处理")
                    logger.info(f"   成功率: {report.get('success_rate', 0):.1%}")
                    logger.info(f"   当前队列: {report.get('queue_size', 0)}条")
                    
                    # 检查异常
                    if report.get('success_rate', 0) < 0.8:
                        logger.warning("⚠️  流水线成功率较低，请检查")
                    
            except Exception as e:
                logger.error(f"监控流水线失败: {e}")
            
            await asyncio.sleep(30)  # 每30秒检查一次
    
    async def _delayed_ws_init(self):
        """延迟10秒启动WebSocket，确保其他服务已就绪"""
        await asyncio.sleep(10)
        try:
            logger.info("⏳ 延迟启动WebSocket模块...")
            await self.ws_admin.start()
            logger.info("✅ WebSocket模块初始化完成")
        except Exception as e:
            logger.error(f"WebSocket初始化失败: {e}")
    
    async def _delayed_funding_fetch(self):
        """延迟5秒启动资金费率获取"""
        await asyncio.sleep(5)
        
        if not self.funding_manager:
            logger.error("💥 5秒后funding_manager仍为None，跳过自动获取")
            return
        
        logger.info("=" * 60)
        logger.info("✅ 后台任务：funding_manager已就绪，开始获取数据")
        logger.info("=" * 60)
        
        try:
            result = await self.funding_manager.fetch_funding_settlement()
            
            if result['success']:
                logger.info(f"🎉 后台自动获取成功！合约数: {result['filtered_count']}, 权重: {result['weight_used']}")
            else:
                logger.error(f"❌ 后台自动获取失败: {result.get('error')}")
                
        except Exception as e:
            logger.error(f"💥 后台获取异常: {e}")
            logger.error(traceback.format_exc())
    
    async def start_http_server(self):
        """启动HTTP服务器"""
        try:
            from aiohttp import web
            port = int(os.getenv('PORT', 10000))
            host = '0.0.0.0'
            
            app = self.http_server.app
            runner = web.AppRunner(app)
            await runner.setup()
            
            site = web.TCPSite(runner, host, port)
            await site.start()
            
            self.http_runner = runner
            logger.info(f"✅ HTTP服务器已启动: http://{host}:{port}")
            
        except Exception as e:
            logger.error(f"启动HTTP服务器失败: {e}")
            raise
    
    async def run(self):
        """主循环"""
        try:
            success = await self.initialize()
            if not success:
                logger.error("初始化失败，程序退出")
                return
            
            logger.info("🚀 大脑核心运行中...")
            logger.info("🛑 按 Ctrl+C 停止")
            
            check_counter = 0
            while self.running:
                await asyncio.sleep(1)
                check_counter += 1
                
                # 每60秒打印一次系统状态
                if check_counter % 60 == 0:
                    # 获取系统状态
                    ws_status = self.ws_admin.get_status() if hasattr(self.ws_admin, 'get_status') else "unknown"
                    pipeline_status = "running" if self.pipeline_manager else "stopped"
                    
                    logger.info("💓 系统状态: WS=" + ws_status + " | Pipeline=" + pipeline_status)
        
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
        except Exception as e:
            logger.error(f"运行错误: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        logger.info(f"收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        """优雅关闭"""
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            # 关闭流水线
            if hasattr(self, 'pipeline_manager') and self.pipeline_manager:
                await self.pipeline_manager.stop()
                logger.info("✅ 流水线已关闭")
                
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
                logger.info("✅ WebSocket已关闭")
                
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
                logger.info("✅ HTTP服务器已关闭")
                
            logger.info("✅ 大脑核心已关闭")
        except Exception as e:
            logger.error(f"关闭出错: {e}")
        
        sys.exit(0)


def main():
    """主函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    brain = BrainCore()
    
    try:
        asyncio.run(brain.run())
    except KeyboardInterrupt:
        logger.info("程序已停止")
    except Exception as e:
        logger.error(f"程序错误: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()