#!/usr/bin/env python3
"""
大脑核心主控 - Render优化版（最终版）
"""

import asyncio
import logging
import signal
import sys
import os
import traceback  # ✅ 修复：添加这行
from datetime import datetime

# 设置路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store

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
        self.data_handlers = []
        
        # 初始化funding_manager为None
        self.funding_manager = None
        
        # 注册脑回调
        data_store.set_brain_callback(self.receive_processed_data)
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def receive_processed_data(self, processed_data):
        """接收过滤后的成品数据"""
        try:
            data_type = processed_data.get('type', 'unknown')
            exchange = processed_data.get('exchange', 'unknown')
            symbol = processed_data.get('symbol', 'unknown')
            logger.info(f"🧠 收到数据: {exchange}:{symbol} ({data_type})")
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
    
    async def initialize(self):
        """初始化（最终版）"""
        logger.info("=" * 60)
        logger.info("大脑核心启动中...")
        logger.info("=" * 60)
        
        try:
            # 1. 创建HTTP服务器
            port = int(os.getenv('PORT', 10000))
            logger.info(f"【1️⃣】创建HTTP服务器 (端口: {port})...")
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # 2. ✅ 先注册路由（服务器启动前）
            logger.info("【2️⃣】注册所有路由...")
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            # 3. 再启动服务器
            logger.info("【3️⃣】启动HTTP服务器...")
            await self.start_http_server()
            
            # 4. 标记HTTP就绪
            data_store.set_http_server_ready(True)
            logger.info("✅ HTTP服务已就绪！")
            
            # 5. 初始化资金费率管理器
            logger.info("【4️⃣】初始化资金费率管理器...")
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            # 6. 后台启动保活服务
            logger.info("【5️⃣】后台启动保活服务...")
            start_keep_alive_background()
            
            # 7. 启动后台任务（延迟执行）
            asyncio.create_task(self._delayed_ws_init())
            asyncio.create_task(self._delayed_funding_fetch())
            
            self.running = True
            logger.info("=" * 60)
            logger.info("🚀 大脑核心启动完成！")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"🚨 初始化失败: {e}")
            logger.error(traceback.format_exc())  # ✅ 现在不会报错了
            return False
    
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
                
                if check_counter % 30 == 0:
                    logger.info("💓 系统运行正常...")
        
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
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
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
