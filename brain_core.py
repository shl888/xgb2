#!/usr/bin/env python3
"""
大脑核心主控 - Render优化版（防重启版）
关键点：先注册路由，再启动HTTP服务器
"""

import asyncio
import logging
import signal
import sys
import os
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
    # ✅ 把receive_processed_data定义移到前面
    async def receive_processed_data(self, processed_data):
        """接收成品数据"""
        try:
            data_type = processed_data.get('type', 'unknown')
            exchange = processed_data.get('exchange', 'unknown')
            symbol = processed_data.get('symbol', 'unknown')
            logger.info(f"🧠 收到数据: {exchange}:{symbol} ({data_type})")
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
    
    def __init__(self):
        async def direct_to_datastore(data: dict):
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
        
        # ✅ 现在receive_processed_data已经定义了，可以安全调用
        data_store.set_brain_callback(self.receive_processed_data)
        
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def initialize(self):
        """初始化（防重启版）"""
        logger.info("=" * 60)
        logger.info("大脑核心启动中...")
        logger.info("=" * 60)
        
        try:
            # 1. 创建HTTP服务器配置
            port = int(os.getenv('PORT', 10000))
            logger.info(f"【1️⃣】创建HTTP服务器配置...")
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # ✅ 在这里注册路由！（服务器启动前）
            logger.info("【2️⃣】注册所有路由...")
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            # 2. 启动服务器
            logger.info("【3️⃣】启动HTTP服务器...")
            await self.start_http_server()
            
            # 3. 标记为就绪
            data_store.set_http_server_ready(True)
            logger.info("✅ HTTP服务已就绪！")
            
            # 4. 后台启动其他服务
            logger.info("【4️⃣】后台启动其他服务...")
            start_keep_alive_background()
            
            # WebSocket延迟启动
            asyncio.create_task(self._delayed_ws_init())
            
            self.running = True
            logger.info("=" * 60)
            logger.info("🚀 大脑核心启动完成！")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            return False
    
    async def _delayed_ws_init(self):
        """延迟初始化WebSocket（防止启动超时）"""
        await asyncio.sleep(5)
        try:
            await self.ws_admin.start()
            logger.info("✅ WebSocket模块初始化完成")
        except Exception as e:
            logger.error(f"WebSocket初始化失败: {e}")
    
    async def start_http_server(self):
        """启动HTTP服务"""
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
    
    async def _auto_fetch_funding_settlement(self):
        """后台获取资金费率结算数据"""
        if not hasattr(self, 'funding_manager'):
            return
        
        try:
            logger.info("后台任务: 开始获取资金费率结算数据...")
            result = await self.funding_manager.fetch_funding_settlement()
            if result['success']:
                logger.info(f"✅ 成功！合约数: {result['filtered_count']}, 权重: {result['weight_used']}")
            else:
                logger.error(f"❌ 失败: {result.get('error')}")
        except Exception as e:
            logger.error(f"后台获取异常: {e}")
    
    async def run(self):
        """主循环"""
        try:
            success = await self.initialize()
            if not success:
                return
            
            logger.info("🚀 大脑核心运行中...")
            
            while self.running:
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
        except Exception as e:
            logger.error(f"运行错误: {e}")
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        self.running = False
    
    async def shutdown(self):
        """优雅关闭"""
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            if self.ws_admin:
                await self.ws_admin.stop()
            if self.http_runner:
                await self.http_runner.cleanup()
            logger.info("✅ 已关闭")
        except Exception as e:
            logger.error(f"关闭出错: {e}")
        
        sys.exit(0)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    brain = BrainCore()
    
    try:
        asyncio.run(brain.run())
    except KeyboardInterrupt:
        logger.info("已停止")
    except Exception as e:
        logger.error(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
