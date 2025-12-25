#!/usr/bin/env python3
"""
大脑核心主控 - Render优化版
优化启动顺序：先启动HTTP服务，再启动保活，最后后台初始化WebSocket
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime
from typing import Dict, Any

# 设置路径 - Render兼容版
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ✅ 修改1：只导入管理员类
from websocket_pool.admin import WebSocketAdmin

from http_server.server import HTTPServer
from shared_data.data_store import data_store

logger = logging.getLogger(__name__)

# ============ 【优化版保活启动函数】============
def start_keep_alive_background():
    """
    启动保活服务（后台线程）- 优化版
    确保HTTP服务就绪后再开始保活
    """
    try:
        # 使用新的启动函数（带HTTP就绪检查）
        from keep_alive import start_with_http_check
        
        import threading
        
        def run_keeper():
            """保活服务运行函数"""
            try:
                # ✅ 使用新的智能启动函数
                start_with_http_check()
            except Exception as e:
                logger.error(f"保活服务运行异常: {e}")
                # 保活失败不影响主程序
                # 简单重启逻辑（防止循环崩溃）
                import time
                time.sleep(60)  # 等待1分钟再试
                try:
                    start_with_http_check()
                except:
                    logger.error("保活服务重启失败，将停止运行")
        
        # 检查环境变量是否设置
        app_url = os.environ.get("APP_URL")
        if not app_url or "your-app" in app_url:
            logger.warning("⚠️  未设置APP_URL环境变量，保活服务可能无法正确工作")
            logger.info("💡 请在Render环境变量中设置: APP_URL=https://你的应用.onrender.com")
            # 但还是启动，让用户知道需要配置
        else:
            logger.info(f"✅ 检测到APP_URL: {app_url}")
        
        # 启动后台线程（守护线程，主程序退出时会自动结束）
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logger.info("✅ 保活服务已在后台启动（智能错峰版）")
        
    except ImportError as e:
        logger.error(f"无法导入保活模块: {e}")
        logger.info("⚠️  保活服务未启动，但主程序继续运行")
    except Exception as e:
        logger.error(f"启动保活服务失败: {e}")
        logger.info("⚠️  保活服务未启动，但主程序继续运行")
# ============ 【优化版保活启动函数结束】============

class BrainCore:
    """大脑核心 - 总控制器（Render优化版）"""
    
    def __init__(self):
        # ✅【关键修改1】创建直接对接共享数据模块的回调函数
        async def direct_to_datastore(data: Dict[str, Any]):
            """
            直接对接共享数据模块的回调
            WebSocket数据直接进入共享数据模块，不经过大脑的原始数据处理
            """
            try:
                # 验证数据格式
                if not isinstance(data, dict):
                    logger.error(f"回调数据不是字典类型: {type(data)}")
                    return
                    
                exchange = data.get("exchange")
                symbol = data.get("symbol")
                
                if not exchange:
                    logger.error(f"数据缺少exchange字段: {data.keys()}")
                    return
                if not symbol:
                    logger.error(f"数据缺少symbol字段: {data.keys()}")
                    return
                    
                # ✅【关键】直接调用 data_store.update_market_data
                # 传递三个参数：exchange, symbol, data
                await data_store.update_market_data(exchange, symbol, data)
                
                # 调试日志（可选）
                direct_to_datastore.counter = getattr(direct_to_datastore, 'counter', 0) + 1
                if direct_to_datastore.counter % 100 == 0:
                    logger.info(f"[大脑回调] 直接处理 {direct_to_datastore.counter} 条数据到共享模块")
                    
            except TypeError as e:
                # 如果参数错误，尝试备用方法
                logger.error(f"回调参数错误: {e}")
                logger.error(f"数据格式: {type(data)}")
                if isinstance(data, dict):
                    logger.error(f"数据keys: {list(data.keys())}")
            except Exception as e:
                logger.error(f"直接对接回调错误: {e}")
        
        # ✅【关键修改2】使用直接对接的回调
        self.ws_admin = WebSocketAdmin(direct_to_datastore)
        
        # 核心组件
        self.http_server = None
        self.http_runner = None
        
        # 状态
        self.running = False
        self.data_handlers = []
        
        # ✅ 设置大脑回调：接收过滤后的成品数据
        data_store.set_brain_callback(self.receive_processed_data)
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def initialize(self):
        """初始化所有模块 - 修正启动顺序"""
        logger.info("=" * 60)
        logger.info("大脑核心启动中...")
        logger.info("=" * 60)
        
        try:
            # 检查环境
            self.check_environment()
            
            # ✅ 【第一步】先初始化HTTP服务器（最重要！）
            port = int(os.getenv('PORT', 10000))  # Render要求端口10000
            logger.info(f"【第一步】初始化HTTP服务器 (端口: {port})...")
            
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # ✅ 立即启动HTTP服务器（不等待其他组件）
            await self.start_http_server()
            
            # ✅ 等待HTTP服务完全就绪（关键！）
            await self._wait_for_http_ready()
            
            # ✅ 【第二步】启动保活服务（HTTP就绪后才启动）
            logger.info("【第二步】启动后台保活服务...")
            start_keep_alive_background()
            
            # ✅ 【第三步】初始化WebSocket模块（使用直接对接回调）
            logger.info("【第三步】初始化WebSocket模块...")
            # ✅ 重要修改：WebSocket使用direct_to_datastore回调，直接对接data_store
            await self.ws_admin.start()
            
            # 可以保留原有的数据处理器（但处理器现在接收的是成品数据）
            self.add_data_handler(self.log_important_data)
            
            self.running = True
            logger.info("✅ HTTP服务已就绪！保活服务已启动！")
            logger.info("✅ WebSocket模块已启动（数据直接进入共享数据模块）...")
            logger.info("🧠 大脑已设置为只接收过滤后的成品数据")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _wait_for_http_ready(self):
        """等待HTTP服务完全就绪"""
        logger.info("等待HTTP服务就绪...")
        
        max_attempts = 10  # 最多尝试10次
        for i in range(max_attempts):
            try:
                # 使用aiohttp客户端检查服务
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(f'http://0.0.0.0:{os.getenv("PORT", 10000)}/health', timeout=2) as response:
                        if response.status == 200 or response.status == 202:
                            logger.info(f"✅ HTTP服务已就绪 (尝试 {i+1}/{max_attempts})")
                            return True
            except Exception as e:
                if i < max_attempts - 1:
                    logger.debug(f"HTTP服务检查中... (尝试 {i+1}/{max_attempts})")
                    await asyncio.sleep(1)  # 等待1秒再试
                else:
                    logger.warning(f"HTTP服务检查超时，但继续启动流程: {e}")
        
        return True  # 即使检查失败也继续（可能是检查方法问题）
    
    # ✅ 修改：接收过滤后的成品数据（不再是原始数据）
    async def receive_processed_data(self, processed_data):
        """
        接收过滤处理后的成品数据
        大脑只处理这种精炼数据
        """
        try:
            # 简单日志
            data_type = processed_data.get('type', 'unknown')
            exchange = processed_data.get('exchange', 'unknown')
            symbol = processed_data.get('symbol', 'unknown')
            
            logger.info(f"🧠 收到成品数据: {exchange}:{symbol} ({data_type})")
            
            # 调用数据处理器
            for handler in self.data_handlers:
                try:
                    await handler(processed_data)
                except Exception as e:
                    logger.error(f"数据处理器错误: {e}")
            
        except Exception as e:
            logger.error(f"大脑接收成品数据错误: {e}")
    
    def check_environment(self):
        """检查环境配置"""
        logger.info("环境检查:")
        logger.info(f"Python版本: {sys.version}")
        
        # 检查端口配置
        port = os.getenv('PORT', '10000')
        logger.info(f"服务端口: {port}")
        if port != '10000':
            logger.warning("⚠️  Render要求使用端口10000，当前配置为: %s", port)
        
        # 检查API密钥配置
        api_configs = {
            '币安': ['BINANCE_API_KEY', 'BINANCE_API_SECRET'],
            '欧意': ['OKX_API_KEY', 'OKX_API_SECRET', 'OKX_PASSPHRASE']
        }
        
        for name, keys in api_configs.items():
            has_keys = all(os.getenv(key) for key in keys)
            status = "✅ 已配置" if has_keys else "⚠️  未配置（仅公开数据）"
            logger.info(f"  {name}: {status}")
    
    def add_data_handler(self, handler):
        """添加数据处理器"""
        self.data_handlers.append(handler)
        logger.info(f"已添加数据处理器: {handler.__name__}")
    
    async def log_important_data(self, data):
        """示例数据处理器 - 现在处理的是成品数据"""
        # 这里处理的是过滤后的成品数据
        data_type = data.get('type')
        
        if data_type == 'funding_decision':
            rate = data.get('original_rate', 0)
            symbol = data.get('symbol', 'unknown')
            if abs(rate) > 0.0003:
                logger.info(f"[大脑处理] 高资金费率: {symbol} = {rate:.6f}")
    
    async def start_http_server(self):
        """启动HTTP服务器 - Render兼容版"""
        try:
            from aiohttp import web
            
            port = int(os.getenv('PORT', 10000))
            host = '0.0.0.0'
            
            # 获取HTTP服务器的app
            app = self.http_server.app
            
            # 创建并启动runner
            runner = web.AppRunner(app)
            await runner.setup()
            
            site = web.TCPSite(runner, host, port)
            await site.start()
            
            # 保存runner以便关闭
            self.http_runner = runner
            
            logger.info(f"✅ HTTP服务器已启动: http://{host}:{port}")
            logger.info(f"✅ 健康检查: http://{host}:{port}/health")
            logger.info(f"✅ 公开端点: http://{host}:{port}/public/ping")
            
        except Exception as e:
            logger.error(f"启动HTTP服务器失败: {e}")
            raise
    
    async def check_connection_status(self):
        """定期检查连接状态"""
        if not self.ws_admin or not self.running:
            return
        
        try:
            # 通过管理员获取状态
            status = await self.ws_admin.get_status()
            logger.info(f"[状态检查] WebSocket模块状态: {status}")
            
        except Exception as e:
            logger.error(f"检查连接状态错误: {e}")
    
    async def run(self):
        """运行大脑核心"""
        try:
            # 初始化
            success = await self.initialize()
            if not success:
                logger.error("初始化失败，程序退出")
                return
            
            # 主循环
            logger.info("🚀 大脑核心运行中...")
            logger.info("🛑 按 Ctrl+C 停止")
            
            check_counter = 0
            while self.running:
                try:
                    await asyncio.sleep(1)
                    check_counter += 1
                    
                    # 每30秒检查一次连接状态
                    if check_counter % 30 == 0:
                        await self.check_connection_status()
                        
                except asyncio.CancelledError:
                    logger.info("任务被取消，开始关闭流程")
                    break
                    
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
        except Exception as e:
            logger.error(f"运行错误: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        logger.info(f"收到信号 {signum}，正在关闭...")
        self.running = False  # 只设置标志，不直接开始关闭
    
    async def shutdown(self):
        """优雅关闭"""
        if not self.running:
            return
        
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            # 通过管理员停止WebSocket模块
            if self.ws_admin:
                await self.ws_admin.stop()
            
            # 关闭HTTP服务器
            if self.http_runner:
                await self.http_runner.cleanup()
            
            logger.info("✅ 大脑核心已关闭")
            
        except Exception as e:
            logger.error(f"关闭过程中出错: {e}")
        
        sys.exit(0)

def main():
    """主函数"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 创建并运行大脑核心
    brain = BrainCore()
    
    try:
        asyncio.run(brain.run())
    except KeyboardInterrupt:
        logger.info("程序已停止")
    except Exception as e:
        logger.error(f"程序错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
