# websocket_pool/admin.py
"""
WebSocket连接池管理员 - 生产级实现
大脑核心只与这个类交互，不直接接触模块内部组件
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime

# 模块内部导入（大脑不需要知道这些）
from .pool_manager import WebSocketPoolManager
from .monitor import ConnectionMonitor

logger = logging.getLogger(__name__)

class WebSocketAdmin:
    """
    WebSocket模块管理员
    职责：
    1. 封装WebSocketPoolManager和ConnectionMonitor的复杂性
    2. 对外提供简单的启动、停止、查询接口
    3. 处理模块内部协调逻辑
    4. 向大脑核心汇报精简状态，不暴露内部实现
    """
    
    def __init__(self, data_callback: Optional[Callable] = None):
        """
        初始化管理员
        
        Args:
            data_callback: 数据回调函数，可选（None时使用默认回调）
        """
        # 模块内部组件，对外部隐藏
        self._pool_manager = WebSocketPoolManager(data_callback)
        self._monitor = ConnectionMonitor(self._pool_manager)
        
        # 管理员状态
        self._running = False
        self._initialized = False
        
        logger.info("WebSocketAdmin 初始化完成")
    
    # ========== 对外接口（大脑核心只调用这些方法）==========
    
    async def start(self):
        """启动整个WebSocket模块"""
        if self._running:
            logger.warning("WebSocket模块已在运行中")
            return True
        
        try:
            logger.info("=" * 60)
            logger.info("WebSocketAdmin 正在启动模块...")
            logger.info("=" * 60)
            
            # 1. 初始化连接池
            logger.info("[管理员] 步骤1: 初始化WebSocket连接池")
            await self._pool_manager.initialize()
            
            # 2. 启动监控
            logger.info("[管理员] 步骤2: 启动连接监控")
            await self._monitor.start_monitoring()
            
            self._running = True
            self._initialized = True
            
            logger.info("✅ WebSocketAdmin 模块启动成功")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"WebSocketAdmin 启动失败: {e}")
            await self.stop()  # 确保已启动的部分被关闭
            return False
    
    async def stop(self):
        """停止整个WebSocket模块"""
        if not self._running:
            logger.info("WebSocket模块未在运行")
            return
        
        logger.info("WebSocketAdmin 正在停止模块...")
        
        # 1. 停止监控
        if self._monitor:
            await self._monitor.stop_monitoring()
        
        # 2. 关闭连接池
        if self._pool_manager:
            await self._pool_manager.shutdown()
        
        self._running = False
        logger.info("✅ WebSocketAdmin 模块已停止")
    
    async def get_status(self) -> Dict[str, Any]:
        """获取模块状态摘要（精简信息，不包含内部细节）"""
        try:
            # 获取内部详细状态
            internal_status = await self._pool_manager.get_all_status()
            
            # 精简状态，只汇报大脑核心关心的信息
            summary = {
                "module": "websocket_pool",
                "status": "healthy" if self._running else "stopped",
                "initialized": self._initialized,
                "exchanges": {},
                "timestamp": datetime.now().isoformat()
            }
            
            for exchange, ex_status in internal_status.items():
                if isinstance(ex_status, dict):
                    # 简化交易所状态
                    masters = ex_status.get("masters", [])
                    warm_standbys = ex_status.get("warm_standbys", [])
                    
                    connected_masters = sum(1 for m in masters if isinstance(m, dict) and m.get("connected", False))
                    connected_warm = sum(1 for w in warm_standbys if isinstance(w, dict) and w.get("connected", False))
                    
                    summary["exchanges"][exchange] = {
                        "masters_connected": connected_masters,
                        "masters_total": len(masters),
                        "standbys_connected": connected_warm,
                        "standbys_total": len(warm_standbys),
                        "health": "good" if connected_masters == len(masters) else "warning"
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"WebSocketAdmin 获取状态失败: {e}")
            return {
                "module": "websocket_pool",
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查（快速检查，只返回是否正常）"""
        if not self._running:
            return {
                "healthy": False,
                "message": "模块未运行"
            }
        
        try:
            # 快速检查连接池状态
            status = await self.get_status()
            
            # 检查是否有严重问题
            for exchange_info in status.get("exchanges", {}).values():
                masters_connected = exchange_info.get("masters_connected", 0)
                masters_total = exchange_info.get("masters_total", 0)
                
                if masters_connected == 0 and masters_total > 0:
                    return {
                        "healthy": False,
                        "message": f"交易所主连接全部断开",
                        "details": status
                    }
            
            return {
                "healthy": True,
                "message": "所有交易所主连接正常",
                "details": status
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "message": f"健康检查异常: {e}"
            }
    
    # ========== 扩展接口（可选）==========
    
    async def reconnect_exchange(self, exchange_name: str):
        """重连指定交易所"""
        if exchange_name in self._pool_manager.exchange_pools:
            pool = self._pool_manager.exchange_pools[exchange_name]
            logger.info(f"[管理员] 正在重连交易所: {exchange_name}")
            
            # 调用内部方法重新初始化
            symbols = pool.symbols
            await pool.shutdown()
            await asyncio.sleep(2)
            await pool.initialize(symbols)
            
            logger.info(f"[管理员] 交易所重连完成: {exchange_name}")
            return True
        
        logger.error(f"[管理员] 交易所不存在: {exchange_name}")
        return False
    
    def is_running(self) -> bool:
        """判断模块是否在运行"""
        return self._running
