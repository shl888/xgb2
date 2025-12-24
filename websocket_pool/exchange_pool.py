"""
单个交易所的连接池管理 - 监控调度版
监控连接负责调度管理主备切换
"""
import asyncio
import logging
import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # brain_core目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .connection import WebSocketConnection, ConnectionType
from .config import EXCHANGE_CONFIGS

logger = logging.getLogger(__name__)

class ExchangeWebSocketPool:
    """单个交易所的WebSocket连接池 - 监控调度版"""
    
    def __init__(self, exchange: str, data_callback=None):
        self.exchange = exchange
        if data_callback:
            self.data_callback = data_callback
        else:
            self.data_callback = self._create_default_callback()
            
        self.config = EXCHANGE_CONFIGS.get(exchange, {})
        
        self.master_connections = []
        self.warm_standby_connections = []
        self.monitor_connection = None
        
        self.symbols = []
        self.symbol_groups = []
        
        self.health_check_task = None
        self.monitor_scheduler_task = None
    
    def _create_default_callback(self):
        async def default_callback(data):
            try:
                if "exchange" not in data or "symbol" not in data:
                    return
                    
                await data_store.update_market_data(
                    data["exchange"],
                    data["symbol"],
                    data
                )
            except Exception as e:
                logger.error(f"[{self.exchange}] 数据存储失败: {e}")
        return default_callback
        
    async def initialize(self, symbols: List[str]):
        self.symbols = symbols
        
        symbols_per_master = self.config.get("symbols_per_master", 300)
        self.symbol_groups = [
            symbols[i:i + symbols_per_master]
            for i in range(0, len(symbols), symbols_per_master)
        ]
        
        masters_count = self.config.get("masters_count", 3)
        if len(self.symbol_groups) > masters_count:
            self._balance_symbol_groups(masters_count)
        
        logger.info(f"[{self.exchange}] 初始化连接池，共 {len(symbols)} 个合约，分为 {len(self.symbol_groups)} 组")
        
        await self._initialize_masters()
        await self._initialize_warm_standbys()
        await self._initialize_monitor_scheduler()
        
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
    def _balance_symbol_groups(self, target_groups: int):
        avg_size = len(self.symbols) // target_groups
        remainder = len(self.symbols) % target_groups
        
        self.symbol_groups = []
        start = 0
        
        for i in range(target_groups):
            size = avg_size + (1 if i < remainder else 0)
            if start + size <= len(self.symbols):
                self.symbol_groups.append(self.symbols[start:start + size])
                start += size
        
        logger.info(f"[{self.exchange}] 合约重新平衡为 {len(self.symbol_groups)} 组")
    
    async def _initialize_masters(self):
        ws_url = self.config.get("ws_public_url")
        
        for i, symbol_group in enumerate(self.symbol_groups):
            conn_id = f"{self.exchange}_master_{i}"
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.MASTER,
                data_callback=self.data_callback,
                symbols=symbol_group
            )
            
            success = await connection.connect()
            if success:
                self.master_connections.append(connection)
                logger.info(f"[{conn_id}] 主连接启动成功")
        
        logger.info(f"[{self.exchange}] 主连接初始化完成: {len(self.master_connections)} 个")
    
    async def _initialize_warm_standbys(self):
        ws_url = self.config.get("ws_public_url")
        warm_standbys_count = self.config.get("warm_standbys_count", 3)
        
        for i in range(warm_standbys_count):
            heartbeat_symbols = self._get_heartbeat_symbols()
            
            conn_id = f"{self.exchange}_warm_{i}"
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.WARM_STANDBY,
                data_callback=self.data_callback,
                symbols=heartbeat_symbols
            )
            
            success = await connection.connect()
            if success:
                self.warm_standby_connections.append(connection)
                logger.info(f"[{conn_id}] 温备连接启动成功")
        
        logger.info(f"[{self.exchange}] 温备连接初始化完成: {len(self.warm_standby_connections)} 个")
    
    def _get_heartbeat_symbols(self):
        if self.exchange == "binance":
            return ["BTCUSDT"]
        elif self.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    async def _initialize_monitor_scheduler(self):
        """🚨【核心修复】确保监控调度循环一定启动"""
        ws_url = self.config.get("ws_public_url")
        if not ws_url:
            logger.warning(f"[{self.exchange}] 缺少WebSocket URL配置")
            # 即使没有URL也继续
            ws_url = ""
        
        conn_id = f"{self.exchange}_monitor"
        
        # 1. 创建监控连接
        try:
            self.monitor_connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.MONITOR,
                data_callback=self.data_callback,
                symbols=[]
            )
        except Exception as e:
            logger.warning(f"[{self.exchange}] 创建监控连接对象失败: {e}")
            self.monitor_connection = None
        
        # 2. 尝试连接（但不依赖结果）
        if self.monitor_connection:
            try:
                success = await self.monitor_connection.connect()
                if success:
                    logger.info(f"[{conn_id}] 监控连接成功")
                else:
                    logger.warning(f"[{conn_id}] 监控连接失败，但继续启动调度循环")
            except Exception as e:
                logger.warning(f"[{conn_id}] 监控连接异常: {e}")
        
        # 3. 🚨【关键】无论如何都启动调度循环
        if not self.monitor_scheduler_task or self.monitor_scheduler_task.done():
            try:
                self.monitor_scheduler_task = asyncio.create_task(
                    self._monitor_scheduling_loop()
                )
                logger.info(f"[{conn_id}] 监控调度循环已启动")
            except Exception as e:
                logger.error(f"[{conn_id}] 启动监控调度循环失败: {e}")
                # 重试一次
                try:
                    await asyncio.sleep(1)
                    self.monitor_scheduler_task = asyncio.create_task(
                        self._monitor_scheduling_loop()
                    )
                    logger.info(f"[{conn_id}] 监控调度循环重试成功")
                except Exception as retry_error:
                    logger.error(f"[{conn_id}] 监控调度循环重试失败: {retry_error}")
    
    async def _monitor_scheduling_loop(self):
        """监控调度循环"""
        logger.info(f"[监控调度] {self.exchange} 监控调度循环开始")
        
        while True:
            try:
                # 检查主连接状态
                for i, master_conn in enumerate(self.master_connections):
                    if not master_conn.connected:
                        await self._monitor_handle_master_failure(i, master_conn)
                
                # 检查温备连接状态
                for i, warm_conn in enumerate(self.warm_standby_connections):
                    if not warm_conn.connected:
                        await warm_conn.connect()
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"[监控调度] 调度循环错误: {e}")
                await asyncio.sleep(3)
    
    async def _select_best_standby_from_pool(self):
        available_standbys = [
            conn for conn in self.warm_standby_connections 
            if conn.connected and not conn.is_active
        ]
        
        if not available_standbys:
            return None
        
        selected_standby = min(
            available_standbys,
            key=lambda conn: (
                conn.last_message_seconds_ago or 999,
                conn.reconnect_count,
                len(conn.symbols)
            )
        )
        
        return selected_standby
    
    async def _monitor_handle_master_failure(self, master_index: int, failed_master):
        standby_conn = await self._select_best_standby_from_pool()
        
        if not standby_conn:
            await failed_master.connect()
            return
        
        await self._monitor_execute_failover(master_index, failed_master, standby_conn)
    
    async def _monitor_execute_failover(self, master_index: int, old_master, new_master):
        try:
            if old_master.connected and old_master.subscribed:
                await old_master._unsubscribe()
            
            old_master.symbols = []
            
            master_symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else []
            
            success = await new_master.switch_role(ConnectionType.MASTER, master_symbols)
            if not success:
                return False
            
            if new_master in self.warm_standby_connections:
                self.warm_standby_connections.remove(new_master)
            
            self.master_connections[master_index] = new_master
            
            await old_master.disconnect()
            await asyncio.sleep(1)
            
            if await old_master.connect():
                heartbeat_symbols = self._get_heartbeat_symbols()
                await old_master.switch_role(ConnectionType.WARM_STANDBY, heartbeat_symbols)
                
                if old_master not in self.warm_standby_connections:
                    self.warm_standby_connections.append(old_master)
            
            return True
            
        except Exception as e:
            logger.error(f"[监控调度] 故障转移执行失败: {e}")
            return False
    
    async def _report_status_to_data_store(self):
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "monitor": None,
            }
            
            for conn in self.master_connections:
                status = await conn.check_health()
                status_report["masters"].append(status)
            
            for conn in self.warm_standby_connections:
                status = await conn.check_health()
                status_report["warm_standbys"].append(status)
            
            if self.monitor_connection:
                status = await self.monitor_connection.check_health()
                status_report["monitor"] = status
            
            await data_store.update_connection_status(
                self.exchange, 
                "websocket_pool", 
                status_report
            )
            
        except Exception as e:
            pass
    
    async def _health_check_loop(self):
        while True:
            try:
                masters_connected = sum(1 for c in self.master_connections if c.connected)
                if masters_connected < len(self.master_connections):
                    logger.info(f"[健康检查] {self.exchange}: {masters_connected}/{len(self.master_connections)} 个主连接活跃")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                await asyncio.sleep(30)
    
    async def get_status(self):
        return await self._report_status_to_data_store()
    
    async def shutdown(self):
        logger.info(f"[{self.exchange}] 正在关闭连接池...")
        
        if self.health_check_task:
            self.health_check_task.cancel()
        if self.monitor_scheduler_task:
            self.monitor_scheduler_task.cancel()
        
        tasks = []
        for conn in self.master_connections:
            tasks.append(conn.disconnect())
        for conn in self.warm_standby_connections:
            tasks.append(conn.disconnect())
        if self.monitor_connection:
            tasks.append(self.monitor_connection.disconnect())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"[{self.exchange}] 连接池已关闭")
