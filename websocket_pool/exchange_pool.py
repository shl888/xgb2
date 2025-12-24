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
        # ✅【关键修改】使用传入的回调，如果没有则创建默认回调
        if data_callback:
            self.data_callback = data_callback
        else:
            # 创建默认回调，直接对接共享数据模块
            self.data_callback = self._create_default_callback()
            
        self.config = EXCHANGE_CONFIGS.get(exchange, {})
        
        # 连接池
        self.master_connections = []  # 主连接
        self.warm_standby_connections = []  # 温备连接（共享池）
        self.monitor_connection = None  # 监控连接（调度中心）
        
        # 状态
        self.symbols = []  # 所有合约
        self.symbol_groups = []  # 分组后的合约列表
        
        # 任务 - 🚨 简化：只保留必要的健康检查
        self.health_check_task = None
        self.monitor_scheduler_task = None  # 🚨 新增：监控调度任务
    
    def _create_default_callback(self):
        """创建默认回调函数，直接对接共享数据模块"""
        async def default_callback(data):
            """默认数据回调 - 直接存入共享存储"""
            try:
                if "exchange" not in data or "symbol" not in data:
                    logger.warning(f"[{self.exchange}] 数据缺少必要字段: {data}")
                    return
                    
                # ✅【关键修改】直接调用 data_store.update_market_data
                await data_store.update_market_data(
                    data["exchange"],
                    data["symbol"],
                    data
                )
                    
            except Exception as e:
                logger.error(f"[{self.exchange}] 数据存储失败: {e}")
        
        return default_callback
        
    async def initialize(self, symbols: List[str]):
        """初始化连接池"""
        try:
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
            
            # 🚨 添加执行顺序日志
            logger.info(f"[{self.exchange}] 步骤1: 开始初始化主连接")
            
            # 初始化主连接
            await self._initialize_masters()
            
            logger.info(f"[{self.exchange}] 步骤2: 开始初始化温备连接")
            
            # 初始化温备连接
            await self._initialize_warm_standbys()
            
            logger.info(f"[{self.exchange}] 步骤3: 开始初始化监控连接")
            
            # 🚨 初始化监控连接（调度中心）
            await self._initialize_monitor_scheduler()
            
            logger.info(f"[{self.exchange}] 步骤4: 启动健康检查")
            
            # 启动健康检查（只检查，不行动）
            self.health_check_task = asyncio.create_task(self._health_check_loop())
            
            logger.info(f"[{self.exchange}] ✅ 所有初始化步骤完成")
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 初始化连接池失败: {e}", exc_info=True)
            raise
    
    def _balance_symbol_groups(self, target_groups: int):
        """平衡合约分组"""
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
        """初始化主连接"""
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
                logger.info(f"[{conn_id}] 主连接启动成功，订阅 {len(symbol_group)} 个合约")
            else:
                logger.error(f"[{conn_id}] 主连接启动失败")
        
        logger.info(f"[{self.exchange}] 主连接初始化完成: {len(self.master_connections)} 个")
    
    async def _initialize_warm_standbys(self):
        """初始化温备连接"""
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
                logger.info(f"[{conn_id}] 温备连接启动成功（将延迟订阅心跳）")
            else:
                logger.error(f"[{conn_id}] 温备连接启动失败")
        
        logger.info(f"[{self.exchange}] 温备连接初始化完成: {len(self.warm_standby_connections)} 个")
    
    def _get_heartbeat_symbols(self):
        """获取温备心跳合约列表"""
        if self.exchange == "binance":
            return ["BTCUSDT"]
        elif self.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    async def _initialize_monitor_scheduler(self):
        """🚨 初始化监控连接 - 作为调度中心"""
        try:
            logger.info(f"【监控启动】开始初始化 {self.exchange} 监控调度器...")
            
            ws_url = self.config.get("ws_public_url")
            if not ws_url:
                logger.error(f"【监控启动】{self.exchange} 缺少ws_public_url配置")
                return
            
            conn_id = f"{self.exchange}_monitor"
            logger.info(f"【监控启动】连接ID: {conn_id}, URL: {ws_url}")
            
            # 🚨 为监控连接创建专用的回调函数
            def monitor_callback(data):
                """监控专用回调 - 只确认连接活跃"""
                try:
                    # 更新最后消息时间
                    if self.monitor_connection:
                        import time
                        self.monitor_connection.last_message_time = time.time()
                except Exception as e:
                    logger.error(f"[{conn_id}] 监控回调错误: {e}")
            
            logger.info(f"【监控启动】正在创建WebSocketConnection对象...")
            
            self.monitor_connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.MONITOR,
                data_callback=monitor_callback,  # 🚨 使用专用回调，不是self.data_callback
                symbols=[]  # 调度器不订阅数据
            )
            
            logger.info(f"【监控启动】正在连接WebSocket...")
            
            # 🚨 添加连接超时控制
            try:
                success = await asyncio.wait_for(
                    self.monitor_connection.connect(),
                    timeout=10.0  # 10秒超时
                )
            except asyncio.TimeoutError:
                logger.error(f"【监控启动】连接超时")
                success = False
            except Exception as conn_error:
                logger.error(f"【监控启动】连接异常: {conn_error}")
                success = False
            
            if success:
                logger.info(f"✅【监控启动】[{conn_id}] 监控连接成功")
                
                # 🚨 启动监控调度循环
                try:
                    logger.info(f"【监控启动】正在创建监控调度循环任务...")
                    self.monitor_scheduler_task = asyncio.create_task(
                        self._monitor_scheduling_loop(),
                        name=f"{conn_id}_scheduler"
                    )
                    
                    # 添加任务状态检查
                    await asyncio.sleep(0.1)  # 给任务一点时间启动
                    if not self.monitor_scheduler_task.done():
                        logger.info(f"✅【监控启动】[{conn_id}] 监控调度循环已启动")
                    else:
                        logger.error(f"❌【监控启动】监控调度任务立即结束了")
                        
                except Exception as task_error:
                    logger.error(f"❌【监控启动】创建调度任务失败: {task_error}")
                    import traceback
                    logger.error(traceback.format_exc())
            else:
                logger.error(f"❌【监控启动】[{conn_id}] 监控连接失败")
                
                # 🚨 记录重试
                logger.info(f"【监控启动】5秒后重试监控连接...")
                await asyncio.sleep(5)
                asyncio.create_task(self._retry_monitor_initialization())
                
        except Exception as e:
            logger.error(f"❌【监控启动】初始化监控调度器时发生异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _retry_monitor_initialization(self):
        """重试初始化监控连接"""
        conn_id = f"{self.exchange}_monitor"
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            retry_count += 1
            logger.warning(f"【监控重试】第{retry_count}次重试初始化 {self.exchange} 监控连接...")
            
            try:
                await self._initialize_monitor_scheduler()
                break
            except Exception as e:
                logger.error(f"【监控重试】第{retry_count}次重试失败: {e}")
                if retry_count < max_retries:
                    await asyncio.sleep(5 * retry_count)  # 指数退避
    
    async def _monitor_scheduling_loop(self):
        """🚨 监控调度循环 - 真正的权力中心"""
        logger.info(f"[监控调度] 开始监控调度循环，每3秒检查一次")
        
        while True:
            try:
                # 1. 监控所有主连接状态
                for i, master_conn in enumerate(self.master_connections):
                    if not master_conn.connected:
                        logger.warning(f"[监控调度] 主连接{i} ({master_conn.connection_id}) 断开")
                        
                        # 🚨 监控调度决策：是否启动故障转移
                        await self._monitor_handle_master_failure(i, master_conn)
                
                # 2. 监控所有温备连接状态
                for i, warm_conn in enumerate(self.warm_standby_connections):
                    if not warm_conn.connected:
                        logger.warning(f"[监控调度] 温备连接{i} ({warm_conn.connection_id}) 断开")
                        
                        # 🚨 监控调度决策：重连温备
                        await warm_conn.connect()
                        if warm_conn.connected:
                            logger.info(f"[监控调度] 温备连接{i} 重连成功")
                
                # 3. 定期报告状态到共享存储
                await self._report_status_to_data_store()
                
                await asyncio.sleep(3)  # 每3秒调度一次
                
            except Exception as e:
                logger.error(f"[监控调度] 调度循环错误: {e}")
                await asyncio.sleep(3)
    
    async def _select_best_standby_from_pool(self):
        """🚨 从共享池选择最佳温备"""
        available_standbys = [
            conn for conn in self.warm_standby_connections 
            if conn.connected and not conn.is_active
        ]
        
        if not available_standbys:
            logger.warning(f"[监控调度] 温备池中没有可用连接")
            return None
        
        # 🚨 选择策略：最健康（最近有消息、重连次数少、订阅数少）
        selected_standby = min(
            available_standbys,
            key=lambda conn: (
                conn.last_message_seconds_ago or 999,  # 消息越新越好
                conn.reconnect_count,                   # 重连次数越少越好
                len(conn.symbols)                       # 当前负担越轻越好
            )
        )
        
        logger.info(f"[监控调度] 从池中选择最佳温备: {selected_standby.connection_id}")
        return selected_standby
    
    async def _monitor_handle_master_failure(self, master_index: int, failed_master):
        """🚨 监控处理主连接故障 - 调度决策（共享池版）"""
        logger.info(f"[监控调度] 处理主连接{master_index}故障")
        
        # 🚨 【关键修改】从共享池选择最佳温备
        standby_conn = await self._select_best_standby_from_pool()
        
        if not standby_conn:
            logger.warning(f"[监控调度] 没有可用的温备连接，尝试重连主连接")
            await failed_master.connect()
            return
        
        # 🚨 监控决策：执行故障转移
        logger.info(f"[监控调度] 🚨 决策：执行故障转移")
        success = await self._monitor_execute_failover(master_index, failed_master, standby_conn)
        
        if not success:
            logger.warning(f"[监控调度] 故障转移失败，尝试重连原主连接")
            await failed_master.connect()
    
    async def _monitor_execute_failover(self, master_index: int, old_master, new_master):
        """🚨 监控执行故障转移 - 权力正式交接（共享池版）"""
        logger.info(f"[监控调度] 🚨 开始故障转移: {old_master.connection_id} -> {new_master.connection_id}")
        
        try:
            # 1. 🚨 监控命令：原主连接准备降级
            logger.info(f"[监控调度] 步骤1: 原主连接准备降级")
            if old_master.connected and old_master.subscribed:
                logger.info(f"[监控调度] 命令原主连接取消订阅")
                await old_master._unsubscribe()
            
            # 🚨 关键：清空原主连接的合约列表，防止重连后重复订阅
            old_master.symbols = []
            
            # 2. 🚨 监控命令：温备连接升级为主
            logger.info(f"[监控调度] 步骤2: 温备连接升级为主")
            
            # 获取该主连接应该负责的合约组
            master_symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else []
            
            success = await new_master.switch_role(ConnectionType.MASTER, master_symbols)
            if not success:
                logger.error("[监控调度] 温备切换角色失败")
                return False
            
            # 3. 🚨 监控更新：连接池权力结构（共享池逻辑）
            logger.info(f"[监控调度] 步骤3: 更新连接池权力结构（共享池）")
            
            # 🚨 【关键修改】不按索引交换，而是从池中移除新主，添加旧主到池
            if new_master in self.warm_standby_connections:
                self.warm_standby_connections.remove(new_master)
            
            # 更新主连接列表
            self.master_connections[master_index] = new_master
            
            # 4. 🚨 监控命令：原主连接重连为温备并加入共享池
            logger.info(f"[监控调度] 步骤4: 原主连接重连为温备并加入共享池")
            await old_master.disconnect()
            await asyncio.sleep(1)  # 等待断开完成
            
            if await old_master.connect():
                # 🚨 监控设置：只给心跳合约，不给主合约
                heartbeat_symbols = self._get_heartbeat_symbols()
                await old_master.switch_role(ConnectionType.WARM_STANDBY, heartbeat_symbols)
                
                # 🚨 将旧主加入温备共享池
                if old_master not in self.warm_standby_connections:
                    self.warm_standby_connections.append(old_master)
                
                logger.info(f"[监控调度] 原主连接已降级为温备，订阅心跳合约并加入共享池")
            
            # 5. 🚨 监控记录：故障转移完成
            logger.info(f"[监控调度] ✅ 故障转移完成（共享池模式）")
            logger.info(f"[监控调度] 📊 新主连接: {new_master.connection_id} (合约: {len(master_symbols)}个)")
            logger.info(f"[监控调度] 📊 温备池连接: {[conn.connection_id for conn in self.warm_standby_connections]}")
            
            # 6. 🚨 监控报告：更新状态到共享存储
            await self._report_failover_to_data_store(master_index, old_master.connection_id, new_master.connection_id)
            
            return True
            
        except Exception as e:
            logger.error(f"[监控调度] 故障转移执行失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def _report_status_to_data_store(self):
        """报告状态到共享存储"""
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "monitor": None,
                "pool_mode": "shared_pool"  # 🚨 新增：标记为共享池模式
            }
            
            # 报告主连接状态
            for conn in self.master_connections:
                status = await conn.check_health()
                status_report["masters"].append(status)
            
            # 报告温备连接状态
            for conn in self.warm_standby_connections:
                status = await conn.check_health()
                status_report["warm_standbys"].append(status)
            
            # 报告监控连接状态
            if self.monitor_connection:
                status = await self.monitor_connection.check_health()
                status_report["monitor"] = status
            
            await data_store.update_connection_status(
                self.exchange, 
                "websocket_pool", 
                status_report
            )
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 报告状态失败: {e}")
    
    async def _report_failover_to_data_store(self, master_index: int, old_master_id: str, new_master_id: str):
        """报告故障转移到共享存储"""
        try:
            failover_record = {
                "exchange": self.exchange,
                "master_index": master_index,
                "old_master": old_master_id,
                "new_master": new_master_id,
                "timestamp": datetime.now().isoformat(),
                "type": "failover",
                "pool_mode": "shared_pool"  # 🚨 新增：标记共享池模式
            }
            
            # 可以存储在专门的位置或添加到状态中
            await data_store.update_connection_status(
                self.exchange,
                "failover_history",
                failover_record
            )
            
            logger.info(f"[监控调度] 故障转移记录已保存")
            
        except Exception as e:
            logger.error(f"[监控调度] 保存故障转移记录失败: {e}")
    
    async def _health_check_loop(self):
        """健康检查循环 - 只检查，不行动"""
        while True:
            try:
                # 简单健康检查，只记录状态
                masters_connected = sum(1 for c in self.master_connections if c.connected)
                warm_connected = sum(1 for c in self.warm_standby_connections if c.connected)
                
                if masters_connected < len(self.master_connections):
                    logger.info(f"[健康检查] {self.exchange}: {masters_connected}/{len(self.master_connections)} 个主连接活跃")
                
                if warm_connected < len(self.warm_standby_connections):
                    logger.info(f"[健康检查] {self.exchange}: {warm_connected}/{len(self.warm_standby_connections)} 个温备连接活跃")
                
                await asyncio.sleep(30)  # 每30秒检查一次
                
            except Exception as e:
                logger.error(f"[健康检查] 错误: {e}")
                await asyncio.sleep(30)
    
    async def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        return await self._report_status_to_data_store()
    
    async def shutdown(self):
        """关闭连接池"""
        logger.info(f"[{self.exchange}] 正在关闭连接池...")
        
        # 取消任务
        if self.health_check_task:
            self.health_check_task.cancel()
        if self.monitor_scheduler_task:
            self.monitor_scheduler_task.cancel()
        
        # 断开所有连接
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
