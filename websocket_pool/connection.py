# websocket_pool/connection.py
"""
单个WebSocket连接实现 - 支持角色互换
支持自动重连、数据解析、状态管理
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable
import websockets
import aiohttp
import time

# 合约收集器
try:
    from .symbol_collector import add_symbol_from_websocket
    SYMBOL_COLLECTOR_AVAILABLE = True
except ImportError:
    logger = logging.getLogger(__name__)
    SYMBOL_COLLECTOR_AVAILABLE = False

logger = logging.getLogger(__name__)

class ConnectionType:
    MASTER = "master"
    WARM_STANDBY = "warm_standby"
    MONITOR = "monitor"

class WebSocketConnection:
    """单个WebSocket连接 - 支持主备切换"""
    
    def __init__(
        self,
        exchange: str,
        ws_url: str,
        connection_id: str,
        connection_type: str,
        data_callback: Callable,
        symbols: list = None
    ):
        self.exchange = exchange
        self.ws_url = ws_url
        self.connection_id = connection_id
        self.connection_type = connection_type
        self.original_type = connection_type
        self.data_callback = data_callback
        self.symbols = symbols or []
        
        self.ws = None
        self.connected = False
        self.last_message_time = None
        self.reconnect_count = 0
        self.subscribed = False
        self.is_active = False
        self.keepalive_task = None
        self.receive_task = None
        self.delayed_subscribe_task = None
        self.ticker_count = 0
        self.okx_ticker_count = 0
        self.ping_interval = 15
        self.reconnect_interval = 3
        self.last_subscribe_time = 0
        self.min_subscribe_interval = 2.0
    
    async def connect(self):
        """建立WebSocket连接 - 带超时保护"""
        try:
            logger.info(f"[{self.connection_id}] 正在连接 {self.ws_url}")
            
            self.ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_interval + 5,
                    close_timeout=1
                ),
                timeout=30
            )
            
            self.connected = True
            self.last_message_time = datetime.now()
            self.reconnect_count = 0
            
            logger.info(f"[{self.connection_id}] ✅ 连接成功")
            
            if self.connection_type == ConnectionType.MASTER and self.symbols:
                await self._subscribe()
                self.subscribed = True
                self.is_active = True
                logger.info(f"[{self.connection_id}] 主连接已激活并订阅 {len(self.symbols)} 个合约")
            
            elif self.connection_type == ConnectionType.WARM_STANDBY and self.symbols:
                delay_seconds = self._get_delay_for_warm_standby()
                self.delayed_subscribe_task = asyncio.create_task(
                    self._delayed_subscribe(delay_seconds)
                )
                logger.info(f"[{self.connection_id}] 温备连接将在 {delay_seconds} 秒后订阅心跳")
            
            elif self.connection_type == ConnectionType.MONITOR:
                logger.info(f"[{self.connection_id}] 监控连接已就绪（不订阅）")
            
            self.receive_task = asyncio.create_task(self._receive_messages())
            
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"[{self.connection_id}] ❌ 连接超时30秒")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"[{self.connection_id}] ❌ 连接失败: {e}")
            self.connected = False
            return False
    
    def _get_delay_for_warm_standby(self):
        """根据连接ID获取延迟时间，错开订阅"""
        try:
            parts = self.connection_id.split('_')
            if len(parts) >= 3:
                index = int(parts[-1])
                return 10 + (index * 5)
        except:
            pass
        return 10
    
    async def _delayed_subscribe(self, delay_seconds: int):
        """延迟订阅，避免触发交易所限制"""
        try:
            logger.info(f"[{self.connection_id}] ⏳ 等待 {delay_seconds} 秒后订阅...")
            await asyncio.sleep(delay_seconds)
            
            if self.connected and not self.subscribed and self.symbols:
                logger.info(f"[{self.connection_id}] 开始延迟订阅...")
                await self._subscribe()
                self.subscribed = True
                logger.info(f"[{self.connection_id}] 延迟订阅完成")
            elif not self.connected:
                logger.warning(f"[{self.connection_id}] 连接已断开，取消延迟订阅")
                
        except Exception as e:
            logger.error(f"[{self.connection_id}] 延迟订阅失败: {e}")
    
    async def switch_role(self, new_role: str, new_symbols: list = None):
        """切换连接角色"""
        try:
            old_role = self.connection_type
            
            if new_role == ConnectionType.MASTER and old_role == ConnectionType.WARM_STANDBY:
                logger.info(f"[{self.connection_id}] 从温备切换为主连接")
                
                if self.delayed_subscribe_task:
                    self.delayed_subscribe_task.cancel()
                
                if self.connected and self.subscribed:
                    await self._unsubscribe()
                    self.subscribed = False
                
                if new_symbols:
                    self.symbols = new_symbols
                
                self.is_active = True
                self.connection_type = new_role
                
                if self.connected and self.symbols:
                    await self._subscribe()
                    self.subscribed = True
                
                logger.info(f"[{self.connection_id}] 切换完成，订阅 {len(self.symbols)} 个合约")
                return True
                
            elif new_role == ConnectionType.WARM_STANDBY and old_role == ConnectionType.MASTER:
                logger.info(f"[{self.connection_id}] 从主连接切换为温备")
                
                if self.connected and self.subscribed:
                    await self._unsubscribe()
                    self.subscribed = False
                
                if new_symbols:
                    self.symbols = new_symbols
                else:
                    if self.exchange == "binance":
                        self.symbols = ["BTCUSDT"]
                    elif self.exchange == "okx":
                        self.symbols = ["BTC-USDT-SWAP"]
                
                self.is_active = False
                self.connection_type = new_role
                
                if self.connected and self.symbols:
                    await self._subscribe()
                    self.subscribed = True
                
                logger.info(f"[{self.connection_id}] 切换完成，订阅 {len(self.symbols)} 个心跳合约")
                return True
            
            else:
                self.connection_type = new_role
                logger.info(f"[{self.connection_id}] 角色从 {old_role} 改为 {new_role}")
                return True
                
        except Exception as e:
            logger.error(f"[{self.connection_id}] 角色切换失败: {e}")
            return False
    
    async def _subscribe(self):
        """订阅数据"""
        if not self.symbols:
            logger.warning(f"[{self.connection_id}] 没有合约可订阅")
            return
        
        logger.info(f"[{self.connection_id}] 开始订阅 {len(self.symbols)} 个合约")
        
        if self.exchange == "binance":
            await self._subscribe_binance()
        elif self.exchange == "okx":
            await self._subscribe_okx()
    
    async def _subscribe_binance(self):
        """订阅币安数据"""
        try:
            streams = []
            for symbol in self.symbols:
                symbol_lower = symbol.lower()
                streams.append(f"{symbol_lower}@ticker")
                streams.append(f"{symbol_lower}@markPrice")
            
            logger.info(f"[{self.connection_id}] 准备订阅 {len(streams)} 个streams")
            
            batch_size = 50
            for i in range(0, len(streams), batch_size):
                batch = streams[i:i+batch_size]
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": batch,
                    "id": i // batch_size + 1
                }
                
                await self.ws.send(json.dumps(subscribe_msg))
                logger.info(f"[{self.connection_id}] 发送订阅批次 {i//batch_size+1}/{(len(streams)+batch_size-1)//batch_size}")
                
                if i + batch_size < len(streams):
                    await asyncio.sleep(1.5)
            
            self.subscribed = True
            logger.info(f"[{self.connection_id}] ✅ 订阅完成，共 {len(self.symbols)} 个合约")
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 订阅失败: {e}")
    
    async def _subscribe_okx(self):
        """订阅OKX数据"""
        try:
            logger.info(f"[{self.connection_id}] 开始订阅OKX数据，共 {len(self.symbols)} 个合约")
            
            if self.symbols and not self.symbols[0].endswith('-SWAP'):
                logger.warning(f"[{self.connection_id}] OKX合约格式可能错误")
            
            all_subscriptions = []
            for symbol in self.symbols:
                all_subscriptions.append({"channel": "tickers", "instId": symbol})
                all_subscriptions.append({"channel": "funding-rate", "instId": symbol})
            
            logger.info(f"[{self.connection_id}] 准备订阅 {len(all_subscriptions)} 个频道 (包含资金费率)")
            
            batch_size = 50
            total_batches = (len(all_subscriptions) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(all_subscriptions))
                batch_args = all_subscriptions[start_idx:end_idx]
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": batch_args
                }
                
                await self.ws.send(json.dumps(subscribe_msg))
                logger.info(f"[{self.connection_id}] 发送批次 {batch_idx+1}/{total_batches} (包含资金费率)")
                
                if batch_idx < total_batches - 1:
                    await asyncio.sleep(1.5)
            
            self.subscribed = True
            logger.info(f"[{self.connection_id}] ✅ OKX订阅完成")
            return True
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 订阅失败: {e}")
            return False
    
    async def _unsubscribe(self):
        """取消订阅"""
        try:
            if not self.symbols:
                return
                
            if self.exchange == "binance":
                streams = []
                for symbol in self.symbols:
                    symbol_lower = symbol.lower()
                    streams.append(f"{symbol_lower}@ticker")
                    streams.append(f"{symbol_lower}@markPrice")
                
                for i in range(0, len(streams), 50):
                    batch = streams[i:i+50]
                    unsubscribe_msg = {
                        "method": "UNSUBSCRIBE",
                        "params": batch,
                        "id": 1
                    }
                    await self.ws.send(json.dumps(unsubscribe_msg))
                    await asyncio.sleep(1)
                
            elif self.exchange == "okx":
                for i in range(0, len(self.symbols), 10):
                    batch = self.symbols[i:i+10]
                    args = [{"channel": "tickers", "instId": symbol} for symbol in batch]
                    
                    unsubscribe_msg = {
                        "op": "unsubscribe",
                        "args": args
                    }
                    await self.ws.send(json.dumps(unsubscribe_msg))
                    await asyncio.sleep(2)
            
            logger.info(f"[{self.connection_id}] 取消订阅 {len(self.symbols)} 个合约")
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 取消订阅失败: {e}")
    
    async def _receive_messages(self):
        """接收消息"""
        try:
            async for message in self.ws:
                self.last_message_time = datetime.now()
                await self._process_message(message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"[{self.connection_id}] 连接关闭")
            self.connected = False
            self.subscribed = False
            self.is_active = False
        except Exception as e:
            logger.error(f"[{self.connection_id}] 接收消息错误: {e}")
            self.connected = False
            self.subscribed = False
            self.is_active = False
    
    async def _process_message(self, message):
        """处理接收到的消息"""
        try:
            data = json.loads(message)
            
            if self.exchange == "binance" and "id" in data:
                logger.info(f"[{self.connection_id}] 收到订阅响应 ID={data.get('id')}")
            
            if self.exchange == "binance":
                await self._process_binance_message(data)
            elif self.exchange == "okx":
                await self._process_okx_message(data)
                
        except json.JSONDecodeError:
            logger.warning(f"[{self.connection_id}] 无法解析JSON消息")
        except Exception as e:
            logger.error(f"[{self.connection_id}] 处理消息错误: {e}")
    
    async def _process_binance_message(self, data):
        """处理币安消息"""
        if "result" in data or "id" in data:
            return
        
        event_type = data.get("e", "")
        
        if event_type == "24hrTicker":
            symbol = data.get("s", "").upper()
            if not symbol:
                return
            
            self.ticker_count += 1
            
            if self.ticker_count % 100 == 0:
                logger.info(f"[{self.connection_id}] 已处理 {self.ticker_count} 个ticker消息")
            
            processed = {
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "ticker",
                "price_change_percent": float(data.get("P", 0)),
                "last_price": float(data.get("c", 0)),
                "volume": float(data.get("v", 0)),
                "quote_volume": float(data.get("q", 0)),
                "high_price": float(data.get("h", 0)),
                "low_price": float(data.get("l", 0)),
                "event_time": data.get("E", 0),
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                await self.data_callback(processed)
            except Exception as e:
                logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
        
        elif event_type == "markPriceUpdate":
            symbol = data.get("s", "").upper()
            
            # 收集币安合约名
            if SYMBOL_COLLECTOR_AVAILABLE:
                try:
                    add_symbol_from_websocket("binance", symbol)
                except Exception as e:
                    logger.debug(f"收集币安合约失败 {symbol}: {e}")
            
            processed = {
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "mark_price",
                "mark_price": float(data.get("p", 0)),
                "funding_rate": float(data.get("r", 0)),
                "next_funding_time": data.get("T", 0),
                "event_time": data.get("E", 0),
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                await self.data_callback(processed)
            except Exception as e:
                logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
    
    async def _process_okx_message(self, data):
        """处理OKX消息"""
        if data.get("event"):
            event_type = data.get("event")
            if event_type == "error":
                logger.error(f"[{self.connection_id}] OKX错误: {data}")
            elif event_type == "subscribe":
                logger.info(f"[{self.connection_id}] OKX订阅成功: {data.get('arg', {})}")
            return
        
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        symbol = arg.get("instId", "")
        
        try:
            if channel == "funding-rate":
                if data.get("data") and len(data["data"]) > 0:
                    funding_data = data["data"][0]
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    
                    # 收集OKX合约名
                    if SYMBOL_COLLECTOR_AVAILABLE:
                        try:
                            add_symbol_from_websocket("okx", processed_symbol)
                        except Exception as e:
                            logger.debug(f"收集OKX合约失败 {processed_symbol}: {e}")
                    
                    funding_rate = float(funding_data.get("fundingRate", 0))
                    logger.info(f"[{self.connection_id}] 收到资金费率: {processed_symbol}={funding_rate:.6f}")
                    
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "funding_rate",
                        "funding_rate": funding_rate,
                        "next_funding_time": funding_data.get("fundingTime", ""),
                        "mark_price": float(funding_data.get("markPx", 0)),
                        "timestamp": datetime.now().isoformat(),
                        "original_symbol": symbol
                    }
                    try:
                        await self.data_callback(processed)
                    except Exception as e:
                        logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
                    
            elif channel == "tickers":
                if data.get("data") and len(data["data"]) > 0:
                    ticker_data = data["data"][0]
                    
                    self.okx_ticker_count += 1
                    
                    if self.okx_ticker_count % 50 == 0:
                        logger.info(f"[{self.connection_id}] 已处理 {self.okx_ticker_count} 个OKX ticker")
                    
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "ticker",
                        "price_change_percent": float(ticker_data.get("sodUtc8", 0)),
                        "last_price": float(ticker_data.get("last", 0)),
                        "volume": float(ticker_data.get("volCcy24h", 0)),
                        "quote_volume": float(ticker_data.get("vol24h", 0)),
                        "high_price": float(ticker_data.get("high24h", 0)),
                        "low_price": float(ticker_data.get("low24h", 0)),
                        "timestamp": ticker_data.get("ts", ""),
                        "processed_time": datetime.now().isoformat(),
                        "original_symbol": symbol
                    }
                    try:
                        await self.data_callback(processed)
                    except Exception as e:
                        logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
                    
        except Exception as e:
            logger.warning(f"[{self.connection_id}] 解析OKX数据失败: {e}")
    
    async def disconnect(self):
        """断开连接"""
        try:
            if self.delayed_subscribe_task:
                self.delayed_subscribe_task.cancel()
            
            if self.ws and self.connected:
                await self.ws.close()
                self.connected = False
                
            if self.receive_task:
                self.receive_task.cancel()
                
            self.subscribed = False
            self.is_active = False
            
            logger.info(f"[{self.connection_id}] 连接已完全断开")
            
        except Exception as e:
            # 🚨 修复SyntaxError：确保字符串正确闭合
            logger.error(f"[{self.connection_id}] 断开连接时发生错误: {e}")
    
    async def check_health(self) -> Dict[str, Any]:
        """检查连接健康状态"""
        now = datetime.now()
        last_msg_seconds = (now - self.last_message_time).total_seconds() if self.last_message_time else 999
        
        return {
            "connection_id": self.connection_id,
            "exchange": self.exchange,
            "type": self.connection_type,
            "connected": self.connected,
            "subscribed": self.subscribed,
            "is_active": self.is_active,
            "symbols_count": len(self.symbols),
            "last_message_seconds_ago": last_msg_seconds,
            "reconnect_count": self.reconnect_count,
            "timestamp": now.isoformat()
        }
