"""
数据过滤器 -
负责将原始数据过滤成成品数据
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DataFilter:
    """数据过滤器：原始数据 → 成品数据"""
    
    def __init__(self):
        # 过滤器配置（从大脑代码中提取）
        self.config = {
            'min_funding_rate': 0.0003,  # 资金费率阈值（0.03%）
            'log_high_funding_rate': True,  # 是否记录高资金费率
        }
        logger.info("🔧 数据过滤器初始化完成")
    
    def filter_and_process(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        过滤并处理原始数据，生成成品数据
        返回：None表示数据被过滤掉，Dict表示成品数据
        """
        try:
            # 1. 基础验证
            if not self._validate_data(raw_data):
                return None
            
            # 2. 提取关键信息
            exchange = raw_data.get("exchange", "")
            symbol = raw_data.get("symbol", "")
            data_type = raw_data.get("data_type", "")
            
            # 3. 根据数据类型过滤
            if data_type == "funding_rate":
                return self._process_funding_rate(raw_data, exchange, symbol)
            elif data_type == "ticker":
                return self._process_ticker(raw_data, exchange, symbol)
            elif data_type == "mark_price":
                return self._process_mark_price(raw_data, exchange, symbol)
            
            # 4. 其他数据类型直接通过（不特殊处理）
            return self._create_basic_processed_data(raw_data, exchange, symbol)
            
        except Exception as e:
            logger.error(f"过滤处理数据失败: {e}")
            return None
    
    def _validate_data(self, data: Dict) -> bool:
        """验证数据完整性"""
        required_fields = ['exchange', 'symbol', 'data_type', 'timestamp']
        for field in required_fields:
            if field not in data:
                logger.debug(f"数据缺少必要字段 {field}: {data}")
                return False
        return True
    
    def _process_funding_rate(self, raw_data: Dict, exchange: str, symbol: str) -> Optional[Dict]:
        """处理资金费率数据"""
        funding_rate = raw_data.get("funding_rate")
        
        # 如果资金费率不存在，过滤掉
        if funding_rate is None:
            return None
        
        # 检查是否达到记录阈值
        if abs(funding_rate) > self.config['min_funding_rate']:
            if self.config['log_high_funding_rate']:
                logger.info(f"[资金费率监控] {exchange}:{symbol} = {funding_rate:.6f}")
        
        # 创建成品数据
        processed_data = {
            'type': 'funding_decision',
            'exchange': exchange,
            'symbol': symbol,
            'original_rate': funding_rate,
            'abs_rate': abs(funding_rate),
            'next_funding_time': raw_data.get('next_funding_time'),
            'timestamp': raw_data.get('timestamp'),
            'store_time': raw_data.get('store_timestamp', datetime.now().isoformat()),
            'data_type': 'funding_rate',
            'is_important': abs(funding_rate) > self.config['min_funding_rate']
        }
        
        return processed_data
    
    def _process_ticker(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """处理ticker数据"""
        processed_data = {
            'type': 'price_update',
            'exchange': exchange,
            'symbol': symbol,
            'price': raw_data.get('last'),
            'bid': raw_data.get('bid'),
            'ask': raw_data.get('ask'),
            'volume': raw_data.get('volume'),
            'change_percent': raw_data.get('change_percent'),
            'timestamp': raw_data.get('timestamp'),
            'store_time': raw_data.get('store_timestamp', datetime.now().isoformat()),
            'data_type': 'ticker'
        }
        
        return processed_data
    
    def _process_mark_price(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """处理标记价格数据"""
        processed_data = {
            'type': 'mark_price_update',
            'exchange': exchange,
            'symbol': symbol,
            'mark_price': raw_data.get('mark_price'),
            'timestamp': raw_data.get('timestamp'),
            'store_time': raw_data.get('store_timestamp', datetime.now().isoformat()),
            'data_type': 'mark_price'
        }
        
        return processed_data
    
    def _create_basic_processed_data(self, raw_data: Dict, exchange: str, symbol: str) -> Dict:
        """创建基本成品数据（对于未特殊处理的数据类型）"""
        return {
            'type': 'generic_data',
            'exchange': exchange,
            'symbol': symbol,
            'data_type': raw_data.get('data_type', 'unknown'),
            'timestamp': raw_data.get('timestamp'),
            'store_time': raw_data.get('store_timestamp', datetime.now().isoformat()),
            'raw_data': raw_data  # 包含原始数据
        }

# 全局过滤器实例
data_filter = DataFilter()
