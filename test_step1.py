"""
Step1过滤测试 - 真实数据版
功能：拉取真实API数据，测试第一步数据提取功能
运行：python test_step1.py
"""

import sys
sys.path.append("./shared_data")

import requests
import logging
from typing import Dict, List, Any
from collections import defaultdict

from step1_filter import Step1Filter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealDataFetcher:
    """真实数据获取器"""
    
    def __init__(self):
        self.websocket_api = "https://xgb2.onrender.com/api/debug/all_websocket_data"
        self.history_api = "https://xgb2.onrender.com/api/funding/settlement/public"
    
    def fetch_all(self) -> List[Dict[str, Any]]:
        """获取所有原始数据（合并实时+历史）"""
        try:
            # 获取WebSocket实时数据
            logger.info("正在获取WebSocket实时数据...")
            response = requests.get(f"{self.websocket_api}?show_all=true", timeout=10)
            response.raise_for_status()
            
            data = response.json()
            raw_items = []
            for exchange, symbols in data.get("data", {}).items():
                for symbol, data_types in symbols.items():
                    for data_type, payload in data_types.items():
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        raw_items.append({
                            "exchange": exchange,
                            "symbol": symbol,
                            "data_type": data_type,
                            "raw_data": payload.get("raw_data", {}),
                            "timestamp": payload.get("timestamp"),
                            "source": payload.get("source", "websocket")
                        })
            
            logger.info(f"✅ 获取到 {len(raw_items)} 条实时数据")
            
            # 获取币安历史费率数据
            logger.info("正在获取币安历史费率数据...")
            response = requests.get(self.history_api, timeout=10)
            response.raise_for_status()
            
            history_data = response.json().get("data", [])
            logger.info(f"✅ 获取到 {len(history_data)} 条历史费率数据")
            
            return raw_items + history_data
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return []

def main():
    print("=" * 90)
    print("Step1过滤测试 - 真实数据版")
    print("=" * 90 + "\n")
    
    # 1. 获取真实原始数据
    print("1. 获取真实原始数据...")
    fetcher = RealDataFetcher()
    raw_data = fetcher.fetch_all()
    if not raw_data:
        logger.error("❌ 没有获取到数据，测试终止")
        return
    print(f"   原始数据: {len(raw_data)} 条\n")
    
    # 2. 运行Step1过滤
    print("2. 运行Step1过滤（提取5种数据源）...")
    step1 = Step1Filter()
    step1_results = step1.process(raw_data)
    print(f"   Step1输出: {len(step1_results)} 条提取数据\n")
    
    # 3. 验证统计
    print("3. 验证统计:")
    print(f"   统计结果: {dict(step1.stats)}\n")
    
    # 4. 打印前10条详细信息
    print("4. 提取数据详情（前10条）:\n")
    
    for i, item in enumerate(step1_results[:10], 1):
        print(f"【{i}】 {item.data_type}")
        print(f"     交易所: {item.exchange}")
        print(f"     合约: {item.symbol}")
        print(f"     提取字段: {list(item.payload.keys())}")
        print(f"     合约名: {item.payload.get('contract_name', 'N/A')}")
        print(f"     价格/费率: {item.payload.get('latest_price') or item.payload.get('funding_rate', 'N/A')}")
        print("━" * 90)
    
    # 5. 深度验证
    print("\n" + "=" * 90)
    print("🧪 深度验证")
    print("=" * 90 + "\n")
    
    # 按数据类型分组
    grouped = defaultdict(list)
    for item in step1_results:
        grouped[item.data_type].append(item)
    
    # 验证5种数据源都存在
    expected_types = [
        "okx_ticker",
        "okx_funding_rate",
        "binance_ticker",
        "binance_mark_price",
        "binance_funding_settlement"
    ]
    
    print("数据类型验证:")
    for dtype in expected_types:
        count = len(grouped[dtype])
        print(f"   {dtype}: {count} 条")
        if count == 0:
            print(f"   ⚠️  警告: {dtype} 没有数据！")
    
    # 验证字段完整性
    print("\n字段完整性验证:")
    all_valid = True
    for item in step1_results:
        if not item.exchange or not item.symbol:
            print(f"   ❌ 数据缺少exchange或symbol: {item}")
            all_valid = False
        if not item.payload:
            print(f"   ❌ 数据payload为空: {item}")
            all_valid = False
    
    if all_valid:
        print("   ✅ 所有数据完整性验证通过")
    
    # 6. 最终结论
    print("\n" + "=" * 90)
    total = len(step1_results)
    if total >= 2000 and all(len(grouped[t]) > 0 for t in expected_types):
        print("🎉 **恭喜！Step1过滤功能100%正常！**")
        print(f"✅ 成功提取 {total} 条有效数据")
        print("✅ 5种数据源全部识别")
    elif total > 0:
        print("✅ Step1过滤功能基本正常")
        print(f"✅ 成功提取 {total} 条有效数据")
    else:
        print("❌ Step1过滤失败，未提取到任何数据")
    print("=" * 90)

if __name__ == "__main__":
    main()
