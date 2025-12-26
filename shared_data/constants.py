# shared_data/constants.py
"""
常量定义 - 精简版（避免循环导入）
"""

# 数据类型 - 直接定义，不导入其他模块
DATA_TYPE_TICKER = "ticker"
DATA_TYPE_FUNDING_RATE = "funding_rate"
DATA_TYPE_MARK_PRICE = "mark_price"
DATA_TYPE_HISTORICAL_FUNDING = "historical_funding"
DATA_TYPE_TRADE = "trade"
DATA_TYPE_ACCOUNT = "account"
DATA_TYPE_ORDER = "order"

# 交易所
EXCHANGE_BINANCE = "binance"
EXCHANGE_OKX = "okx"

# 处理步骤状态
STEP_STATUS_PENDING = "pending"
STEP_STATUS_PROCESSING = "processing"
STEP_STATUS_COMPLETED = "completed"
STEP_STATUS_FAILED = "failed"

# 特殊通道标识
CHANNEL_NORMAL = "normal"
CHANNEL_SPECIAL = "special"
