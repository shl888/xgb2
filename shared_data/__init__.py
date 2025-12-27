"""
shared_data 顶级模块
功能：数据存储 + 智能流水线 + 5步过滤
"""

# 核心组件（必需）
from .data_store import data_store  # 全局数据存储实例
from .pipeline_manager import PipelineManager, PipelineConfig, DataType  # 管理员

# 5个步骤类（可选，用于单独测试或调试）
from .step1_filter import Step1Filter, ExtractedData
from .step2_fusion import Step2Fusion, FusedData
from .step3_align import Step3Align, AlignedData
from .step4_calc import Step4Calc, PlatformData
from .step5_cross_calc import Step5CrossCalc, CrossPlatformData

# 模块导出列表
__all__ = [
    # 核心实例
    'data_store',
    
    # 管理员（主要接口）
    'PipelineManager',
    'PipelineConfig',
    'DataType',
    
    # 5个步骤类（高级调试用）
    'Step1Filter',
    'Step2Fusion',
    'Step3Align',
    'Step4Calc',
    'Step5CrossCalc',
    
    # 数据模型（类型提示用）
    'ExtractedData',
    'FusedData',
    'AlignedData',
    'PlatformData',
    'CrossPlatformData',
]

# 版本信息
__version__ = "2.0.0"
__author__ = "你的套利神器"
__description__ = "智能数据处理流水线模块"

# 初始化日志
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# 模块加载完成日志
logger = logging.getLogger(__name__)
logger.info(f"✅ shared_data v{__version__} 模块加载完成")
logger.info(f"📦 导出: {len(__all__)} 个核心组件")
