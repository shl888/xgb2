# shared_data/__init__.py
"""
共享数据处理模块
"""

from .data_store import data_store
from .pipeline_manager import PipelineManager
from .pipeline_starter import PipelineStarter  # ✅ 新增这行
from .step1_filter import Step1Filter
from .step2_fusion import Step2Fusion
from .step3_align import Step3Align
from .step4_single_calc import Step4SingleCalc
from .step5_cross_calc import Step5CrossCalc

__all__ = [
    'data_store',
    'PipelineManager',
    'PipelineStarter',  # ✅ 新增这行
    'Step1Filter',
    'Step2Fusion', 
    'Step3Align',
    'Step4SingleCalc',
    'Step5CrossCalc'
]
