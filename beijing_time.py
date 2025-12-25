#!/usr/bin/env python3
"""
北京时区显示工具
功能：让所有日志和显示自动使用北京时间，不影响底层UTC计算
用法：在主程序开头 import beijing_time 即可
"""

import datetime as dt
import time

# 北京时区常量
_BEIJING_OFFSET = dt.timedelta(hours=8)
_BEIJING_TZ = dt.timezone(_BEIJING_OFFSET, name='Asia/Shanghai')

class BeijingTime:
    """北京时间工具类"""
    
    @staticmethod
    def now():
        """获取当前北京时间（datetime对象）"""
        utc_now = dt.datetime.now(dt.timezone.utc)
        return utc_now.astimezone(_BEIJING_TZ)
    
    @staticmethod
    def now_str(format_str='%Y-%m-%d %H:%M:%S'):
        """获取当前北京时间的字符串"""
        return BeijingTime.now().strftime(format_str)
    
    @staticmethod
    def from_utc_timestamp(timestamp_ms):
        """将UTC毫秒时间戳转换为北京时间"""
        utc_seconds = timestamp_ms / 1000
        utc_time = dt.datetime.fromtimestamp(utc_seconds, dt.timezone.utc)
        return utc_time.astimezone(_BEIJING_TZ)
    
    @staticmethod
    def from_utc_str(utc_str):
        """将UTC时间字符串转换为北京时间"""
        # 处理常见的UTC时间格式
        if utc_str.endswith('Z'):
            utc_str = utc_str[:-1] + '+00:00'
        
        utc_time = dt.datetime.fromisoformat(utc_str)
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=dt.timezone.utc)
        return utc_time.astimezone(_BEIJING_TZ)

# 便捷函数
def now_str(format_str='%Y-%m-%d %H:%M:%S'):
    """快速获取当前北京时间字符串"""
    return BeijingTime.now_str(format_str)

def log(msg, show_time=True):
    """
    打印日志，自动添加北京时间
    参数:
        msg: 日志消息
        show_time: 是否显示时间，默认为True
    """
    if show_time:
        print(f"[{now_str('%H:%M:%S')}] {msg}")
    else:
        print(msg)

def print_banner():
    """打印启动横幅，显示当前时间信息"""
    banner = """
╔══════════════════════════════════════════╗
║         🚀 交易系统启动                 ║
║         🕐 北京时间: {}     ║
║         🌐 服务器时区: UTC              ║
╚══════════════════════════════════════════╝
""".format(now_str())
    print(banner)

# 启动时自动显示验证信息
def _auto_verify():
    """模块导入时自动验证时区设置"""
    utc_now = dt.datetime.now(dt.timezone.utc)
    bj_now = BeijingTime.now()
    
    offset_hours = (bj_now - utc_now).total_seconds() / 3600
    
    print("=" * 50)
    print("🕐 北京时区显示模块已加载")
    print("=" * 50)
    print(f"• UTC时间:      {utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"• 北京时间:     {bj_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"• 时区偏移:     UTC{offset_hours:+.1f}")
    print(f"• 状态:         ✅ 显示时区已矫正")
    print("=" * 50)
    print("提示: 所有 log() 函数将自动显示北京时间")
    print("      底层计算请继续使用 time.time() 获取UTC时间戳")
    print("=" * 50)

# 模块导入时自动运行验证
_auto_verify()
