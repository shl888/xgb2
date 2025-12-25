#!/usr/bin/env python3
"""
资金费率结算页面模板
与欢迎页面保持一致的紫色渐变风格
"""
import datetime
from typing import Any


def get_html_page(manager: Any) -> str:
    """
    生成资金费率结算管理页面HTML
    :param manager: FundingSettlementManager实例
    """
    
    # 获取状态
    status = manager.get_status()
    
    # 从data_store获取合约数据
    from shared_data.data_store import data_store
    
    contracts = data_store.funding_settlement.get('binance', {})
    
    # 生成合约表格HTML
    contracts_html = ""
    for symbol, data in sorted(contracts.items()):
        funding_rate = data.get('funding_rate', 0)
        funding_time = data.get('funding_time', 0)
        
        # 计算数据年龄
        if funding_time:
            age_seconds = (datetime.datetime.now().timestamp() * 1000 - funding_time) / 1000
            age_str = f"{int(age_seconds)}秒" if age_seconds < 3600 else f"{int(age_seconds / 3600)}小时"
        else:
            age_str = "未知"
        
        # 格式化费率
        rate_color = "#28a745" if funding_rate >= 0 else "#dc3545"
        rate_str = f"{funding_rate:.6f}"
        
        contracts_html += f"""
        <tr>
            <td>{symbol}</td>
            <td style="color: {rate_color}; font-weight: 600;">{rate_str}</td>
            <td>{datetime.datetime.fromtimestamp(funding_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if funding_time else 'N/A'}</td>
            <td>{age_str}</td>
        </tr>
        """
    
    # 如果没有数据
    if not contracts_html:
        contracts_html = """
        <tr>
            <td colspan="4" style="text-align: center; padding: 40px; color: #666;">
                <div style="font-size: 48px; margin-bottom: 10px;">📊</div>
                <div>暂无数据</div>
                <div style="font-size: 14px; margin-top: 10px;">请点击"获取数据"按钮</div>
            </td>
        </tr>
        """
    
    # 状态卡片
    last_fetch = status.get('last_fetch_time', '从未')
    is_fetched = status.get('is_auto_fetched', False)
    manual_count = status.get('manual_fetch_count', '0/3')
    weight_info = status.get('api_weight_per_request', 10)
    
    status_badge = "✅ 已获取" if is_fetched else "⏳ 未获取"
    status_color = "#4CAF50" if is_fetched else "#ff9800"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>资金费率结算管理 | Brain Core Trading</title>
        <meta charset="utf-8">
        <style>
            * {{margin: 0; padding: 0; box-sizing: border-box; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;}}
            body {{background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px;}}
            .container {{max-width: 1200px; margin: 0 auto; background: rgba(255, 255, 255, 0.95); border-radius: 20px; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); overflow: hidden;}}
            .header {{background: linear-gradient(to right, #667eea, #764ba2); color: white; padding: 30px; text-align: center;}}
            .header h1 {{font-size: 32px; margin-bottom: 10px;}}
            .status-grid {{display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; padding: 30px; background: #f8f9fa;}}
            .status-card {{background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); border-left: 4px solid #667eea;}}
            .status-card h3 {{color: #667eea; font-size: 14px; margin-bottom: 10px;}}
            .status-card .value {{font-size: 28px; font-weight: 700; color: #333;}}
            .action-section {{padding: 30px; text-align: center; border-bottom: 1px solid #eaeaea;}}
            .fetch-button {{background: linear-gradient(to right, #4CAF50, #8BC34A); color: white; border: none; padding: 15px 40px; font-size: 16px; font-weight: 600; border-radius: 50px; cursor: pointer;}}
            .fetch-button:hover {{transform: translateY(-2px);}}
            table {{width: 100%; border-collapse: collapse; background: white;}}
            th {{background: #667eea; color: white; padding: 15px; text-align: left;}}
            td {{padding: 12px 15px; border-bottom: 1px solid #eaeaea;}}
            .rate-positive {{color: #28a745; font-weight: 600;}}
            .rate-negative {{color: #dc3545; font-weight: 600;}}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 资金费率结算管理</h1>
            </div>
            
            <div class="status-grid">
                <div class="status-card">
                    <h3>数据状态</h3>
                    <div class="value" style="color: {status_color};">{status_badge}</div>
                    <div>上次获取: {last_fetch}</div>
                </div>
                <div class="status-card">
                    <h3>USDT合约</h3>
                    <div class="value">{len(contracts)}</div>
                    <div>永续合约数量</div>
                </div>
                <div class="status-card">
                    <h3>手动触发</h3>
                    <div class="value">{manual_count}</div>
                    <div>每小时限制 3次</div>
                </div>
                <div class="status-card">
                    <h3>API权重</h3>
                    <div class="value">{weight_info}</div>
                    <div>每次请求消耗</div>
                </div>
            </div>
            
            <div style="padding: 30px; text-align: center;">
                <button class="fetch-button" onclick="fetchData()">🔄 获取数据</button>
            </div>
            
            <div style="padding: 30px;">
                <h2>📈 资金费率数据</h2>
                <table>
                    <thead>
                        <tr>
                            <th>合约</th>
                            <th>结算费率</th>
                            <th>结算时间</th>
                            <th>数据年龄</th>
                        </tr>
                    </thead>
                    <tbody>
                        {contracts_html}
                    </tbody>
                </table>
            </div>
        </div>
        
        <script>
            async function fetchData() {{
                const password = prompt('请输入访问密码:');
                if (!password) return;
                
                const button = document.querySelector('.fetch-button');
                button.disabled = true;
                button.textContent = '获取中...';
                
                try {{
                    const response = await fetch('/api/funding/settlement/fetch', {{
                        method: 'POST',
                        headers: {{'X-Access-Password': password}}
                    }});
                    
                    const result = await response.json();
                    
                    if (result.success) {{
                        location.reload();
                    }} else {{
                        alert('失败: ' + result.error);
                    }}
                }} catch (e) {{
                    alert('错误: ' + e.message);
                }} finally {{
                    button.disabled = false;
                    button.textContent = '🔄 获取数据';
                }}
            }}
        </script>
    </body>
    </html>
    """
    
    return html_content
