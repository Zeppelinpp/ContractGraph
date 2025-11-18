"""
运行所有高级分析场景

一键运行四个场景的分析：
1. FraudRank 欺诈风险传导分析
2. 高级循环交易检测
3. 空壳公司网络识别
4. 关联方串通网络分析
"""

import os
import sys
from datetime import datetime

# 添加父目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from analysis.fraud_rank import main as fraud_rank_main
from analysis.circular_trade import main as circular_trade_main
from analysis.shell_company import main as shell_company_main
from analysis.collusion import main as collusion_main


def main():
    start_time = datetime.now()
    
    print("\n" + "=" * 80)
    print("知识图谱高级分析套件")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    analyses = [
        ("FraudRank 欺诈风险传导分析", fraud_rank_main),
        ("高级循环交易检测", circular_trade_main),
        ("空壳公司网络识别", shell_company_main),
        ("关联方串通网络分析", collusion_main),
    ]
    
    results = {}
    
    for name, func in analyses:
        print(f"\n{'='*80}")
        print(f"正在执行: {name}")
        print('='*80)
        
        try:
            func()
            results[name] = "✓ 成功"
        except Exception as e:
            print(f"\n错误: {str(e)}")
            import traceback
            traceback.print_exc()
            results[name] = f"✗ 失败: {str(e)}"
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # 生成总结报告
    print("\n" + "=" * 80)
    print("分析完成总结")
    print("=" * 80)
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {duration:.2f} 秒")
    print("\n分析结果:")
    for name, status in results.items():
        print(f"  {status} - {name}")
    print("\n所有报告已保存至 reports/ 目录")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()

