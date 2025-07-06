
import re
from collections import Counter
from pathlib import Path
import json

def analyze_retry_patterns(log_file):
    """分析重试任务模式"""
    
    retry_addresses = []
    normal_addresses = []
    retry_pattern = r'🔄 非建筑物，使用日文地址重试: (.+)'
    success_pattern = r'✅ (.+) \| POI: (\d+) \| 状态: 已保存'
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            # 收集重试地址
            retry_match = re.search(retry_pattern, line)
            if retry_match:
                retry_addresses.append(retry_match.group(1))
            
            # 收集成功地址
            success_match = re.search(success_pattern, line)
            if success_match:
                normal_addresses.append(success_match.group(1))
    
    # 分析
    total_tasks = len(normal_addresses) + len(retry_addresses)
    retry_rate = len(retry_addresses) / total_tasks * 100 if total_tasks > 0 else 0
    
    print(f"\n📊 重试任务分析:")
    print(f"  总任务数: {total_tasks}")
    print(f"  重试任务数: {len(retry_addresses)}")
    print(f"  重试率: {retry_rate:.1f}%")
    
    # 分析重试地址特征
    if retry_addresses:
        print(f"\n🔍 重试地址示例:")
        for addr in retry_addresses[:5]:
            print(f"  - {addr}")
    
    # 保存分析结果
    results = {
        'total_tasks': total_tasks,
        'retry_count': len(retry_addresses),
        'retry_rate': retry_rate,
        'retry_addresses_sample': retry_addresses[:20]
    }
    
    with open('performance_analysis/retry_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    return results

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        analyze_retry_patterns(sys.argv[1])
    else:
        print("使用方法: python retry_analyzer.py crawler_output.log")
