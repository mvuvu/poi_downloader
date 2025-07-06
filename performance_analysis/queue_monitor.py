
import time
import json
from datetime import datetime
from pathlib import Path

# 监控输出目录
output_dir = Path("performance_analysis/queue_monitor")
output_dir.mkdir(parents=True, exist_ok=True)

print("🔍 开始监控任务队列...")
print("⚠️  注意：需要手动观察poi_crawler_simple.py的输出来收集以下数据：")
print("1. 每5分钟记录一次已处理任务数")
print("2. 观察重试任务的出现频率")
print("3. 记录Chrome重启的时间点")
print("4. 注意是否出现长时间无输出的情况")

# 创建数据记录模板
data_template = {
    "timestamp": "",
    "elapsed_minutes": 0,
    "total_processed": 0,
    "retry_count": 0,
    "chrome_restart_count": 0,
    "notes": ""
}

# 保存模板
with open(output_dir / "data_template.json", 'w') as f:
    json.dump(data_template, f, indent=2)

print(f"\n📝 请手动记录数据到: {output_dir / 'manual_records.json'}")
print("格式示例：")
print(json.dumps(data_template, indent=2))
