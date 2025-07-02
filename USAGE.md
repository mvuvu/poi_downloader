# POI爬虫使用指南 📖

## 🚀 快速开始

### 1. 初始化项目

```bash
# 首次使用，运行初始化脚本
python init.py
```

这会自动：
- 安装所需依赖包
- 创建项目目录结构
- 生成配置文件模板
- 检查WebDriver环境

### 2. 准备输入数据

将包含地址的CSV文件放入 `data/input/` 目录：

```csv
District,Latitude,Longitude,Address
千代田区,35.6862245,139.7347045,東京都千代田区鍛冶町1丁目7-1
千代田区,35.6903667,139.7712745,東京都千代田区二番町10-46
```

**重要**: Address列必须包含完整的日本地址。

### 3. 运行爬虫

```bash
# 使用默认配置
python final_crawler.py

# 或者在代码中指定输入文件
python -c "
import pandas as pd
from final_crawler import FinalPOICrawler

df = pd.read_csv('data/input/your_file.csv')
addresses = df['Address'].tolist()

config = {
    'max_workers': 2,
    'output_file': 'data/output/results.csv'
}

crawler = FinalPOICrawler(config)
crawler.process_addresses(addresses)
crawler.close()
"
```

## ⚙️ 配置管理

### 创建预设配置

```bash
python config.py create-presets
```

这会创建4种预设配置：

| 配置 | 用途 | 特点 |
|------|------|------|
| `default` | 日常使用 | 平衡性能和稳定性 |
| `fast` | 快速爬取 | 高并发，无头模式 |
| `stable` | 稳定爬取 | 单线程，高重试 |
| `debug` | 调试模式 | 详细日志，截图 |

### 使用不同配置

```python
from config import ConfigManager

# 加载配置
manager = ConfigManager()
config = manager.load_config('fast')  # 使用快速模式

# 创建爬虫
crawler = FinalPOICrawler(config.__dict__)
```

### 自定义配置

编辑 `config/default.json`:

```json
{
  "crawler": {
    "max_workers": 3,          // 并发线程数 (1-4推荐)
    "driver_pool_size": 3,     // WebDriver池大小
    "batch_size": 20,          // 批量保存数据量
    "timeout": 15,             // 页面加载超时(秒)
    "retry_times": 3,          // 重试次数
    "headless": false,         // 无头模式 (true/false)
    "checkpoint_interval": 50, // 检查点保存间隔
    "enable_images": false,    // 加载图片 (false更快)
    "debug_mode": false        // 调试模式
  }
}
```

## 📊 监控和调试

### 实时进度监控

运行时会显示详细进度：

```
📊 进度报告:
  处理: 25/100
  成功: 21 (84.0%)
  失败: 4
  平均: 3.3s/个
  预计剩余: 4.2分钟
```

### 检查点恢复

爬虫支持断点续爬：

```bash
# 中断后重新运行，自动从断点继续
python final_crawler.py
```

检查点信息保存在 `checkpoint.json`：

```json
{
  "processed_addresses": ["地址1", "地址2"],
  "processed_count": 25,
  "success_count": 21,
  "failed_addresses": ["失败地址1"],
  "timestamp": "2025-07-02T15:30:00"
}
```

### 查看日志

```bash
# 查看详细日志
tail -f logs/crawler.log

# 查看错误日志
grep "ERROR" logs/crawler.log
```

## 🎨 高级用法

### Jupyter Notebook交互式使用

```python
# 在Jupyter中运行
import pandas as pd
from final_crawler import FinalPOICrawler

# 小批量测试
addresses = ["東京都千代田区鍛冶町1丁目7-1"]
config = {'max_workers': 1, 'output_file': 'test.csv'}

crawler = FinalPOICrawler(config)
crawler.process_addresses(addresses)
crawler.close()

# 查看结果
results = pd.read_csv('test.csv')
print(results.head())
```

### 批量处理多个文件

```python
import glob
from pathlib import Path

# 处理data/input/目录下所有CSV文件
input_dir = Path('data/input')
output_dir = Path('data/output')

for csv_file in input_dir.glob('*.csv'):
    print(f"处理文件: {csv_file}")
    
    df = pd.read_csv(csv_file)
    addresses = df['Address'].tolist()
    
    output_file = output_dir / f"poi_{csv_file.stem}.csv"
    config = {'output_file': str(output_file)}
    
    crawler = FinalPOICrawler(config)
    crawler.process_addresses(addresses)
    crawler.close()
```

### 数据预处理

```python
# 地址数据清理
def clean_addresses(df):
    # 移除空值
    df = df.dropna(subset=['Address'])
    
    # 标准化地址格式
    df['Address'] = df['Address'].str.strip()
    
    # 移除重复地址
    df = df.drop_duplicates(subset=['Address'])
    
    return df

df = pd.read_csv('input.csv')
df_clean = clean_addresses(df)
df_clean.to_csv('cleaned_input.csv', index=False)
```

## 🔧 性能优化

### 1. 硬件优化

- **CPU**: 多核心有助于并发处理
- **内存**: 建议8GB以上，特别是大批量数据
- **网络**: 稳定的宽带连接

### 2. 参数调优

**高性能配置** (适合服务器):
```json
{
  "max_workers": 4,
  "driver_pool_size": 4,
  "batch_size": 10,
  "headless": true,
  "enable_images": false
}
```

**稳定配置** (适合网络不稳定):
```json
{
  "max_workers": 1,
  "retry_times": 5,
  "timeout": 30,
  "checkpoint_interval": 25
}
```

### 3. 系统优化

```bash
# Linux系统优化
# 增加文件描述符限制
ulimit -n 4096

# 优化TCP连接
echo 'net.ipv4.tcp_tw_reuse = 1' >> /etc/sysctl.conf
sysctl -p
```

## ❓ 常见问题

### Q: 爬取速度很慢怎么办？

A: 
1. 启用无头模式: `"headless": true`
2. 禁用图片加载: `"enable_images": false`
3. 增加并发数: `"max_workers": 3`
4. 减少超时时间: `"timeout": 10`

### Q: 经常出现"建造物"识别错误？

A: 这是Google Maps页面结构变化导致的，新版本已修复：
- 使用多策略元素定位
- 支持"建造物"/"建筑物"/"建築物"等多种表述

### Q: 如何处理反爬虫检测？

A:
1. 降低并发数: `"max_workers": 1`
2. 增加随机延迟
3. 使用代理IP (需要自行配置)
4. 更换User-Agent

### Q: 内存占用过高？

A:
1. 减少批处理大小: `"batch_size": 10`
2. 减少WebDriver池: `"driver_pool_size": 2`
3. 启用无头模式: `"headless": true`

### Q: 如何验证数据质量？

A:
```python
# 数据质量检查
results = pd.read_csv('output.csv')

print("数据统计:")
print(f"总POI数: {len(results)}")
print(f"唯一建筑: {results['blt_name'].nunique()}")
print(f"平均评分: {results['rating'].mean():.2f}")
print(f"空值检查: {results.isnull().sum()}")

# 检查异常数据
print("\n异常数据:")
print(f"评分>5: {len(results[results['rating'] > 5])}")
print(f"评分<1: {len(results[results['rating'] < 1])}")
```

## 📞 技术支持

遇到问题时：

1. **查看日志**: `logs/crawler.log`
2. **检查配置**: `config/default.json`
3. **验证环境**: `python init.py`
4. **重置状态**: 删除 `checkpoint.json`

## 🔄 版本更新

检查更新：
```bash
git pull origin main
pip install -r requirements.txt --upgrade
```

---

**提示**: 建议先用小批量数据测试，确认配置无误后再进行大规模爬取。