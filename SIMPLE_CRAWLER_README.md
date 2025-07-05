# 简化版POI爬虫 (poi_crawler_simple.py)

基于现有代码重新构建的简洁高效版本，采用持久化Chrome工作线程架构。

## 🏗️ 架构设计

### 核心组件

1. **ChromeWorker** - 持久化Chrome工作线程
   - 每个线程持有独立的Chrome驱动实例
   - 持续运行，避免频繁创建/销毁driver
   - 自动清理缓存和内存

2. **queue.Queue** - 任务分配控制
   - 线程安全的任务队列
   - 自动负载均衡
   - 支持优雅停止

3. **ResultBuffer** - 结果缓存池
   - 内存缓存POI数据
   - 定期自动落盘
   - 批量写入优化

## 🚀 使用方法

### 基础用法

#### 单文件处理
```bash
# 使用默认配置（10个工作线程，安静模式）
python poi_crawler_simple.py data/input/世田谷区_optimized.csv

# 详细日志模式（开发调试时使用）
python poi_crawler_simple.py data/input/世田谷区_optimized.csv --verbose

# 指定输出文件
python poi_crawler_simple.py data/input/世田谷区_optimized.csv -o my_output.csv
```

#### 批量处理模式
```bash
# --all: 自动处理所有区域文件
python poi_crawler_simple.py --all

# --pattern: 使用通配符模式选择文件
python poi_crawler_simple.py --pattern "data/input/*区_complete*.csv"

# --file-list: 从TXT文件读取文件列表
python poi_crawler_simple.py --file-list my_files.txt

# 批量处理 + 自定义输出目录
python poi_crawler_simple.py --all -o /path/to/output/dir
```

#### 高级配置
```bash
# 高性能配置（15个线程，大批次，快速刷新）
python poi_crawler_simple.py --all -w 15 -b 100 -f 15

# 内存友好配置（5个线程，小批次）
python poi_crawler_simple.py data/input/large_file.csv -w 5 -b 25 -f 60

# 调试模式（详细日志 + 少量线程）
python poi_crawler_simple.py --all --verbose -w 3
```

### 参数说明

| 参数 | 默认值 | 说明 |
|-----|--------|------|
| `input_file` | - | 输入CSV文件路径（单文件模式） |
| `--all` | False | 批量处理所有区域文件 (data/input/*区_*.csv) |
| `--file-list` | - | 从TXT文件读取要处理的文件列表 |
| `--pattern` | - | 使用通配符模式选择文件 |
| `--workers` / `-w` | 10 | Chrome工作线程数量 |
| `--batch-size` / `-b` | 50 | 结果缓存批次大小 |
| `--flush-interval` / `-f` | 30 | 自动落盘间隔（秒） |
| `--output` / `-o` | 自动生成 | 输出文件路径或目录 |
| `--verbose` / `-v` | False | 详细日志输出模式 |

### 文件列表格式 (TXT)

创建一个文本文件，每行一个CSV文件路径：

```txt
# 示例文件列表 - 注释行以 # 开头
# 支持相对路径（相对于data/input/）和绝对路径

# 优先处理的重要区域
千代田区_complete_1751434744.csv
港区_complete_1751436047.csv

# 主要商业区域
新宿区_optimized_1751437745.csv
渋谷区_optimized_1751446693.csv

# 绝对路径示例
/path/to/specific/file.csv
```

## 🎯 优势特点

### 1. 持久化架构
- **稳定的Chrome实例**：避免频繁创建/销毁driver
- **减少资源消耗**：每个线程复用同一个driver
- **降低启动开销**：初始化一次，持续使用

### 2. 高效的任务管理
- **Queue.Queue控制**：线程安全的任务分发
- **自动负载均衡**：空闲线程自动获取新任务
- **优雅停止机制**：确保任务完整处理

### 3. 智能缓存系统
- **内存缓存池**：批量收集POI数据
- **定期自动落盘**：30秒或50条数据触发保存
- **批量写入优化**：减少磁盘IO操作

### 4. 完善的错误处理
- **线程级异常隔离**：单个线程异常不影响其他线程
- **自动重试机制**：Chrome异常时自动清理重建
- **详细状态报告**：实时监控处理进度

## 📊 性能对比

| 特性 | 原版turbo | 简化版simple |
|-----|----------|-------------|
| 架构复杂度 | 高 | 简洁 |
| Chrome实例管理 | 池化+生命周期 | 持久化 |
| 任务分发 | 自定义调度器 | Queue.Queue |
| 数据保存 | 多进程+队列 | 缓存池+定期落盘 |
| 内存使用 | 较高 | 优化 |
| 启动速度 | 较慢 | 快速 |
| 维护性 | 复杂 | 简单 |

## 🔧 技术细节

### Chrome优化配置
```python
# 🔧 禁用图片加载 - 提升20~40%页面加载效率
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs)

# GPU禁用
--disable-gpu
--disable-gpu-sandbox
--use-gl=swiftshader

# 内存限制
--max-memory-in-mb=300
--max-old-space-size=256

# 性能优化
--disable-extensions
--aggressive-cache-discard
--disable-plugins
```

### 🔧 性能优化特性

1. **图片加载禁用**：通过禁用图片加载，页面加载速度提升20~40%
2. **日志压缩**：默认安静模式，减少控制台开销，只在关键时刻输出
3. **智能跳过**：POI为空时快速跳过，避免无意义的磁盘写入
4. **唯一文件名**：时间戳+随机数确保输出文件名唯一，防止意外覆盖

### 线程安全设计
- **驱动隔离**：每个线程独立的Chrome实例
- **队列保护**：使用threading.Lock保护共享资源
- **优雅停止**：stop_event协调线程退出

### 内存管理
- **定期清理**：每100个任务清理浏览器缓存
- **垃圾回收**：强制执行window.gc()
- **Cookie清理**：定期删除累积的cookies

## 📈 监控信息

### 实时状态

**安静模式（默认）**：
```
🚀 启动 10 个Chrome工作线程...
✅ 所有工作线程已启动
📈 总进度: 200/1000 (20.0%) - 成功: 180, 失败: 20
📈 总进度: 400/1000 (40.0%) - 成功: 360, 失败: 40
```

**详细模式（--verbose）**：
```
🚀 Worker 0: 启动
✅ Worker 0: Chrome驱动创建成功
📊 Worker 3: 已处理 100 个任务 (成功: 90, 失败: 10)
💾 批次保存: 47 条数据 (累计: 423)
🏨 检测到酒店页面: hotel | 東京都新宿区歌舞伎町...
```

### 完成报告
```
🎉 所有任务完成！
⏱️  耗时: 12.5 分钟
📊 总计: 1000 个任务
✅ 成功: 920
❌ 失败: 80
📈 成功率: 92.0%
📁 结果已保存到: data/output/世田谷区_simple_1751734567.csv
```

## 🛠️ 故障排除

### 常见问题

1. **Chrome启动失败**
   - 检查ChromeDriver版本
   - 确认系统有足够内存
   - 查看防火墙设置

2. **内存使用过高**
   - 减少工作线程数：`-w 5`
   - 增加刷新频率：`-f 15`
   - 降低批次大小：`-b 25`

3. **处理速度慢**
   - 增加工作线程数：`-w 15`
   - 检查网络连接
   - 确认Google Maps访问正常

## 🔄 从turbo版本迁移

### 命令对比
```bash
# 原版turbo
python parallel_poi_crawler_turbo.py data/input/file.csv --workers 30

# 简化版simple
python poi_crawler_simple.py data/input/file.csv -w 10
```

### 数据格式兼容
- 支持相同的CSV输入格式
- 输出格式完全兼容
- FormattedAddress优先级保持一致

## 📝 注意事项

1. **资源使用**：10个Chrome实例大约占用3GB内存
2. **网络要求**：需要稳定的Google Maps访问
3. **并发限制**：建议不超过15个工作线程
4. **磁盘空间**：确保输出目录有足够空间
5. **中断恢复**：当前版本不支持断点续传（可后续添加）

## 🚀 快速开始

### 单文件测试
```bash
# 1. 测试小文件（3个线程，详细日志）
python poi_crawler_simple.py data/input/small_test.csv -w 3 --verbose

# 2. 正式运行单个区域
python poi_crawler_simple.py data/input/世田谷区_optimized.csv

# 3. 高性能单文件处理
python poi_crawler_simple.py data/input/large_file.csv -w 15 -b 100 -f 15
```

### 批量处理
```bash
# 1. 处理所有complete文件（高质量数据）
python poi_crawler_simple.py --pattern "data/input/*区_complete*.csv"

# 2. 处理所有区域文件
python poi_crawler_simple.py --all

# 3. 从列表文件处理（自定义优先级）
python poi_crawler_simple.py --file-list example_file_list.txt
```

## 📋 常见使用场景

### 场景1：快速测试
```bash
# 测试新配置，只处理几个重要区域
python poi_crawler_simple.py --pattern "data/input/{千代田,港,新宿}区_*.csv" --verbose -w 3
```

### 场景2：生产环境批量处理
```bash
# 高性能批量处理所有区域
python poi_crawler_simple.py --all -w 15 -b 100 -f 15 -o /production/output/
```

### 场景3：按优先级处理
```bash
# 创建priority_files.txt，按重要性排序
python poi_crawler_simple.py --file-list priority_files.txt
```

### 场景4：资源受限环境
```bash
# 内存或CPU受限时的配置
python poi_crawler_simple.py --all -w 5 -b 25 -f 60
```