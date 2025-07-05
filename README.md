# POI Crawler

从Google Maps并行爬取POI数据，支持断点续传和智能地址转换的高效数据采集工具。

## 版本选择

### Standard 版 (parallel_poi_crawler.py)
- 稳定可靠，适合生产环境
- 多进程架构，资源使用保守
- 推荐日常使用

### Turbo 版 (parallel_poi_crawler_turbo.py)
- 🚀 **异步混合架构**：协程 + 多线程 + 多进程的高性能设计
- 🧠 **动态资源调度**：智能CPU/内存监控，自动负载均衡
- 🔄 **无锁高并发**：线程本地存储，避免锁竞争
- 🎯 **最佳实践配置**：12核心机器36线程，27个Chrome实例
- 📊 **多进程数据处理**：CPU密集型任务独立进程，避免GIL限制

### Simple 版 (poi_crawler_simple.py)
- ⚡ **轻量化设计**：单进程多线程架构，简单高效
- 🔧 **易于调试**：简化的代码结构，便于问题定位和修改
- 🎯 **核心功能**：完整的POI爬取功能，支持断点续传和重试机制
- 💾 **断点续传**：完善的进度管理，支持中断后继续爬取
- 🔄 **智能重试**：非建筑物自动使用日文地址重试

## 特性

### 核心功能
- **异步并行爬取** - 协程 + 多线程混合架构，IO密集型任务高效处理
- **智能Chrome驱动池** - 生命周期管理，自动故障恢复和内存控制
- **断点续传** - 进度自动保存，支持中断后继续爬取
- **智能地址转换** - 日文地址自动转换为标准英文格式
- **多进程数据处理** - CPU密集型DataFrame操作独立进程运行

### 数据质量保障
- **自动去重** - 批次级和文件级双重去重机制
- **智能重试** - 支持primary→secondary→fallback三层重试
- **酒店页面过滤** - 自动跳过酒店分类页面，避免无效数据
- **错误监控和警告** - 完善的错误日志和警告系统
- **动态资源感知** - 实时监控内存/CPU使用，自动调节并发级别

### 操作便利性
- **灵活文件选择** - 支持多文件、通配符、文件列表等多种选择方式
- **批量处理** - 支持批量处理多个区域文件
- **进度监控** - 实时查看爬取进度和状态
- **静默运行** - 无头浏览器模式，后台稳定运行

## 系统要求

- Python 3.8+
- Chrome浏览器
- 充足的内存（建议8GB+）
- 稳定的网络连接

## 安装

```bash
# 克隆项目
git clone <repository-url>
cd poi_crawler

# 安装依赖
pip install -r requirements.txt
```

## 快速开始

```bash
# 1. 准备输入数据（CSV文件，包含坐标和地址信息）
# 2. 放置文件到 data/input/ 目录

# 3a. 使用标准版（推荐）
python parallel_poi_crawler.py --all

# 3b. 使用Turbo版（高性能）
python parallel_poi_crawler_turbo.py --all

# 3c. 使用Simple版（轻量测试）
python poi_crawler_simple.py --all
```

## 详细使用说明

### POI数据爬取

#### 基本使用
```bash
# 标准版 - 处理所有区域文件
python parallel_poi_crawler.py --all

# Turbo版 - 异步高性能处理
python parallel_poi_crawler_turbo.py --all

# 处理单个文件
python parallel_poi_crawler.py data/input/千代田区_complete.csv
python parallel_poi_crawler_turbo.py data/input/千代田区_complete.csv
python poi_crawler_simple.py data/input/千代田区_complete.csv

# 查看爬取进度和状态
python parallel_poi_crawler.py --status
python parallel_poi_crawler_turbo.py --status
```

#### 多文件选择（新功能）
```bash
# 处理多个指定文件
python parallel_poi_crawler.py data/input/千代田区.csv data/input/港区.csv

# 使用通配符选择文件
python parallel_poi_crawler.py --pattern "data/input/*区_complete*.csv"
python parallel_poi_crawler.py --pattern "data/input/*_optimized*.csv"

# 从文件列表读取
python parallel_poi_crawler.py --file-list files_to_process.txt

# 组合使用
python parallel_poi_crawler.py file1.csv --pattern "*_complete*.csv" --file-list more.txt
```

#### 高级选项
```bash
# 标准版 - 使用自定义工作进程数
python parallel_poi_crawler.py --all --workers 4

# Turbo版 - 自定义线程数（默认：CPU核心数×3，推荐配置）
python parallel_poi_crawler_turbo.py --all --workers 36

# Simple版 - 自定义工作线程数（默认：10）
python poi_crawler_simple.py --all --workers 5

# 异步模式（默认且唯一模式）
python parallel_poi_crawler_turbo.py --all

# 禁用断点续传
python parallel_poi_crawler.py --pattern "*.csv" --no-resume
python poi_crawler_simple.py --pattern "*.csv" --no-resume

# 自定义批次大小
python parallel_poi_crawler.py --all --batch-size 100
python poi_crawler_simple.py --all --batch-size 25

# Simple版详细日志
python poi_crawler_simple.py --all --verbose
```

### 地址转换

```bash
# 转换单个文件（直接覆盖原文件）
python address_converter.py data/oring_add/千代田区.csv

# 批量转换所有文件
python address_converter.py --all

# 强制重新转换所有文件
python address_converter.py --regenerate
```

## 数据格式

### 输入CSV格式要求
| 列名 | 说明 | 示例 |
|------|------|------|
| District | 区域名称 | 千代田区 |
| Latitude | 纬度坐标 | 35.6895 |
| Longitude | 经度坐标 | 139.6917 |
| Address | 日文地址 | 東京都千代田区神田駿河台3丁目1-1 |

### POI输出数据字段
| 字段 | 说明 | 示例 |
|------|------|------|
| name | POI名称 | スターバックス |
| rating | 评分 | 4.2 |
| class | 分类 | コーヒーショップ |
| add | 地址 | 東京都千代田区... |
| comment_count | 评论数 | 150 |
| blt_name | 建筑物名称 | 神田ビル |
| lat | 纬度 | 35.6895 |
| lng | 经度 | 139.6917 |

### 地址转换格式
- **输入**：東京都千代田区神田駿河台3丁目1-1
- **输出**：〒101-0062,+Tokyo,+Chiyoda+City,+Kandasurugadai,+3-chōme−1-1
- 自动添加 `ConvertedAddress` 列到原文件

## 项目结构

```
poi_crawler/
├── parallel_poi_crawler.py        # 主程序 - 多进程稳定版
├── parallel_poi_crawler_turbo.py  # Turbo版 - 高性能多线程版  
├── poi_crawler_simple.py          # Simple版 - 轻量化调试版
├── address_converter.py           # 地址转换工具
├── info_tool.py                  # POI信息提取模块
├── driver_action.py              # 浏览器自动化操作
├── requirements.txt              # Python依赖列表
├── CLAUDE.md                    # 开发指南
├── README.md                    # 项目说明
└── data/                        # 数据目录
    ├── input/                  # 爬取输入文件目录
    ├── output/                 # POI输出结果目录
    ├── progress/               # 进度跟踪文件目录
    ├── warnings/               # 警告日志目录
    ├── no_poi_warnings/        # 无POI警告目录
    ├── non_building_warnings/  # 非建筑物警告目录
    └── archive/                # 映射数据存档
        ├── tokyo_complete_mapping.json  # 地址映射数据
        └── x-ken-all.csv              # 邮编数据
```

## 高级功能

### 断点续传机制
- 程序会自动在 `data/progress/` 目录保存进度
- 重启后自动从上次中断位置继续
- 删除进度文件可重新开始爬取

### 性能优化

#### 标准版 (parallel_poi_crawler.py)
- 默认工作进程数：CPU核心数-1
- 批量大小：50条记录/批次
- 驱动池复用：减少浏览器启动开销

#### Turbo版 (parallel_poi_crawler_turbo.py)
- **异步混合架构**：协程 + ThreadPoolExecutor + multiprocessing
- **最佳实践配置**：36线程 + 27个Chrome实例（12核心推荐）
- **智能并发控制**：信号量限制同时任务数（25个）
- **多进程数据处理**：DataFrame操作独立进程，避免GIL
- **动态资源调度**：实时监控CPU/内存，自动负载均衡
- **内存优化**：Chrome实例350MB限制，总内存约10.5GB
- **FIFO任务队列**：确保公平处理
- **线程本地统计**：无锁竞争，高效并发

#### Simple版 (poi_crawler_simple.py)
- **单进程多线程**：10个持久化Chrome工作线程
- **批量大小**：50条记录/批次（可调节）
- **断点续传**：完善的进度管理和恢复机制
- **智能重试**：非建筑物自动使用日文地址重试
- **酒店页面过滤**：自动识别并跳过酒店分类页面
- **内存优化**：定期清理浏览器缓存，防止内存泄漏
- **调试友好**：详细的日志输出，便于问题定位

### 地址转换原理
基于预建的东京地区映射数据库：
- 区名映射：日文区名→英文区名
- 地名映射：日文地名→英文地名  
- 邮编映射：地区→对应邮编
- 格式标准化：统一英文地址格式

### 文件列表格式
创建文件列表 `files_to_process.txt`：
```
# 这是注释，会被忽略
data/input/千代田区_complete.csv
data/input/港区_complete.csv
# 空行会被忽略

data/input/中央区_complete.csv
```

## 故障排除

### 常见问题

1. **Chrome驱动问题**
   ```bash
   # 手动更新Chrome驱动
   pip install --upgrade webdriver-manager
   ```

2. **内存不足**
   ```bash
   # 减少工作进程数
   python parallel_poi_crawler.py --all --workers 2
   
   # 使用轻量级Simple版
   python poi_crawler_simple.py --all --workers 3
   ```

3. **网络超时**
   - 检查网络连接稳定性
   - 考虑使用VPN（如访问受限）

4. **Turbo版性能问题**
   ```bash
   # 如果系统资源不足，降低线程数
   python parallel_poi_crawler_turbo.py --all --workers 24
   
   # 异步模式现在是默认且唯一的模式，无需额外参数
   
   # 或者使用稳定的标准版
   python parallel_poi_crawler.py --all
   
   # 或者使用轻量化Simple版进行调试
   python poi_crawler_simple.py --all --verbose
   ```

5. **调试和开发问题**
   ```bash
   # 使用Simple版的详细日志
   python poi_crawler_simple.py data/input/测试文件.csv --verbose --workers 1
   
   # 禁用断点续传进行全新测试
   python poi_crawler_simple.py data/input/测试文件.csv --no-resume
   ```

6. **数据格式错误**
   - 确保输入CSV包含必需列
   - 检查坐标格式是否为小数

### 日志查看
程序运行日志会显示错误信息，注意查看控制台输出。

## 版本选择建议

### 何时使用标准版 (parallel_poi_crawler.py)
- **生产环境**：需要稳定性和可靠性
- **大批量任务**：处理成千上万的地址数据
- **资源受限环境**：内存或CPU资源有限的服务器
- **长期运行**：需要24/7不间断运行的任务

### 何时使用Turbo版 (parallel_poi_crawler_turbo.py)
- **高性能需求**：追求最大处理速度
- **资源充足环境**：16GB+内存，8+核心CPU
- **大规模数据处理**：处理整个东京地区的数据
- **时间敏感任务**：需要快速完成的紧急项目

### 何时使用Simple版 (poi_crawler_simple.py)
- **开发和测试**：代码调试和功能验证
- **小规模数据**：处理几百到几千条地址
- **学习和研究**：理解POI爬取机制
- **问题排查**：当其他版本出现问题时用于定位
- **资源极度受限**：内存少于8GB的环境

## 注意事项

- 地址转换会直接覆盖原文件，请提前备份重要数据
- 爬取大量数据时请确保网络稳定
- 遵守Google Maps服务条款，合理控制请求频率
- 建议在非高峰时段运行大批量任务

## 架构设计

### Turbo版异步架构

```
📋 主控制器
├── 🔄 AsyncCrawlWrapper (协程调度层)
│   ├── ThreadPoolExecutor (IO密集型任务)
│   └── Semaphore (并发控制)
├── 🚗 ChromeDriverPool (驱动池管理)
│   ├── 生命周期管理
│   └── 故障恢复
├── 💾 DataSaveWorker (多进程数据处理)
│   ├── 独立进程运行
│   └── 避免GIL限制
└── 📊 DynamicResourceScheduler (动态调度)
    ├── CPU监控
    ├── 内存监控
    └── 自动负载均衡
```

### 配置参数（12核心32GB系统）

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| max_workers | 36 | CPU核心数 × 3 |
| max_drivers | 27 | 线程数 × 0.75 |
| max_concurrent_tasks | 25 | 信号量限制 |
| batch_size | 125 | 批处理大小 |
| chrome_memory_limit | 350MB | 单实例内存限制 |
| progress_interval | 5分钟 | 进度保存间隔 |

### 性能特点

- **CPU使用率**：充分利用多核（从20%提升到80%+）
- **内存管理**：严格控制Chrome实例数量和内存使用
- **并发模型**：IO密集型协程 + CPU密集型多进程
- **容错机制**：自动故障恢复和资源重分配

## 开发指南

参考 `CLAUDE.md` 文件了解代码架构和开发规范。
参考 `TURBO_FIXES.md` 文件了解Turbo版优化历程。