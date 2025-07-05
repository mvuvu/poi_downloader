# POI Crawler

从Google Maps并行爬取POI数据，支持断点续传和智能地址转换的高效数据采集工具。

## 版本选择

### Standard 版 (parallel_poi_crawler.py)
- 稳定可靠，适合生产环境
- 多进程架构，资源使用保守
- 推荐日常使用

### Turbo 版 (parallel_poi_crawler_turbo.py)
- 高性能多线程架构，适合高配置机器
- 科学的并发配置：12核心机器可达48线程
- 高效Chrome驱动池管理和资源复用
- 适合大量数据爬取任务

## 特性

### 核心功能
- **多线程/多进程并行爬取** - 支持多核CPU充分利用，提高爬取效率
- **Chrome驱动池管理** - 智能复用Chrome实例，减少启动开销
- **断点续传** - 进度自动保存，支持中断后继续爬取
- **智能地址转换** - 日文地址自动转换为标准英文格式

### 数据质量保障
- **自动去重** - 批次级和文件级双重去重机制
- **智能重试** - 支持primary→secondary→fallback三层重试
- **酒店页面过滤** - 自动跳过酒店分类页面，避免无效数据
- **错误监控和警告** - 完善的错误日志和警告系统

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
```

## 详细使用说明

### POI数据爬取

#### 基本使用
```bash
# 标准版 - 处理所有区域文件
python parallel_poi_crawler.py --all

# Turbo版 - 高性能处理
python parallel_poi_crawler_turbo.py --all

# 处理单个文件
python parallel_poi_crawler.py data/input/千代田区_complete.csv
python parallel_poi_crawler_turbo.py data/input/千代田区_complete.csv

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

# Turbo版 - 自定义线程数（默认：CPU核心数×4）
python parallel_poi_crawler_turbo.py --all --workers 24

# 禁用断点续传
python parallel_poi_crawler.py --pattern "*.csv" --no-resume

# 自定义批次大小
python parallel_poi_crawler.py --all --batch-size 100
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
├── address_converter.py           # 地址转换工具
├── info_tool.py                  # POI信息提取模块
├── driver_action.py              # 浏览器自动化操作
├── requirements.txt              # Python依赖列表
├── CLAUDE.md                    # 开发指南
├── README.md                    # 项目说明
├── TURBO_FIXES.md               # Turbo版优化记录
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
- 高并发线程数：12核心机器可达48线程
- Chrome驱动池：20个实例智能管理
- 批量处理：25条记录/批次，平衡性能和IO
- FIFO任务队列：确保公平处理
- 线程本地统计：无锁竞争，高效并发

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
   ```

3. **网络超时**
   - 检查网络连接稳定性
   - 考虑使用VPN（如访问受限）

4. **Turbo版性能问题**
   ```bash
   # 如果系统资源不足，降低线程数
   python parallel_poi_crawler_turbo.py --all --workers 16
   
   # 或者使用稳定的标准版
   python parallel_poi_crawler.py --all
   ```

4. **数据格式错误**
   - 确保输入CSV包含必需列
   - 检查坐标格式是否为小数

### 日志查看
程序运行日志会显示错误信息，注意查看控制台输出。

## 注意事项

- 地址转换会直接覆盖原文件，请提前备份重要数据
- 爬取大量数据时请确保网络稳定
- 遵守Google Maps服务条款，合理控制请求频率
- 建议在非高峰时段运行大批量任务

## 开发指南

参考 `CLAUDE.md` 文件了解代码架构和开发规范。