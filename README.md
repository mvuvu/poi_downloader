# POI Crawler - Google Maps 兴趣点爬虫

高性能并行爬虫，专门用于从Google Maps提取日本地址的POI（兴趣点）信息，支持断点续传和智能地址转换。

## 🚀 核心特性

- **🔄 断点续传** - 支持任务中断后自动恢复，保证数据完整性
- **⚡ 高速并行** - 多进程架构，比传统单线程快4-8倍
- **🌐 智能地址转换** - 自动使用ConvertedAddress字段，提升爬取成功率
- **🤐 静默运行** - 完全静默Chrome，仅显示关键进度信息
- **💾 实时保存** - 按区域实时追加数据，中断不丢失
- **📊 进度管理** - 完整的任务状态查看和管理功能

## 📦 安装与配置

### 安装依赖
```bash
pip install -r requirements.txt
```

### 数据格式
在 `data/input/` 目录下放置CSV文件，支持两种格式：

**推荐格式（包含转换地址）：**
```csv
District,Latitude,Longitude,Address,ConvertedAddress
千代田区,35.6917,139.7736,東京都千代田区内神田1丁目13-1,"〒101-0047+Tokyo,+Chiyoda+City,+Uchikanda,+1-chōme−13-1"
```

**传统格式：**
```csv
District,Latitude,Longitude,Address
千代田区,35.6917,139.7736,東京都千代田区内神田1丁目13-1
```

> 爬虫会自动优先使用 `ConvertedAddress` 字段，提升地址识别成功率。

## 🎯 使用方法

### 基础操作

**批量处理所有区（推荐）：**
```bash
python parallel_poi_crawler.py --all
```

**处理单个区文件：**
```bash
python parallel_poi_crawler.py data/input/千代田区_complete.csv
```

**自定义配置：**
```bash
# 使用8个并发进程，批次大小50
python parallel_poi_crawler.py --all --workers 8 --batch-size 50

# 禁用断点续传（从头开始）
python parallel_poi_crawler.py --all --no-resume
```

### 断点续传管理

**查看未完成任务：**
```bash
python parallel_poi_crawler.py --status
```

**清理所有进度文件：**
```bash
python parallel_poi_crawler.py --clean-progress
```

**查看帮助：**
```bash
python parallel_poi_crawler.py --help
```

### 命令行参数完整列表

| 参数 | 说明 | 示例 |
|------|------|------|
| `--all` | 批量处理所有区文件 | `--all` |
| `--workers N` | 设置并发进程数 | `--workers 8` |
| `--batch-size N` | 设置批次大小 | `--batch-size 50` |
| `--no-resume` | 禁用断点续传 | `--no-resume` |
| `--status` | 查看未完成任务状态 | `--status` |
| `--clean-progress` | 清理所有进度文件 | `--clean-progress` |

## 📊 输出示例

### 正常爬取模式
```
开始爬取 千代田区 8689 个地址，分 435 批次处理
优化批次大小: 60, 最大并发: 4
输出文件: data/output/千代田区_poi_data_1751467888.csv

〒101-0047+Tokyo,+Chiyoda+City,+Uchikanda,+1-chōme−13-1 | POI: 5
〒101-0031+Tokyo,+Chiyoda+City,+Higashikanda,+2-chōme−6-5 | POI: 0

批次 1/435 完成: 成功 18, 失败 2, 耗时 35.2s, 总进度 60/8689

千代田区 爬取完成！总成功: 8500, 总失败: 189
数据已保存到: data/output/千代田区_poi_data_1751467888.csv
```

### 断点续传模式
```
发现未完成的千代田区爬取任务，从第67批次继续
恢复输出文件: data/output/千代田区_poi_data_1751467888.csv
继续爬取 千代田区，剩余 368 批次
```

### 任务状态查看
```bash
$ python parallel_poi_crawler.py --status

未完成的任务:
============================================================
区域: 千代田区
进度: 67/435 批次 (15.4%)
成功: 1340, 失败: 32
最后更新: 2025-01-03 14:25:30
输出文件: data/output/千代田区_poi_data_1751467888.csv
------------------------------------------------------------
```

## 💽 输出数据

### 文件结构
```
data/
├── input/                    # 输入CSV文件（23个东京区）
│   ├── 千代田区_complete_xxx.csv
│   └── ...
├── output/                   # 输出结果文件
│   ├── 千代田区_poi_data_1751467888.csv
│   └── ...
├── progress/                 # 断点续传进度文件
│   ├── 千代田区_progress.json
│   └── ...
└── archive/                  # 归档的地址转换文件
    ├── tokyo_complete_mapping.json
    └── ...
```

### 数据字段说明
| 字段 | 说明 | 示例 |
|------|------|------|
| `name` | POI名称 | "セブン-イレブン千代田内神田1丁目店" |
| `rating` | 评分 | "3.2" |
| `class` | POI分类 | "コンビニエンスストア" |
| `add` | POI详细地址 | "〒101-0047 東京都千代田区内神田1..." |
| `comment_count` | 评论数量 | "15" |
| `blt_name` | 建筑物名称 | "内神田TSビル" |
| `lat` | 纬度 | "35.6917" |
| `lng` | 经度 | "139.7736" |

## 🏗️ 项目架构

```
poi_crawler/
├── parallel_poi_crawler.py    # 主程序 - 并行爬虫 + 断点续传
├── info_tool.py              # 数据提取工具集
├── driver_action.py          # Selenium交互操作
├── requirements.txt          # Python依赖包
├── .gitignore               # Git忽略配置
├── CLAUDE.md                # AI助手指南
├── README.md                # 项目说明文档
└── data/
    ├── input/               # 输入数据目录
    ├── output/              # 输出数据目录
    ├── progress/            # 断点续传进度目录
    └── archive/             # 归档文件目录
```

## 🔧 技术架构

### 核心依赖
```
selenium>=4.15.0           # 浏览器自动化
webdriver-manager>=4.0.1   # Chrome驱动管理
beautifulsoup4>=4.12.2     # HTML解析
pandas>=2.0.0              # 数据处理
lxml>=4.9.3                # XML/HTML解析引擎
```

### 关键技术特性

**1. 断点续传机制**
- 每个批次完成后自动保存进度到JSON文件
- 支持查看、清理和管理未完成任务
- 任务完成后自动清理进度文件

**2. 智能地址处理**
- 优先使用 `ConvertedAddress` 字段（英文格式）
- 回退到 `Address` 字段（日文格式）
- 提升Google Maps地址识别成功率

**3. 高性能并行架构**
- `ProcessPoolExecutor` 实现真正多进程并行
- 动态批次大小调整（默认60个地址/批次）
- 独立Chrome实例，避免进程间干扰

**4. 静默运行模式**
- 完全headless Chrome配置
- 禁用图片、JavaScript、GPU等资源
- 仅显示关键进度信息

## ⚠️ 重要说明

### 技术限制
- **Google Maps限制**：单个建筑物最多1130个POI
- **XPath依赖**：界面变化需更新选择器
- **网络稳定性**：需要稳定的网络连接

### 性能优化建议
- **并发数**：建议CPU核心数，避免过载
- **批次大小**：网络较差时可降至30-40
- **内存使用**：大区域（如世田谷区）需要较多内存

### 故障排除
- **任务卡死** → 检查网络连接，重启程序
- **地址识别失败** → 确认ConvertedAddress格式正确
- **进度丢失** → 使用 `--status` 检查断点续传状态
- **重复数据** → 清理进度文件后重新开始

## 📈 性能数据

### 东京23区处理规模
| 区域 | 地址数量 | 预计耗时* |
|------|----------|-----------|
| 千代田区 | 8,689 | ~2小时 |
| 世田谷区 | 99,657 | ~20小时 |
| **总计** | **879,955** | **~200小时** |

*基于4核并发，实际速度受网络和硬件影响

## 📄 许可证

MIT License - 详见LICENSE文件