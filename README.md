# POI Crawler

从Google Maps高效爬取POI数据的轻量化工具。

## 系统要求

- Python 3.8+
- Chrome浏览器
- 稳定的网络连接
- 建议8GB+内存

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

# 3. 开始爬取
python poi_crawler_simple.py --all
```

## 使用说明

### 基本命令

```bash
# 处理所有输入文件
python poi_crawler_simple.py --all

# 处理单个文件
python poi_crawler_simple.py data/input/千代田区_complete.csv

# 使用通配符选择文件
python poi_crawler_simple.py --pattern "data/input/*区_complete*.csv"

# 从文件列表读取
python poi_crawler_simple.py --file-list files_to_process.txt
```

### 高级选项

```bash
# 自定义工作线程数（默认：10）
python poi_crawler_simple.py --all --workers 8

# 自定义批次大小（默认：50）
python poi_crawler_simple.py --all --batch-size 25

# 启用详细日志
python poi_crawler_simple.py --all --verbose

# 禁用断点续传
python poi_crawler_simple.py --all --no-resume

# 禁用进度条（适合脚本环境）
python poi_crawler_simple.py --all --no-progress

# 调试模式（单线程+详细日志）
python poi_crawler_simple.py data/input/test_sample.csv --workers 1 --verbose

# 生产环境（禁用进度条，适合脚本）
python poi_crawler_simple.py --all --no-progress
```

### 地址处理说明

系统支持的地址字段（按优先级）：

1. **FormattedAddress** - 标准化格式地址（优先使用）
2. **Address** - 日文原始地址（重试时使用）
3. **ConvertedAddress** - 英文转换地址（备用）

地址处理建议：

- 准备多种格式的地址数据可提高成功率
- 日文地址应包含完整的行政区划信息
- 英文地址使用标准拼写和格式

## 数据格式

### 输入CSV格式要求

| 列名             | 说明         | 示例                               | 必需 |
| ---------------- | ------------ | ---------------------------------- | ---- |
| District         | 区域名称     | 千代田区                           | 可选   |
| Latitude         | 纬度坐标     | 35.6895                            | 可选  |
| Longitude        | 经度坐标     | 139.6917                           | 可选   |
| Address          | 日文地址     | 東京都千代田区神田駿河台3丁目1-1   | 是   |
| ConvertedAddress | 英文转换地址 | 〒101-0062,+Tokyo,+Chiyoda+City... | 可选 |
| FormattedAddress | 标准化地址   | 3-chōme-1-1+Kanda+Surugadai...    | 可选 |

### POI输出数据字段

| 字段          | 说明       | 示例              |
| ------------- | ---------- | ----------------- |
| name          | POI名称    | スターバックス    |
| rating        | 评分       | 4.2               |
| class         | 分类       | コーヒーショップ  |
| add           | 地址       | 東京都千代田区... |
| comment_count | 评论数     | 150               |
| blt_name      | 建筑物名称 | 神田ビル          |
| lat           | 纬度       | 35.6895           |
| lng           | 经度       | 139.6917          |

### 地址处理优先级

系统按以下优先级选择地址：

1. **FormattedAddress** - 优先使用（如果存在且非空）
2. **Address** - 日文原始地址
3. **ConvertedAddress** - 英文转换地址

当 FormattedAddress 被识别为无效地址页面时，系统会自动使用 Address 的日文地址进行重试。

## 项目结构

```
poi_crawler/
├── poi_crawler_simple.py          # 主程序 - 轻量化POI爬虫
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
    └── archive/                # 历史数据存档
        └── tokyo_complete_mapping.json  # 地址映射数据
```

### 智能重试机制

当检测到以下情况时，系统会自动使用日文地址重试：

- 页面无有效H1标题（无效地址页面）
- 当前使用的是英文地址
- 存在原始日文地址

## 故障排除

### 常见问题

1. **Chrome驱动问题**

   ```bash
   # 手动更新Chrome驱动
   pip install --upgrade webdriver-manager
   ```
2. **内存不足**

   ```bash
   # 减少工作线程数
   python poi_crawler_simple.py --all --workers 5

   # 减少批次大小
   python poi_crawler_simple.py --all --batch-size 25
   ```
3. **网络超时**

   - 检查网络连接稳定性
   - 考虑使用VPN（如访问受限）
4. **数据格式错误**

   - 确保输入CSV包含必需列
   - 检查坐标格式是否为小数

### 调试模式

```bash
# 启用详细日志进行调试
python poi_crawler_simple.py data/input/测试文件.csv --verbose --workers 1

# 禁用断点续传进行全新测试
python poi_crawler_simple.py data/input/测试文件.csv --no-resume

# 组合调试选项
python poi_crawler_simple.py data/input/测试文件.csv --verbose --no-resume --workers 1
```

### 日志查看

程序运行日志会显示错误信息，注意查看控制台输出。主要日志类型：

- 🔍 处理信息
- ✅ 成功消息
- ❌ 错误警告
- 📊 统计数据
- 🔄 重试信息

## 注意事项

- 提供多种地址格式可获得最佳爬取效果
- 爬取大量数据时请确保网络稳定
- 遵守Google Maps服务条款，合理控制请求频率
- 建议在非高峰时段运行大批量任务
- 长时间运行数小时后性能会逐渐下降，建议分批处理

## 开发指南

参考 `CLAUDE.md` 文件了解代码架构和开发规范。

### 文件说明

- `poi_crawler_simple.py` - 主程序入口，管理线程池和任务队列
- `info_tool.py` - POI数据提取，包含XPath选择器
- `driver_action.py` - 浏览器自动化操作（滚动、点击等）

---

## 测试样例

项目包含测试样例文件 `data/input/test_sample.csv`，包含8条东京知名地点的测试数据，可用于快速验证系统功能。

```bash
# 使用测试文件验证系统
python poi_crawler_simple.py data/input/test_sample.csv --verbose
```

---

**版本**: v6.1 (2025-07)
**特性**: 轻量化架构 | 断点续传 | 实时进度条 | 优化重试机制 | 无效地址早期检测
