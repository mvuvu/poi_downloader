# POI Crawler - Google Maps 兴趣点爬虫

高性能并行爬虫，用于从Google Maps提取日本地址的POI（兴趣点）信息。

## 功能特点

- **高速并行处理** - 多进程架构，比传统单线程快4-8倍
- **静默运行** - 爬取过程完全静默，仅显示地址完成状态和批次进度
- **智能数据提取** - 自动识别建筑物类型，提取POI详情和评论数量
- **实时数据保存** - 以区命名文件，实时追加数据，中断不丢失
- **灵活配置** - 支持自定义并发数和批次大小

## 快速开始

### 安装依赖
```bash
pip install -r requirements.txt
```

### 准备数据
在 `data/input/` 目录下放置CSV文件，格式：
```csv
District,Latitude,Longitude,Address
千代田区,35.6917,139.7736,東京都千代田区内神田1丁目13-1
```

### 运行爬虫

**批量处理所有区（推荐）：**
```bash
python parallel_poi_crawler.py --all
```

**处理单个区文件：**
```bash
python parallel_poi_crawler.py data/input/千代田区_complete.csv
```

## 输出示例

**批量处理模式：**
```
发现 23 个区文件，开始批量处理...

============================================================
处理第 1/23 个区: 千代田区
============================================================
开始爬取 千代田区 8689 个地址，分 435 批次处理
输出文件: data/output/千代田区_poi_data_1751467888.csv

東京都千代田区鍛冶町1丁目7-1 | 建筑物: 是 | 滑动: 是 | POI: 5
東京都千代田区麹町2丁目6-5 | 建筑物: 否 | 滑动: 否 | POI: 0

批次 1/435 完成: 成功 18, 失败 2, 耗时 35.2s, 总进度 20/8689

千代田区 爬取完成！总成功: 8500, 总失败: 189

============================================================
处理第 2/23 个区: 中央区
============================================================
[继续处理其他区...]

============================================================
全部区域处理完成！
============================================================
总耗时: 120.5 分钟
总成功: 850000, 总失败: 29955
处理了 23 个区:
  千代田区: 成功8500, 失败189
  中央区: 成功9800, 失败597
  港区: 成功15200, 失败567
  ...
```

## 输出数据

### 文件结构
- **批量模式**：每个区生成独立文件 `data/output/[区名]_poi_data_[timestamp].csv`
- **单区模式**：单个文件 `data/output/[区名]_poi_data_[timestamp].csv`

### 数据字段
| 字段 | 说明 | 来源函数 |
|------|------|----------|
| name | POI名称 | get_poi_name() |
| rating | 评分 | get_rating() |
| class | POI分类 | get_class_address() |
| add | POI地址 | get_class_address() |
| comment_count | 该POI的评论数量 | get_rating_count() |
| blt_name | 建筑物名称 | get_building_name() |
| lat | 纬度 | get_coords() |
| lng | 经度 | get_coords() |

### 数据规模（东京23区）
| 区名 | 地址数量 | 区名 | 地址数量 |
|------|----------|------|----------|
| 千代田区 | 8,689 | 豊島区 | 28,390 |
| 中央区 | 10,397 | 北区 | 30,243 |
| 港区 | 15,767 | 中野区 | 33,929 |
| 文京区 | 18,880 | 板橋区 | 46,769 |
| 台东区 | 22,088 | 葛飾区 | 49,294 |
| 渋谷区 | 22,307 | 江戸川区 | 63,583 |
| 墨田区 | 25,309 | 杉並区 | 63,881 |
| 江東区 | 25,519 | 大田区 | 66,577 |
| 目黒区 | 26,303 | 足立区 | 72,200 |
| 品川区 | 27,270 | 練馬区 | 75,420 |
| 新宿区 | 28,058 | 世田谷区 | 99,657 |
| 荒川区 | 19,423 | **总计** | **879,955** |

## 配置选项

### 批量处理参数
```bash
# 批量处理所有区（自动发现input目录中的CSV文件）
python parallel_poi_crawler.py --all

# 单区处理
python parallel_poi_crawler.py data/input/specific_district.csv
```

### 性能调优
```python
# 在代码中修改并发数和批次大小（parallel_poi_crawler.py第321行）
crawler = ParallelPOICrawler(
    max_workers=4,    # 并发进程数（默认CPU核心数-1）
    batch_size=20     # 批次大小（实际代码默认50）
)
```

### 浏览器选项
- 完全静默headless Chrome
- 禁用GPU/WebGL/图片/JavaScript以提升速度
- 每个进程独立浏览器实例

## 项目架构

```
poi_crawler/
├── parallel_poi_crawler.py    # 主程序-并行爬虫(338行)
├── info_tool.py              # 数据提取工具(200行)
├── driver_action.py          # 浏览器交互操作(74行)
├── data/
│   ├── input/               # 输入CSV文件(23个区)
│   └── output/              # 输出结果文件（以区命名）
├── requirements.txt         # Python依赖(6个包)
├── .gitignore              # Git忽略配置
├── CLAUDE.md               # AI助手指南
├── README.md               # 项目说明文档
└── start.ipynb             # Jupyter笔记本(开发用)
```

## 核心技术

### 依赖组件
- **Selenium** - 浏览器自动化（ChromeDriver管理）
- **BeautifulSoup** - HTML解析（lxml引擎）
- **Pandas** - 数据处理和CSV操作
- **webdriver-manager** - Chrome驱动自动管理
- **ProcessPoolExecutor** - 多进程并行处理
- **tqdm** - 进度条显示（部分功能已移除）

### 数据提取逻辑
- **建筑物类型识别**：XPath `//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/span/span/span`
- **POI信息解析**：BeautifulSoup解析HTML，提取名称、评分、分类、地址
- **评论数量**：从 `UY7F9` class元素中提取括号内数字
- **坐标获取**：从Google Maps URL中解析 `/@lat,lng` 格式
- **滚动加载**：自动计算滚动次数，最多112次（1130条POI限制）

## 注意事项

### 关键实现细节
- **Chrome配置**：完全静默模式，禁用图片/JS/GPU以提升性能
- **并行架构**：ProcessPoolExecutor实现真正的多进程并行
- **实时保存**：每个POI立即追加到CSV，防止数据丢失
- **错误隔离**：单个地址失败不影响整批处理
- **内存优化**：分批处理避免大数据集内存溢出

### 技术限制与解决方案
- **XPath依赖**：界面变化需更新 `info_tool.py` 中的选择器
- **Google Maps限制**：单个建筑物最多1130个POI
- **网络稳定性**：建议监控网络状态，必要时降低batch_size

### 故障排除
- **建筑物识别失败** → 检查 `info_tool.py:39` XPath选择器
- **POI数据缺失** → 验证CSS选择器 `Nv2PK.THOPZb.CpccDe` 和 `Nv2PK.Q2HXcd.THOPZb`
- **进程卡死** → 检查Chrome驱动是否正常，重启程序
- **数据重复** → 检查实时追加逻辑，确保CSV header正确
- **大区耗时长** → 世田谷区(99,657地址)预计需要数小时

## 许可证

MIT License