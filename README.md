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
```bash
python parallel_poi_crawler.py data/input/your_addresses.csv
```

## 输出示例

```
开始爬取 千代田区 8693 个地址，分 435 批次处理
输出文件: data/output/千代田区_poi_data_1751467890.csv

東京都千代田区鍛冶町1丁目7-1 | 建筑物: 是 | 滑动: 是 | POI: 5
東京都千代田区麹町2丁目6-5 | 建筑物: 否 | 滑动: 否 | POI: 0
東京都千代田区外神田4丁目4-3 | 建筑物: 是 | 滑动: 否 | POI: 3

批次 1/435 完成: 成功 18, 失败 2, 耗时 35.2s, 总进度 20/8693

爬取完成！总成功: 8500, 总失败: 193
数据已保存到: data/output/千代田区_poi_data_1751467890.csv
```

## 输出数据

结果实时保存在 `data/output/[区名]_poi_data_[timestamp].csv`，包含字段：

| 字段 | 说明 |
|------|------|
| name | POI名称 |
| rating | 评分 |
| class | POI分类 |
| add | POI地址 |
| comment_count | 该POI的评论数量 |
| blt_name | 建筑物名称 |
| lat | 纬度 |
| lng | 经度 |

## 配置选项

### 性能调优
```python
# 修改并发数和批次大小
crawler = ParallelPOICrawler(
    max_workers=4,    # 并发进程数（默认CPU核心数-1）
    batch_size=20     # 批次大小（默认20）
)
```

### 浏览器选项
- 完全静默headless Chrome
- 禁用GPU/WebGL/图片/JavaScript以提升速度
- 每个进程独立浏览器实例

## 项目架构

```
poi_crawler/
├── parallel_poi_crawler.py    # 主程序-并行爬虫
├── info_tool.py              # 数据提取工具
├── driver_action.py          # 浏览器交互操作
├── data/
│   ├── input/               # 输入CSV文件
│   └── output/              # 输出结果文件（以区命名）
├── requirements.txt         # Python依赖
├── .gitignore              # Git忽略配置
└── CLAUDE.md               # AI助手指南
```

## 核心技术

### 依赖组件
- **Selenium** - 浏览器自动化
- **BeautifulSoup** - HTML解析  
- **Pandas** - 数据处理
- **ChromeDriver** - Chrome浏览器驱动
- **multiprocessing** - 并行处理

### 数据提取逻辑
- 建筑物类型识别：通过XPath检测页面元素
- POI信息解析：从HTML中提取名称、评分、分类、地址
- 评论数量：正则表达式匹配 `评分(评论数)` 格式
- 坐标获取：从URL中解析经纬度

## 注意事项

### 技术限制
- 依赖Google Maps页面结构，UI变化可能影响爬取
- XPath选择器可能因界面更新而失效
- 建议合理控制请求频率

### 性能建议  
- 默认配置适合大多数场景
- 大批量数据建议监控系统资源
- 网络不稳定时可减少batch_size

### 故障排除
- **全部显示"建筑物: 否"** - 检查建筑物类型XPath选择器
- **评论数量为0** - 检查HTML结构变化
- **进程卡死** - 可能是网络超时，重启程序

## 许可证

MIT License