# POI Crawler - Google Maps 兴趣点爬虫

高性能并行爬虫，用于从Google Maps提取日本地址的POI（兴趣点）信息。

## 功能特点

- **高速并行处理** - 多进程架构，比传统单线程快4-8倍
- **静默运行** - 爬取过程无冗余输出，仅在关键节点显示进度
- **智能数据提取** - 自动识别建筑物类型，提取POI详情和评论数量
- **批量容错** - 单个地址失败不影响整批处理，数据实时保存
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
开始爬取 100 个地址，分 5 批次处理

東京都新宿区歌舞伎町1-1-1 | 建筑物: 是 | 滑动: 是 | POI: 12 | 评论: 245
東京都新宿区西新宿2-8-1 | 建筑物: 否 | 滑动: 否 | POI: 0 | 评论: 0
東京都渋谷区道玄坂1-2-3 | 建筑物: 是 | 滑动: 否 | POI: 3 | 评论: 67

批次 1/5 完成: 成功 18, 失败 2, 耗时 35.2s, 总进度 20/100

爬取完成！总成功: 95, 总失败: 5
```

## 输出数据

结果保存在 `data/output/final_poi_data_[timestamp].csv`，包含字段：

| 字段 | 说明 |
|------|------|
| name | POI名称 |
| rating | 评分 |
| class | POI分类 |
| add | POI地址 |
| blt_name | 建筑物名称 |
| lat | 纬度 |
| lng | 经度 |
| comment_count | 评论数量 |

## 配置选项

### 性能调优
```python
# 修改并发数和批次大小
crawler = ParallelPOICrawler(
    max_workers=8,    # 并发进程数（建议不超过CPU核心数）
    batch_size=50     # 批次大小
)
```

### 浏览器选项
- 默认使用headless Chrome
- 自动禁用图片/JavaScript/扩展以提升速度
- 每个进程独立浏览器实例

## 项目架构

```
poi_crawler/
├── parallel_poi_crawler.py    # 主程序-并行爬虫
├── info_tool.py              # 数据提取工具
├── driver_action.py          # 浏览器交互操作
├── utilities.py              # 文件I/O工具
├── data/
│   ├── input/               # 输入CSV文件
│   └── output/              # 输出结果文件
└── requirements.txt         # Python依赖
```

## 技术说明

### 核心依赖
- **Selenium** - 浏览器自动化
- **BeautifulSoup** - HTML解析
- **Pandas** - 数据处理
- **ChromeDriver** - Chrome浏览器驱动

### 限制与注意事项
- 依赖Google Maps的页面结构，UI变化可能影响爬取
- 建议合理控制请求频率，避免被封IP
- 大批量爬取时注意监控系统资源使用

## 故障排除

**常见问题：**
1. **ChromeDriver版本不匹配** - 使用webdriver-manager自动管理
2. **页面加载超时** - 检查网络连接和代理设置
3. **XPath失效** - Google Maps界面更新可能需要调整选择器

**性能优化：**
- 减少batch_size可降低内存使用
- 增加max_workers可提升速度（注意CPU限制）
- 调整Chrome选项以适应不同环境

## 许可证

MIT License