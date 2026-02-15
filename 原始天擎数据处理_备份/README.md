# 天擎气象数据处理系统

## 📖 项目背景
2025年暑假，我在天津三星视界工厂打工，负责18650电池载体清洗。那个夏天副高异常顽固，我在流水线旁观察天气变化，萌生了开发气象数据分析系统的想法。回校后，这个想法逐渐成型为“天擎”系统。


## 🎯 项目简介
一个自动化气象数据质量分析系统，实现数据加载→异常检测→缺失值处理→标准化→报告生成全流程。

## 🚀 快速运行
```bash
# 1. 安装依赖
pip install pandas numpy

# 2. 运行系统
python run_pipeline.py

# 3. 查看报告
cat reports/quality_report.txt
📊 核心功能
异常检测：3σ原则识别极端值

缺失值处理：线性插值填充

数据标准化：Z-score归一化

报告生成：自动生成数据质量分析报告

专业可视化：气象局风格500hPa位势高度场

📁 项目结构
text
sky-support-system/
├── run_pipeline.py          # 主运行脚本
├── src/date/                # 核心模块代码
├── data/raw/                # 示例数据
├── reports/                 # 输出报告目录
├── notebooks/               # 可视化分析代码
└── docs/                    # 项目文档
## 📖 了解更多
- [项目故事](docs/STORY.md) - 从工厂到代码的开发历程
- [快速指南](docs/GETTING_STARTED.md) - 详细使用说明  
- [系统架构](docs/ARCHITECTURE.md) - 技术设计文档

## 🎨 示例输出
[500hPa气象图](notebooks/500hPa_气象局风格_校赛版.png)

📜 许可证
MIT License