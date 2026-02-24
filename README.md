【原始天擎】数据处理“大数据技术与应用赛项”校赛项目
## 实验目标
开发气象数据的自动化异常检测与清洗管道，为后续分析提供可靠数据基础

## 技术方案
1. 使用Pandas进行数据读取和初步清洗
2. 基于3σ原则识别极端温度值
3. 实现线性插值处理连续缺失数据
4. 数据标准化和格式统一
5. 生成数据质量分析报告

## 完成标准
- [x] 数据读取模块 (src/data/loader.py)
- [x] 异常值检测函数 (src/data/quality_check.py)  
- [x] 缺失值处理模块 (src/data/imputation.py)
- [x] 数据标准化流程 (src/data/normalization.py)
- [x] 质量报告生成 (src/data/report_generator.py)

## 关联文件
- `src/data/` 目录下的相关Python文件
- `notebooks/01-数据质量分析.ipynb`

## 验收标准
- 能够处理至少1000条气象记录
- 异常检测准确率 > 95%
- 生成可视化的数据质量报告
