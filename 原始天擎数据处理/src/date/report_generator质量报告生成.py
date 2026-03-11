# src/data/report_generator.py

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Any
import os

class QualityReportGenerator:
    """
    生成气象数据质量报告(纯Python版本)
    """
    
    def __init__(self, raw_df: pd.DataFrame, cleaned_df: pd.DataFrame):
        """
        初始化报告生成器
        
        Args:
            raw_df: 原始数据DataFrame
            cleaned_df: 清洗后的数据DataFrame
        """
        self.raw_df = raw_df.copy()
        self.cleaned_df = cleaned_df.copy()
        self.report = {}
    
    def _get_missing_stats(self, df: pd.DataFrame, column: str = 'temperature') -> Dict[str, Any]:
        """
        计算缺失值统计信息（独立函数，不依赖外部模块）
        """
        if column not in df.columns:
            return {"error": f"列 '{column}' 不存在"}
        
        missing_mask = df[column].isna()
        total_missing = int(missing_mask.sum())
        missing_rate = total_missing / len(df) if len(df) > 0 else 0
        
        # 连续缺失段落分析
        if total_missing > 0:
            # 识别连续缺失段落
            groups = (missing_mask != missing_mask.shift()).cumsum()
            gap_lengths = []
            
            for group_id, group in missing_mask.groupby(groups):
                if group.any():  # 这是一个缺失值段落
                    gap_length = int(group.sum())
                    gap_lengths.append(gap_length)
            
            stats = {
                "total_missing": total_missing,
                "missing_rate": float(missing_rate),
                "missing_percentage": f"{missing_rate * 100:.2f}%",
                "gap_count": len(gap_lengths),
                "max_gap_length": max(gap_lengths) if gap_lengths else 0,
                "avg_gap_length": np.mean(gap_lengths) if gap_lengths else 0,
                "gap_lengths": gap_lengths
            }
        else:
            stats = {
                "total_missing": 0,
                "missing_rate": 0.0,
                "missing_percentage": "0.00%",
                "gap_count": 0,
                "max_gap_length": 0,
                "avg_gap_length": 0,
                "gap_lengths": []
            }
        
        return stats
    
    def generate_basic_stats(self, column: str = 'temperature') -> Dict:
        """
        生成基础统计数据
        """
        basic_stats = {}
        
        # 原始数据统计
        if column in self.raw_df.columns:
            raw_data = self.raw_df[column].dropna()
            basic_stats["raw_data"] = {
                "count": int(len(raw_data)),
                "mean": float(raw_data.mean()) if len(raw_data) > 0 else None,
                "std": float(raw_data.std()) if len(raw_data) > 0 else None,
                "min": float(raw_data.min()) if len(raw_data) > 0 else None,
                "max": float(raw_data.max()) if len(raw_data) > 0 else None,
                "25%": float(raw_data.quantile(0.25)) if len(raw_data) > 0 else None,
                "50%": float(raw_data.quantile(0.5)) if len(raw_data) > 0 else None,
                "75%": float(raw_data.quantile(0.75)) if len(raw_data) > 0 else None,
            }
        
        # 清洗后数据统计
        if column in self.cleaned_df.columns:
            cleaned_data = self.cleaned_df[column].dropna()
            basic_stats["cleaned_data"] = {
                "count": int(len(cleaned_data)),
                "mean": float(cleaned_data.mean()) if len(cleaned_data) > 0 else None,
                "std": float(cleaned_data.std()) if len(cleaned_data) > 0 else None,
                "min": float(cleaned_data.min()) if len(cleaned_data) > 0 else None,
                "max": float(cleaned_data.max()) if len(cleaned_data) > 0 else None,
                "25%": float(cleaned_data.quantile(0.25)) if len(cleaned_data) > 0 else None,
                "50%": float(cleaned_data.quantile(0.5)) if len(cleaned_data) > 0 else None,
                "75%": float(cleaned_data.quantile(0.75)) if len(cleaned_data) > 0 else None,
            }
        
        self.report["basic_stats"] = basic_stats
        return basic_stats
    
    def generate_missing_analysis(self, column: str = 'temperature') -> Dict:
        """
        生成缺失值分析
        """
        raw_missing = self._get_missing_stats(self.raw_df, column)
        cleaned_missing = self._get_missing_stats(self.cleaned_df, column)
        
        missing_analysis = {
            "raw_data": raw_missing,
            "cleaned_data": cleaned_missing,
            "summary": {
                "missing_fixed": raw_missing["total_missing"] - cleaned_missing["total_missing"],
                "missing_remaining": cleaned_missing["total_missing"],
                "improvement_percentage": f"{(1 - cleaned_missing['missing_rate'] / raw_missing['missing_rate']) * 100:.2f}%" 
                                         if raw_missing['missing_rate'] > 0 else "100.00%"
            }
        }
        
        self.report["missing_analysis"] = missing_analysis
        return missing_analysis
    
    def generate_outlier_analysis(self, outlier_mask: Optional[pd.Series] = None) -> Dict:
        """
        生成异常值分析
        
        Args:
            outlier_mask: 布尔序列,True表示异常值
        """
        if outlier_mask is not None:
            outlier_count = int(outlier_mask.sum())
            outlier_rate = outlier_count / len(self.raw_df) if len(self.raw_df) > 0 else 0
            
            outlier_analysis = {
                "detected_outliers": {
                    "count": outlier_count,
                    "rate": float(outlier_rate),
                    "percentage": f"{outlier_rate * 100:.2f}%"
                },
                "method": "3σ原则检测"
            }
        else:
            # 如果没有提供异常值掩码，尝试自动检测
            outlier_analysis = self._auto_detect_outliers()
        
        self.report["outlier_analysis"] = outlier_analysis
        return outlier_analysis
    
    def _auto_detect_outliers(self) -> Dict:
        """
        自动检测异常值(基于3σ原则)
        """
        outlier_analysis = {"method": "自动检测 (3σ原则)"}
        
        for col in ['temperature']:  # 可以扩展到更多列
            if col in self.raw_df.columns:
                data = self.raw_df[col].dropna()
                if len(data) > 0:
                    mean_val = data.mean()
                    std_val = data.std()
                    upper_bound = mean_val + 3 * std_val
                    lower_bound = mean_val - 3 * std_val
                    
                    outliers = ((self.raw_df[col] > upper_bound) | 
                               (self.raw_df[col] < lower_bound)).sum()
                    
                    outlier_analysis[col] = {
                        "mean": float(mean_val),
                        "std": float(std_val),
                        "upper_3sigma": float(upper_bound),
                        "lower_3sigma": float(lower_bound),
                        "outlier_count": int(outliers),
                        "outlier_percentage": f"{(outliers / len(self.raw_df)) * 100:.2f}%" 
                                             if len(self.raw_df) > 0 else "0.00%"
                    }
        
        return outlier_analysis
    
    def generate_data_quality_summary(self) -> Dict:
        """
        生成数据质量摘要
        """
        summary = {
            "report_generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_overview": {
                "raw_data_shape": self.raw_df.shape,
                "cleaned_data_shape": self.cleaned_df.shape,
                "columns": list(self.raw_df.columns) if hasattr(self.raw_df, 'columns') else [],
            },
            "quality_indicators": {}
        }
        
        # 计算质量指标
        if "missing_analysis" in self.report:
            missing_info = self.report["missing_analysis"]
            summary["quality_indicators"]["completeness"] = {
                "raw": 1 - missing_info["raw_data"]["missing_rate"],
                "cleaned": 1 - missing_info["cleaned_data"]["missing_rate"],
                "improvement": missing_info["summary"]["improvement_percentage"]
            }
        
        self.report["summary"] = summary
        return summary
    
    def generate_full_report(self, 
                            column: str = 'temperature',
                            outlier_mask: Optional[pd.Series] = None) -> Dict:
        """
        生成完整报告
        
        Returns:
            包含所有分析的报告字典
        """
        print("\n" + "="*60)
        print("生成数据质量报告")
        print("="*60)
        
        # 生成各个部分
        self.generate_basic_stats(column)
        print("✓ 基础统计完成")
        
        self.generate_missing_analysis(column)
        print("✓ 缺失值分析完成")
        
        self.generate_outlier_analysis(outlier_mask)
        print("✓ 异常值分析完成")
        
        self.generate_data_quality_summary()
        print("✓ 质量摘要完成")
        
        # 添加数据示例
        self.report["data_samples"] = {
            "raw_first_5": self.raw_df.head().to_dict(orient='records'),
            "cleaned_first_5": self.cleaned_df.head().to_dict(orient='records')
        }
        
        print("\n✅ 报告生成完成!")
        print(f"报告包含 {len(self.report)} 个部分")
        
        # 在控制台显示简要报告
        self._print_console_summary()
        
        return self.report
    
    def _print_console_summary(self):
        """在控制台打印简要报告"""
        print("\n📊 数据质量简要报告:")
        print("-" * 40)
        
        # 数据概况
        if "summary" in self.report:
            summary = self.report["summary"]
            print(f"📈 数据概况:")
            print(f"  原始数据: {summary['data_overview']['raw_data_shape'][0]} 行, "
                  f"{summary['data_overview']['raw_data_shape'][1]} 列")
            print(f"  清洗后数据: {summary['data_overview']['cleaned_data_shape'][0]} 行, "
                  f"{summary['data_overview']['cleaned_data_shape'][1]} 列")
        
        # 缺失值情况
        if "missing_analysis" in self.report:
            missing = self.report["missing_analysis"]
            print(f"\n⚠️  缺失值情况:")
            print(f"  原始数据缺失率: {missing['raw_data']['missing_percentage']}")
            print(f"  清洗后缺失率: {missing['cleaned_data']['missing_percentage']}")
            print(f"  修复率: {missing['summary']['improvement_percentage']}")
        
        # 异常值情况
        if "outlier_analysis" in self.report:
            outlier = self.report["outlier_analysis"]
            if "detected_outliers" in outlier:
                print(f"\n🚨 异常值情况:")
                print(f"  检测到异常值: {outlier['detected_outliers']['count']} 个")
                print(f"  异常值比例: {outlier['detected_outliers']['percentage']}")
        
        # 生成时间
        if "summary" in self.report:
            print(f"\n🕐 报告生成时间: {self.report['summary']['report_generated_at']}")
        
        print("="*60)
    
    def save_report(self, 
                   format: str = "txt", 
                   path: str = "./reports/quality_report.txt"):
        """
        保存报告到文件
        
        Args:
            format: 文件格式 (txt, json, markdown)
            path: 文件保存路径
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        if format == "txt":
            self._save_as_txt(path)
        elif format == "json":
            self._save_as_json(path)
        elif format == "markdown":
            self._save_as_markdown(path)
        else:
            raise ValueError(f"不支持的格式: {format}，请使用 txt, json 或 markdown")
        
        print(f"📄 报告已保存到: {path}")
    
    def _save_as_txt(self, path: str):
        """保存为文本文件"""
        with open(path, 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("气象数据质量报告\n")
            f.write("="*60 + "\n\n")
            
            # 数据概况
            if "summary" in self.report:
                summary = self.report["summary"]
                f.write("1. 数据概况\n")
                f.write("-"*40 + "\n")
                f.write(f"生成时间: {summary['report_generated_at']}\n")
                f.write(f"原始数据形状: {summary['data_overview']['raw_data_shape']}\n")
                f.write(f"清洗后数据形状: {summary['data_overview']['cleaned_data_shape']}\n")
                f.write(f"数据列: {', '.join(summary['data_overview']['columns'])}\n\n")
            
            # 基础统计
            if "basic_stats" in self.report:
                f.write("2. 基础统计分析\n")
                f.write("-"*40 + "\n")
                
                for data_type, stats in self.report["basic_stats"].items():
                    f.write(f"\n{data_type}:\n")
                    for key, value in stats.items():
                        if value is not None:
                            f.write(f"  {key}: {value}\n")
                f.write("\n")
            
            # 缺失值分析
            if "missing_analysis" in self.report:
                f.write("3. 缺失值分析\n")
                f.write("-"*40 + "\n")
                
                missing = self.report["missing_analysis"]
                f.write(f"\n原始数据:\n")
                for key, value in missing["raw_data"].items():
                    if key != "gap_lengths":
                        f.write(f"  {key}: {value}\n")
                
                f.write(f"\n清洗后数据:\n")
                for key, value in missing["cleaned_data"].items():
                    if key != "gap_lengths":
                        f.write(f"  {key}: {value}\n")
                
                f.write(f"\n处理摘要:\n")
                for key, value in missing["summary"].items():
                    f.write(f"  {key}: {value}\n")
                f.write("\n")
            
            # 异常值分析
            if "outlier_analysis" in self.report:
                f.write("4. 异常值分析\n")
                f.write("-"*40 + "\n")
                
                outlier = self.report["outlier_analysis"]
                if "detected_outliers" in outlier:
                    for key, value in outlier["detected_outliers"].items():
                        f.write(f"  {key}: {value}\n")
                f.write(f"检测方法: {outlier.get('method', 'N/A')}\n\n")
            
            f.write("="*60 + "\n")
            f.write("报告结束\n")
            f.write("="*60 + "\n")
    
    def _save_as_json(self, path: str):
        """保存为JSON文件"""
        import json
        
        # 转换不能序列化的对象
        def default_serializer(obj):
            if isinstance(obj, (np.integer, np.floating)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            return str(obj)
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, indent=2, default=default_serializer, ensure_ascii=False)
    
    def _save_as_markdown(self, path: str):
        """保存为Markdown文件"""
        with open(path, 'w', encoding='utf-8') as f:
            f.write("# 气象数据质量报告\n\n")
            
            # 数据概况
            if "summary" in self.report:
                summary = self.report["summary"]
                f.write(f"**生成时间**: {summary['report_generated_at']}\n\n")
                
                f.write("## 1. 数据概况\n\n")
                f.write(f"- **原始数据**: {summary['data_overview']['raw_data_shape'][0]} 行 × {summary['data_overview']['raw_data_shape'][1]} 列\n")
                f.write(f"- **清洗后数据**: {summary['data_overview']['cleaned_data_shape'][0]} 行 × {summary['data_overview']['cleaned_data_shape'][1]} 列\n")
                f.write(f"- **数据列**: {', '.join(summary['data_overview']['columns'])}\n\n")
            
            # 基础统计
            if "basic_stats" in self.report:
                f.write("## 2. 基础统计分析\n\n")
                
                for data_type, stats in self.report["basic_stats"].items():
                    f.write(f"### {data_type.replace('_', ' ').title()}\n\n")
                    f.write("| 指标 | 值 |\n")
                    f.write("|------|----|\n")
                    for key, value in stats.items():
                        if value is not None:
                            f.write(f"| {key} | {value} |\n")
                    f.write("\n")
            
            # 缺失值分析
            if "missing_analysis" in self.report:
                f.write("## 3. 缺失值分析\n\n")
                
                missing = self.report["missing_analysis"]
                f.write("### 原始数据\n\n")
                f.write("| 指标 | 值 |\n")
                f.write("|------|----|\n")
                for key, value in missing["raw_data"].items():
                    if key != "gap_lengths":
                        f.write(f"| {key} | {value} |\n")
                
                f.write("\n### 清洗后数据\n\n")
                f.write("| 指标 | 值 |\n")
                f.write("|------|----|\n")
                for key, value in missing["cleaned_data"].items():
                    if key != "gap_lengths":
                        f.write(f"| {key} | {value} |\n")
                
                f.write("\n### 处理摘要\n\n")
                f.write("| 指标 | 值 |\n")
                f.write("|------|----|\n")
                for key, value in missing["summary"].items():
                    f.write(f"| {key} | {value} |\n")
                f.write("\n")
            
            # 异常值分析
            if "outlier_analysis" in self.report:
                f.write("## 4. 异常值分析\n\n")
                
                outlier = self.report["outlier_analysis"]
                f.write(f"**检测方法**: {outlier.get('method', 'N/A')}\n\n")
                
                if "detected_outliers" in outlier:
                    f.write("| 指标 | 值 |\n")
                    f.write("|------|----|\n")
                    for key, value in outlier["detected_outliers"].items():
                        f.write(f"| {key} | {value} |\n")
                    f.write("\n")
            
            f.write("---\n")
            f.write("*报告结束*\n")


# 便捷函数
def generate_quality_report(raw_df: pd.DataFrame, 
                           cleaned_df: pd.DataFrame,
                           column: str = 'temperature',
                           outlier_mask: Optional[pd.Series] = None,
                           save_format: str = 'txt',
                           save_path: str = './reports/quality_report.txt') -> QualityReportGenerator:
    """
    快速生成并保存质量报告的便捷函数
    
    Returns:
        报告生成器实例
    """
    # 创建报告生成器
    report_gen = QualityReportGenerator(raw_df, cleaned_df)
    
    # 生成完整报告
    report_gen.generate_full_report(column=column, outlier_mask=outlier_mask)
    
    # 保存报告
    report_gen.save_report(format=save_format, path=save_path)
    
    return report_gen


if __name__ == "__main__":
    # 1. 读取你的真实数据
    data_path = "../../demo_data.csv"  # 从 src/date 退两层
    print(f"正在读取数据: {data_path}")
    df = pd.read_csv(data_path)
    print(f"数据形状: {df.shape}")
    
    # 2. 简单清洗（示例）
    df_cleaned = df.copy()
    if 'temperature' in df_cleaned.columns:
        # 用均值填充温度缺失值
        mean_temp = df_cleaned['temperature'].mean()
        df_cleaned['temperature'].fillna(mean_temp, inplace=True)
    
    # 3. 创建报告生成器
    generator = QualityReportGenerator(raw_df=df, cleaned_df=df_cleaned)
    
    # 4. 生成报告
    generator.generate_full_report(column='temperature')
    
    # 5. 保存报告
    generator.save_report(format='txt', path='../../reports/quality_report.txt')
    
    print("\n✅ 真实数据处理完成！")