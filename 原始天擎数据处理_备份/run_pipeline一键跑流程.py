# run_pipeline.py - 校赛最终优化版
import os
import sys
import pandas as pd
import importlib.util
from datetime import datetime

print("=" * 60)
print("气象数据处理管道 - 校赛演示版")
print("=" * 60)

# 项目根目录
current_dir = os.path.dirname(os.path.abspath(__file__))
print(f"项目根目录: {current_dir}")

# src/date 目录
src_date_dir = os.path.join(current_dir, "src", "date")
print(f"模块目录: {src_date_dir}")

if not os.path.exists(src_date_dir):
    print("❌ 错误: 找不到 src/date/ 目录")
    sys.exit(1)

# 检查文件
print("\n📁 检查模块文件...")
module_files = {
    "loader": "loader.py",
    "imputation": "imputation.py", 
    "quality_check": "quality_check.py",
    "report_generator": "report_generator.py"
}

modules = {}
for name, filename in module_files.items():
    path = os.path.join(src_date_dir, filename)
    if os.path.exists(path):
        print(f"  ✅ {filename}: 存在")
        modules[name] = path
    else:
        print(f"  ❌ {filename}: 不存在")
        # 如果关键模块缺失，尝试创建简单版本
        if filename in ["quality_check.py", "report_generator.py"]:
            print(f"    警告: {filename} 缺失，但本版本已内置相关功能")

print("\n" + "=" * 60)
print("开始导入模块...")
print("=" * 60)

# 导入模块函数
def import_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

try:
    # 导入可用的模块
    if "loader" in modules:
        loader = import_module("loader", modules["loader"])
        load_weather_data = loader.load_weather_data
    else:
        # 备用加载函数
        def load_weather_data(filepath):
            print(f"  使用备用加载器: {filepath}")
            return pd.read_csv(filepath)
    
    if "imputation" in modules:
        imputation = import_module("imputation", modules["imputation"])
        linear_impute = imputation.linear_impute
    else:
        # 备用插值函数
        def linear_impute(data, column='temperature', max_gap=5, method='linear'):
            print(f"  使用备用插值: {column}")
            return data.copy()
    
    # 内置标准化函数
    def zscore_normalize(data, columns=None):
        """Z-score标准化（内置版本）"""
        if columns is None:
            columns = data.select_dtypes(include=['number']).columns
        
        result = data.copy()
        for col in columns:
            if col in data.columns and data[col].notna().any():
                mean_val = data[col].mean()
                std_val = data[col].std()
                if std_val > 0:
                    result[col] = (data[col] - mean_val) / std_val
                else:
                    result[col] = 0
        return result
    
    # 内置异常检测
    def three_sigma_detect(data, columns=None, sigma=3):
        """3σ原则异常检测"""
        if columns is None:
            columns = data.select_dtypes(include=['number']).columns
        
        outlier_info = {}
        for col in columns:
            if col in data.columns and data[col].notna().any():
                col_data = data[col].dropna()
                if len(col_data) > 0:
                    mean_val = col_data.mean()
                    std_val = col_data.std()
                    if std_val > 0:
                        upper = mean_val + sigma * std_val
                        lower = mean_val - sigma * std_val
                        mask = (data[col] > upper) | (data[col] < lower)
                        outlier_info[col] = {
                            'mask': mask,
                            'count': mask.sum(),
                            'mean': mean_val,
                            'std': std_val,
                            'upper': upper,
                            'lower': lower
                        }
        return outlier_info
    
    # 内置报告生成
    def generate_quality_report(raw_df, cleaned_df, column=None, 
                               outlier_info=None, save_path="quality_report.txt"):
        """生成数据质量报告（内置版本）"""
        if column is None:
            numeric_cols = raw_df.select_dtypes(include=['number']).columns
            column = numeric_cols[0] if len(numeric_cols) > 0 else raw_df.columns[0]
        
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("气象数据质量分析报告 - 校赛演示版")
        report_lines.append("=" * 60)
        
        # 1. 数据概况
        report_lines.append("\n1. 数据概况")
        report_lines.append("-" * 40)
        report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"原始数据形状: {raw_df.shape}")
        report_lines.append(f"清洗后数据形状: {cleaned_df.shape}")
        report_lines.append(f"数据列: {', '.join(raw_df.columns.tolist())}")
        report_lines.append(f"主分析列: {column}")
        
        # 2. 基础统计
        report_lines.append("\n2. 基础统计分析")
        report_lines.append("-" * 40)
        
        if column in raw_df.columns:
            raw_col = raw_df[column].dropna()
            if len(raw_col) > 0:
                report_lines.append(f"\n[{column}] - 原始数据:")
                report_lines.append(f"  数量: {len(raw_col):,}")
                report_lines.append(f"  均值: {raw_col.mean():.4f}")
                report_lines.append(f"  标准差: {raw_col.std():.4f}")
                report_lines.append(f"  最小值: {raw_col.min():.4f}")
                report_lines.append(f"  最大值: {raw_col.max():.4f}")
        
        if column in cleaned_df.columns:
            cleaned_col = cleaned_df[column].dropna()
            if len(cleaned_col) > 0:
                report_lines.append(f"\n[{column}] - 清洗后数据:")
                report_lines.append(f"  数量: {len(cleaned_col):,}")
                report_lines.append(f"  均值: {cleaned_col.mean():.4f}")
                report_lines.append(f"  标准差: {cleaned_col.std():.4f}")
                report_lines.append(f"  最小值: {cleaned_col.min():.4f}")
                report_lines.append(f"  最大值: {cleaned_col.max():.4f}")
        
        # 3. 缺失值分析
        report_lines.append("\n3. 缺失值分析")
        report_lines.append("-" * 40)
        
        numeric_cols = raw_df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if col in raw_df.columns:
                missing_before = raw_df[col].isna().sum()
                missing_after = cleaned_df[col].isna().sum() if col in cleaned_df.columns else missing_before
                if missing_before > 0:
                    report_lines.append(f"\n[{col}]:")
                    report_lines.append(f"  原始缺失: {missing_before:,} 个 ({missing_before/len(raw_df)*100:.1f}%)")
                    report_lines.append(f"  清洗后缺失: {missing_after:,} 个")
                    report_lines.append(f"  修复数量: {missing_before - missing_after:,} 个")
        
        # 4. 异常值分析
        report_lines.append("\n4. 异常值分析")
        report_lines.append("-" * 40)
        
        if outlier_info:
            total_outliers = sum(info.get('count', 0) for info in outlier_info.values())
            report_lines.append(f"检测方法: 3σ原则")
            report_lines.append(f"总异常值数量: {total_outliers:,}")
            
            for col, info in outlier_info.items():
                if info.get('count', 0) > 0:
                    report_lines.append(f"\n[{col}]:")
                    report_lines.append(f"  异常值: {info['count']:,} 个")
                    report_lines.append(f"  检测阈值: [{info.get('lower', 0):.2f}, {info.get('upper', 0):.2f}]")
        
        # 5. 处理摘要
        report_lines.append("\n5. 处理摘要")
        report_lines.append("-" * 40)
        
        total_missing_before = raw_df[numeric_cols].isna().sum().sum()
        total_missing_after = cleaned_df[numeric_cols].isna().sum().sum()
        
        report_lines.append(f"处理数据总量: {len(raw_df):,} 行 × {len(numeric_cols)} 列")
        report_lines.append(f"修复缺失值: {total_missing_before - total_missing_after:,} 个")
        report_lines.append(f"数据质量提升: {(total_missing_before - total_missing_after)/max(total_missing_before, 1)*100:.1f}%")
        
        report_lines.append("\n" + "=" * 60)
        report_lines.append("报告结束")
        report_lines.append("=" * 60)
        
        # 保存报告
        report_content = "\n".join(report_lines)
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        return report_content
    
    print("✅ 所有模块加载成功（含内置功能）")
    
except Exception as e:
    print(f"❌ 模块加载失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==================== 数据加载策略 ====================
print("\n" + "=" * 60)
print("数据加载策略")
print("=" * 60)

# 1. 优先使用演示数据
demo_data_path = os.path.join(current_dir, "demo_data.csv")
# 2. 备用：原始测试数据
weather_data_path = os.path.join(current_dir, "data", "raw", "weather.csv")

data_path = None
data_source = ""

if os.path.exists(demo_data_path):
    data_path = demo_data_path
    data_source = "演示数据 (demo_data.csv)"
    print(f"📊 使用: {data_source}")
    print(f"   位置: {data_path}")
elif os.path.exists(weather_data_path):
    data_path = weather_data_path
    data_source = "示例数据 (weather.csv)"
    print(f"📊 使用: {data_source}")
    print(f"   位置: {data_path}")
    print("⚠️  提示: 建议运行 create_demo_data.py 生成更丰富的演示数据")
else:
    print("❌ 错误: 找不到任何数据文件")
    print(f"   检查: {demo_data_path}")
    print(f"   或: {weather_data_path}")
    sys.exit(1)

print("\n" + "=" * 60)
print("开始处理数据...")
print("=" * 60)

# 1. 加载数据
print(f"\n[1/5] 加载数据 ({data_source})...")
try:
    raw_df = pd.read_csv(data_path) if 'loader' not in modules else load_weather_data(data_path)
    print(f"   ✅ 加载成功: {raw_df.shape[0]:,} 行 × {raw_df.shape[1]} 列")
    
    # 确定数值列
    numeric_cols = raw_df.select_dtypes(include=['number']).columns.tolist()
    if len(numeric_cols) == 0:
        print("   ⚠️  警告: 数据中没有数值列，使用所有列")
        numeric_cols = raw_df.columns.tolist()
    
    # 主分析列（用于详细报告）
    target_col = numeric_cols[0] if len(numeric_cols) > 0 else raw_df.columns[0]
    
    print(f"   处理列: {', '.join(numeric_cols[:3])}{'...' if len(numeric_cols) > 3 else ''}")
    print(f"   主分析列: {target_col}")
    
except Exception as e:
    print(f"   ❌ 加载失败: {e}")
    sys.exit(1)

# 2. 异常检测（多列）
print(f"\n[2/5] 异常检测 (3σ原则)...")
try:
    outlier_info = three_sigma_detect(raw_df, columns=numeric_cols, sigma=3)
    total_outliers = sum(info.get('count', 0) for info in outlier_info.values())
    
    print(f"   ✅ 检测完成:")
    print(f"      总异常值: {total_outliers:,} 个")
    
    # 显示异常值较多的列
    for col, info in outlier_info.items():
        if info.get('count', 0) > 0:
            print(f"      {col}: {info['count']:,} 个异常值")
    
    # 主分析列的掩码
    target_outlier_mask = outlier_info.get(target_col, {}).get('mask', None)
    
except Exception as e:
    print(f"   ❌ 异常检测失败: {e}")
    outlier_info = {}
    target_outlier_mask = None

# 3. 数据处理流程
cleaned_df = raw_df.copy()

# 4. 缺失值插值（逐列处理）
print(f"\n[3/5] 缺失值插值...")
try:
    missing_before_total = cleaned_df[numeric_cols].isna().sum().sum()
    
    for col in numeric_cols:
        if col in cleaned_df.columns:
            missing_before = cleaned_df[col].isna().sum()
            if missing_before > 0:
                # 使用线性插值
                if 'imputation' in modules:
                    cleaned_df = linear_impute(cleaned_df, column=col, max_gap=5, method='linear')
                else:
                    # 简单前向填充
                    cleaned_df[col] = cleaned_df[col].fillna(method='ffill').fillna(method='bfill')
    
    missing_after_total = cleaned_df[numeric_cols].isna().sum().sum()
    fixed_count = missing_before_total - missing_after_total
    
    print(f"   ✅ 插值完成:")
    print(f"      修复缺失值: {fixed_count:,} 个")
    print(f"      剩余缺失值: {missing_after_total:,} 个")
    
except Exception as e:
    print(f"   ❌ 插值失败: {e}")

# 5. 数据标准化
print(f"\n[4/5] 数据标准化 (Z-score)...")
try:
    cleaned_df = zscore_normalize(cleaned_df, columns=numeric_cols)
    print(f"   ✅ 标准化完成")
    print(f"      标准化列: {len(numeric_cols)} 个数值列")
except Exception as e:
    print(f"   ❌ 标准化失败: {e}")

# 6. 生成报告
print(f"\n[5/5] 生成质量报告...")
try:
    reports_dir = os.path.join(current_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    report_path = os.path.join(reports_dir, "quality_report.txt")
    
    if 'report_generator' in modules and modules.get('report_generator'):
        # 使用外部模块
        report_gen = import_module("report_generator", modules["report_generator"])
        report = report_gen.generate_quality_report(
            raw_df=raw_df,
            cleaned_df=cleaned_df,
            column=target_col,
            outlier_mask=target_outlier_mask,
            save_format="txt",
            save_path=report_path
        )
    else:
        # 使用内置函数
        report = generate_quality_report(
            raw_df=raw_df,
            cleaned_df=cleaned_df,
            column=target_col,
            outlier_info=outlier_info,
            save_path=report_path
        )
    
    print(f"   ✅ 报告生成成功！")
    print(f"      报告位置: {report_path}")
    
    # 显示报告摘要
    if os.path.exists(report_path):
        print(f"\n📄 报告摘要:")
        with open(report_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # 显示关键信息
            for i, line in enumerate(lines):
                if i < 15 or "缺失值分析" in line or "异常值分析" in line or "处理摘要" in line:
                    if line.strip():
                        print(f"      {line.rstrip()}")
                if i > 30:  # 只显示前面部分
                    break
    
except Exception as e:
    print(f"   ❌ 报告生成失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("🎉 数据处理管道执行完成！")
print("=" * 60)
print(f"📈 处理统计:")
print(f"   数据源: {data_source}")
print(f"   原始数据: {raw_df.shape[0]:,} 行 × {raw_df.shape[1]} 列")
print(f"   清洗后数据: {cleaned_df.shape[0]:,} 行 × {cleaned_df.shape[1]} 列")
print(f"   修复缺失值: {missing_before_total - missing_after_total:,} 个")
print(f"   检测异常值: {total_outliers:,} 个")
print(f"   处理时间: {datetime.now().strftime('%H:%M:%S')}")

if 'report_path' in locals() and os.path.exists(report_path):
    print(f"\n📁 完整报告: {report_path}")
    print(f"💡 提示: 使用 'cat {report_path}' 或文本编辑器查看完整报告")

print("=" * 60)