"""
演示数据生成器 - 为校赛答辩创建有说服力的测试数据
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

print("=" * 60)
print("气象演示数据生成器")
print("=" * 60)

# 生成1000行数据
n_rows = 1000
timestamps = [datetime(2025, 1, 1, 0, 0) + timedelta(hours=i) for i in range(n_rows)]

print(f"生成 {n_rows} 行演示数据...")

# 创建基础数据（合理范围）
np.random.seed(42)  # 固定随机种子，确保可重复
df = pd.DataFrame({
    'timestamp': timestamps,
    'temperature': 15 + np.random.randn(n_rows) * 5,    # 平均15°C
    'humidity': 65 + np.random.randn(n_rows) * 15,      # 平均65%
    'pressure': 1013 + np.random.randn(n_rows) * 10,    # 平均1013hPa
    'wind_speed': 3 + np.random.exponential(2, n_rows)  # 平均3m/s
})

print("✅ 基础数据生成完成")

# === 插入数据质量问题 ===

# 1. 缺失值（约8%）
missing_mask = np.random.rand(n_rows) < 0.08
df.loc[missing_mask, 'temperature'] = np.nan
df.loc[np.random.rand(n_rows) < 0.05, 'humidity'] = np.nan
df.loc[np.random.rand(n_rows) < 0.03, 'pressure'] = np.nan

print(f"✅ 插入缺失值: {df.isna().sum().sum()} 个")

# 2. 明显异常值
df.loc[50, 'temperature'] = 999.0    # 异常高温
df.loc[150, 'temperature'] = -50.0   # 异常低温  
df.loc[250, 'humidity'] = 150.0      # 超100%湿度
df.loc[350, 'humidity'] = -10.0      # 负湿度
df.loc[450, 'pressure'] = 2000.0     # 异常高压
df.loc[550, 'wind_speed'] = 999.0    # 异常风速

print("✅ 插入6个明显异常值")

# 3. 重复行（3组重复）
df = pd.concat([df, df.iloc[[100, 200, 300]]], ignore_index=True)

print("✅ 插入3组重复数据")

# 4. 时间戳错乱（1处）
df.loc[600, 'timestamp'] = df.loc[600, 'timestamp'] - timedelta(days=365)

print("✅ 插入1处时间戳错乱")

# 保存文件
output_path = "demo_data.csv"
df.to_csv(output_path, index=False)

print("=" * 60)
print("🎉 演示数据生成完成！")
print("=" * 60)
print(f"文件位置: {os.path.abspath(output_path)}")
print(f"数据形状: {df.shape}")
print(f"总行数: {len(df)}")
print(f"总列数: {len(df.columns)}")
print("\n📊 数据质量概况:")
print(f"  缺失值总数: {df.isna().sum().sum()}")
print(f"  温度缺失: {df['temperature'].isna().sum()}")
print(f"  湿度缺失: {df['humidity'].isna().sum()}")
print(f"  数据列: {', '.join(df.columns)}")
print("=" * 60)
print("💡 此文件专为校赛答辩设计，包含多种真实数据问题")
print("   用于充分展示'原始天擎'管道的清洗能力")
print("=" * 60)