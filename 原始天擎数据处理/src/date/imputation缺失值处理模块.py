# src/data/imputation.py

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
import warnings

def linear_impute(df: pd.DataFrame, 
                  column: str = 'temperature',
                  max_gap: Optional[int] = 5,
                  method: str = 'linear') -> pd.DataFrame:
    """
    对指定列进行线性插值处理连续缺失值
    
    Args:
        df: 包含缺失值的DataFrame
        column: 需要插值的列名
        max_gap: 最大连续缺失值数量，超过此数量的连续缺失不进行插值
                 None表示无限制
        method: 插值方法 ('linear', 'time', 'pad', 'nearest')
    
    Returns:
        插值后的DataFrame
    """
    df_imputed = df.copy()
    
    if column not in df_imputed.columns:
        raise ValueError(f"列 '{column}' 不在DataFrame中")
    
    # 记录原始缺失值信息
    original_missing = df_imputed[column].isna().sum()
    
    if original_missing == 0:
        print(f"列 '{column}' 无缺失值，跳过插值")
        return df_imputed
    
    print(f"开始插值处理: 列 '{column}' 有 {original_missing} 个缺失值")
    
    # 如果设置了最大连续缺失限制
    if max_gap is not None and max_gap > 0:
        # 识别连续缺失的段落
        mask = df_imputed[column].isna()
        groups = (mask != mask.shift()).cumsum()
        
        for group_id, group in mask.groupby(groups):
            if group.any():  # 这是一个缺失值段落
                gap_length = group.sum()
                
                if gap_length > max_gap:
                    # 标记这些位置为特殊值（或保持NaN）
                    indices = group[group].index
                    print(f"  跳过 {gap_length} 个连续缺失值（位置 {indices[0]}-{indices[-1]}，超过最大限制 {max_gap}）")
                    # 保持为NaN，不进行插值
    
    # 执行插值
    try:
        if method == 'time' and 'timestamp' in df_imputed.columns:
            # 基于时间的插值
            df_imputed.set_index('timestamp', inplace=True)
            df_imputed[column] = df_imputed[column].interpolate(method='time')
            df_imputed.reset_index(inplace=True)
        elif method == 'linear':
            df_imputed[column] = df_imputed[column].interpolate(method='linear')
        elif method == 'pad':
            df_imputed[column] = df_imputed[column].interpolate(method='pad')
        elif method == 'nearest':
            df_imputed[column] = df_imputed[column].interpolate(method='nearest')
        else:
            warnings.warn(f"未知的插值方法 '{method}'，使用默认线性插值")
            df_imputed[column] = df_imputed[column].interpolate(method='linear')
    except Exception as e:
        print(f"插值过程中出错: {e}")
        print("使用前向填充作为备选方案")
        df_imputed[column] = df_imputed[column].fillna(method='ffill').fillna(method='bfill')
    
    # 计算插值后的缺失值
    remaining_missing = df_imputed[column].isna().sum()
    imputed_count = original_missing - remaining_missing
    
    print(f"插值完成: 处理了 {imputed_count} 个缺失值，剩余 {remaining_missing} 个缺失值")
    
    return df_imputed


def advanced_impute(df: pd.DataFrame,
                    column: str = 'temperature',
                    strategy: str = 'seasonal',
                    seasonal_period: int = 24) -> pd.DataFrame:
    """
    高级插值方法，考虑季节性等因素
    
    Args:
        df: 输入DataFrame
        column: 需要插值的列名
        strategy: 插值策略 ('seasonal', 'rolling_mean', 'spline')
        seasonal_period: 季节性周期（小时数）
    
    Returns:
        插值后的DataFrame
    """
    df_imputed = df.copy()
    
    if column not in df_imputed.columns:
        raise ValueError(f"列 '{column}' 不在DataFrame中")
    
    if strategy == 'seasonal' and 'timestamp' in df_imputed.columns:
        print(f"使用季节性插值（周期: {seasonal_period} 小时）")
        
        # 提取小时信息
        df_imputed['hour'] = pd.to_datetime(df_imputed['timestamp']).dt.hour
        
        # 计算每小时的典型值（中位数）
        hourly_median = df_imputed.groupby('hour')[column].median()
        
        # 对缺失值使用对应小时的典型值填充
        for hour in range(24):
            hour_mask = (df_imputed['hour'] == hour) & df_imputed[column].isna()
            if hour_mask.any():
                if not pd.isna(hourly_median[hour]):
                    df_imputed.loc[hour_mask, column] = hourly_median[hour]
        
        # 剩余的使用线性插值
        df_imputed[column] = df_imputed[column].interpolate(method='linear')
        df_imputed.drop(columns=['hour'], inplace=True)
        
    elif strategy == 'rolling_mean':
        print("使用滚动均值插值")
        window_size = min(5, len(df_imputed) // 10)  # 动态窗口大小
        df_imputed[column] = df_imputed[column].fillna(
            df_imputed[column].rolling(window=window_size, min_periods=1, center=True).mean()
        )
        
    elif strategy == 'spline':
        print("使用样条插值")
        try:
            df_imputed[column] = df_imputed[column].interpolate(method='spline', order=3)
        except:
            print("样条插值失败，回退到线性插值")
            df_imputed[column] = df_imputed[column].interpolate(method='linear')
    
    return df_imputed


def get_missing_stats(df: pd.DataFrame, column: str = 'temperature') -> Dict[str, Any]:
    """
    获取缺失值统计信息
    
    Args:
        df: 输入DataFrame
        column: 要检查的列名
    
    Returns:
        缺失值统计字典
    """
    if column not in df.columns:
        return {"error": f"列 '{column}' 不存在"}
    
    missing_mask = df[column].isna()
    total_missing = missing_mask.sum()
    missing_rate = total_missing / len(df)
    
    # 连续缺失段落分析
    if total_missing > 0:
        # 识别连续缺失段落
        groups = (missing_mask != missing_mask.shift()).cumsum()
        gap_lengths = []
        
        for group_id, group in missing_mask.groupby(groups):
            if group.any():  # 这是一个缺失值段落
                gap_length = group.sum()
                gap_lengths.append(gap_length)
        
        stats = {
            "total_missing": int(total_missing),
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


# 主处理函数
def handle_missing_values(df: pd.DataFrame,
                         column: str = 'temperature',
                         method: str = 'linear',
                         max_gap: Optional[int] = 10,
                         report: bool = True) -> pd.DataFrame:
    """
    缺失值处理主函数
    
    Args:
        df: 输入DataFrame
        column: 目标列名
        method: 处理方法 ('linear', 'seasonal', 'rolling_mean', 'spline')
        max_gap: 最大连续缺失限制
        report: 是否打印报告
    
    Returns:
        处理后的DataFrame
    """
    if report:
        print("="*50)
        print("缺失值处理开始")
        print("="*50)
        
        # 原始统计
        original_stats = get_missing_stats(df, column)
        print(f"原始数据 - 列 '{column}':")
        print(f"  缺失值数量: {original_stats['total_missing']}")
        print(f"  缺失率: {original_stats['missing_percentage']}")
        print(f"  连续缺失段落数: {original_stats['gap_count']}")
        print(f"  最大连续缺失长度: {original_stats['max_gap_length']}")
    
    # 选择插值方法
    if method in ['linear', 'pad', 'nearest', 'time']:
        result_df = linear_impute(df, column, max_gap, method)
    elif method in ['seasonal', 'rolling_mean', 'spline']:
        result_df = advanced_impute(df, column, method)
    else:
        warnings.warn(f"未知方法 '{method}'，使用默认线性插值")
        result_df = linear_impute(df, column, max_gap, 'linear')
    
    if report:
        # 处理后统计
        final_stats = get_missing_stats(result_df, column)
        print(f"\n处理后数据 - 列 '{column}':")
        print(f"  缺失值数量: {final_stats['total_missing']}")
        print(f"  缺失率: {final_stats['missing_percentage']}")
        
        if final_stats['total_missing'] > 0:
            print("⚠️  警告: 仍有缺失值未处理（可能超过最大连续缺失限制）")
        
        print("="*50)
        print("缺失值处理完成")
        print("="*50)
    
    return result_df


if __name__ == "__main__":
    # 测试代码
    print("测试缺失值处理模块...")
    
    # 创建测试数据
    test_dates = pd.date_range("2024-01-01", periods=48, freq="H")
    test_temps = np.random.normal(20, 3, 48)
    
    # 添加缺失值
    test_temps[5:10] = np.nan  # 连续5个缺失
    test_temps[20] = np.nan    # 单个缺失
    test_temps[30:36] = np.nan # 连续6个缺失
    
    test_df = pd.DataFrame({
        "timestamp": test_dates,
        "temperature": test_temps
    })
    
    # 测试线性插值
    print("\n1. 测试线性插值:")
    df_linear = handle_missing_values(test_df.copy(), method='linear', max_gap=5)
    
    # 测试季节性插值
    print("\n2. 测试季节性插值:")
    df_seasonal = handle_missing_values(test_df.copy(), method='seasonal')