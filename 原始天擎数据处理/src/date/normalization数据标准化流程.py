# normalization.py - 数据标准化模块
import pandas as pd
import numpy as np

def zscore_normalize(data, columns=None):
    """
    Z-Score标准化:将数据转换为均值为0,标准差为1的分布
    
    适用场景：数据符合正态分布，或有离群点的情况
    公式：(x - mean) / std
    """
    # 如果没有指定列，自动选择所有数值型列
    if columns is None:
        columns = data.select_dtypes(include=[np.number]).columns
    
    result = data.copy()
    
    for col in columns:
        if col in data.columns:
            mean_val = data[col].mean()  # 计算均值
            std_val = data[col].std()    # 计算标准差
            
            # 避免除零错误（当标准差为0时，所有值相同）
            if std_val != 0:
                result[col] = (data[col] - mean_val) / std_val
            else:
                # 所有值相同的情况，统一设为0
                result[col] = 0
    
    return result


def minmax_normalize(data, columns=None):
    """
    Min-Max标准化:将数据缩放到[0,1]区间
    
    适用场景：数据有明确边界，需要保留原始分布形态
    公式：(x - min) / (max - min)
    """
    # 如果没有指定列，自动选择所有数值型列
    if columns is None:
        columns = data.select_dtypes(include=[np.number]).columns
    
    result = data.copy()
    
    for col in columns:
        if col in data.columns:
            min_val = data[col].min()    # 计算最小值
            max_val = data[col].max()    # 计算最大值
            range_val = max_val - min_val  # 计算极差
            
            # 避免除零错误（当所有值相同时，极差为0）
            if range_val != 0:
                result[col] = (data[col] - min_val) / range_val
            else:
                # 所有值相同的情况，统一设为0
                result[col] = 0
    
    return result