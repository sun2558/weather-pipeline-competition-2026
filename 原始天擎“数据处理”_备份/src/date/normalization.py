# normalization.py - 绝对干净版
import pandas as pd
import numpy as np

def zscore_normalize(data, columns=None):
    if columns is None:
        columns = data.select_dtypes(include=[np.number]).columns
    
    result = data.copy()
    
    for col in columns:
        if col in data.columns:
            mean_val = data[col].mean()
            std_val = data[col].std()
            if std_val != 0:
                result[col] = (data[col] - mean_val) / std_val
            else:
                result[col] = 0
    
    return result

def minmax_normalize(data, columns=None):
    if columns is None:
        columns = data.select_dtypes(include=[np.number]).columns
    
    result = data.copy()
    
    for col in columns:
        if col in data.columns:
            min_val = data[col].min()
            max_val = data[col].max()
            range_val = max_val - min_val
            if range_val != 0:
                result[col] = (data[col] - min_val) / range_val
            else:
                result[col] = 0
    
    return result