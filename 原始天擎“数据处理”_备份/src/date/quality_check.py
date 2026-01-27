import numpy as np
import pandas as pd
from typing import Dict, Tuple

class ThreeSigmaDetector:
    """3σ异常检测器 - 天擎系统质检核心"""
    
    def __init__(self, sigma_level: int = 3):
        self.sigma_level = sigma_level
        self.thresholds = {}
        
    def fit(self, data_series: pd.Series) -> None:
        """计算数据的3σ阈值"""
        mean = data_series.mean()
        std = data_series.std()
        
        self.thresholds = {
            'mean': mean,
            'std': std,
            'upper_bound': mean + self.sigma_level * std,
            'lower_bound': mean - self.sigma_level * std
        }
    
    def detect(self, data_series: pd.Series) -> pd.Series:
        """检测异常值"""
        if not self.thresholds:
            self.fit(data_series)
            
        anomalies = (data_series < self.thresholds['lower_bound']) | \
                   (data_series > self.thresholds['upper_bound'])
        return anomalies
    
    def generate_report(self, data_series: pd.Series) -> Dict:
        """生成质检报告"""
        anomalies = self.detect(data_series)
        
        return {
            'total_samples': len(data_series),
            'anomaly_count': anomalies.sum(),
            'anomaly_rate': anomalies.mean(),
            'thresholds': self.thresholds,
            'anomaly_indices': data_series[anomalies].index.tolist()
        }