import numpy as np
import pandas as pd

# 直接把quality_check.py里的类定义复制到这里
class ThreeSigmaDetector:
    def __init__(self, sigma_level: int = 3):
        self.sigma_level = sigma_level
        self.thresholds = {}
    
    def fit(self, data_series: pd.Series) -> None:
        mean = data_series.mean()
        std = data_series.std()
        self.thresholds = {
            'mean': mean, 'std': std,
            'upper_bound': mean + self.sigma_level * std,
            'lower_bound': mean - self.sigma_level * std
        }
    
    def detect(self, data_series: pd.Series) -> pd.Series:
        if not self.thresholds:
            self.fit(data_series)
        anomalies = (data_series < self.thresholds['lower_bound']) | \
                   (data_series > self.thresholds['upper_bound'])
        return anomalies
    
    def generate_report(self, data_series: pd.Series):
        anomalies = self.detect(data_series)
        return {
            'total_samples': len(data_series),
            'anomaly_count': anomalies.sum(),
            'anomaly_rate': anomalies.mean(),
            'thresholds': self.thresholds,
            'anomaly_indices': data_series[anomalies].index.tolist()
        }

# 测试代码
if __name__ == "__main__":
    test_data = pd.Series([15, 16, 18, 17, 16, 55, -25, 19, 18])
    print(f"测试数据: {test_data.tolist()}")

    detector = ThreeSigmaDetector(sigma_level=2)  # 修正拼写，只创建一次
    report = detector.generate_report(test_data)

    print(f"数据均值: {report['thresholds']['mean']:.2f}")
    print(f"数据标准差: {report['thresholds']['std']:.2f}")
    print(f"异常阈值: [{report['thresholds']['lower_bound']:.2f}, {report['thresholds']['upper_bound']:.2f}]")  # 修正字段名
    print(f"异常索引: {report['anomaly_indices']}")
    print(f"异常值: {test_data[report['anomaly_indices']].tolist()}")