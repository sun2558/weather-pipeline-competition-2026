import pandas as pd
import os

def load_weather_data(file_path):
    """
    加载气象数据文件 (工程增强版)
    参数:
        file_path (str):数据文件的路径 (支持.csv或..xlsx)
    返回:
        pandas.DataFrame:加载后的数据框
    异常：
        会抛出清晰的错误信息,帮助快速定位问题
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"错误：文件'{file_path}'不存在")
    try:
        # === 你原有的核心逻辑 ===
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("不支持的文件格式! 请提供 .csv 或 .xlsx 文件。")
        # === 你的原始逻辑结束 ===
    except pd.errors.EmptyDataError:
         raise pd.errors.EmptyDataError(f"文件 '{file_path}' 为空，请检查文件内容。")
    except Exception as e:
        raise type(e)(f"读取文件 '{file_path}' 时出错: {str(e)}")
    print(f"数据加载成功！数据集形状：{df.shape}")

    return df 

if __name__=="__main__":
    test_file = r"C:\VS code\code\sky-support-system\data\raw\test_sample.csv"
    try:
        data = load_weather_data(test_file)
        print("\n=== 模块测试通过！===")
        print("数据预览：")
        print(data.head())
    except Exception as e:
        print(f"\n!!! 模块测试失败: {e} !!!")