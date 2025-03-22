import pickle
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

def read_monthly_pickle(
    symbol: str,
    year: int,
    month: int,
    interval: str = "1min"
) -> Optional[pd.DataFrame]:
    """
    指定した年月のPickleファイルを読み込む関数

    Args:
        symbol (str): 仮想通貨のシンボル（例：'BTC'）
        year (int): 年
        month (int): 月
        interval (str, optional): データの間隔. Defaults to "1min".

    Returns:
        Optional[pd.DataFrame]: 読み込んだデータフレーム。ファイルが存在しない場合はNone。
    """
    # ファイルパスの生成
    filepath = Path("src") / "data" / symbol / interval / "months" / f"{year:04d}{month:02d}.pkl"
    
    try:
        # Pickleファイルを読み込む
        with open(filepath, "rb") as f:
            df = pickle.load(f)
        
        print(f"\n=== {year}年{month}月のデータ ===")
        # DataFrameの基本情報を表示
        print("\n=== DataFrameの基本情報 ===")
        print(df.info())
        
        # 最初の5行を表示
        print("\n=== 最初の5行 ===")
        print(df.head())
        
        # 最後の5行を表示
        print("\n=== 最後の5行 ===")
        print(df.tail())
        
        # 基本統計量を表示
        print("\n=== 基本統計量 ===")
        print(df.describe())
        
        return df
        
    except FileNotFoundError:
        print(f"エラー: {year}年{month}月のデータファイルが見つかりません")
        print(f"探索したパス: {filepath}")
        return None
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return None

if __name__ == "__main__":
    # 使用例
    symbol = "BTC"
    year = 2024
    month = 2
    
    df = read_monthly_pickle(symbol, year, month)
    if df is not None:
        print(f"\nデータの行数: {len(df)}") 