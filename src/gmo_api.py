import requests
import pandas as pd
import pickle
from datetime import datetime, timedelta
import os
from typing import Optional
from tqdm import tqdm
import time
import numpy as np

def fetch_ohlcv_data(
    symbol: str,
    start_date: datetime,
    end_date: datetime,
    interval: str = "1min"
) -> None:
    """
    GMOコインのAPIを使用してOHLCVデータを取得し、Pickle形式で保存する関数
    
    注意: GMOコインのKLine APIは、指定した日付の午前6時(JST)から翌日の午前5:59分(JST)までのデータを返します。
    """
    base_url = "https://api.coin.z.com/public/v1"
    
    # 総日数を計算
    total_days = (end_date - start_date).days + 1
    json_list = []
    
    current_date = start_date
    for _ in tqdm(range(total_days), desc="データ取得進捗", unit="日"):
        date_str = current_date.strftime("%Y%m%d")
        
        # APIリクエストのパラメータ
        params = {
            "symbol": symbol,
            "interval": interval,
            "date": date_str
        }
        
        try:
            # APIリクエストを実行
            tqdm.write(f"{date_str}のデータを取得中...")
            response = requests.get(f"{base_url}/klines", params=params)
            response.raise_for_status()  # エラーチェック
            
            # レスポンスデータを取得
            data = response.json()
            
            if "data" not in data or not data["data"]:
                tqdm.write(f"{date_str}のデータが存在しません")
            else:
                # 最初のデータの形式を表示
                tqdm.write(f"データ形式: {data['data'][0]}")
                json_list.extend(data["data"])
                tqdm.write(f"{date_str}のデータを追加しました")
            
            # API制限を考慮して少し待機（1秒）
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            tqdm.write(f"{date_str}のデータ取得中にエラーが発生しました: {e}")
        
        # 次の日付に進む
        current_date += timedelta(days=1)
    
    if not json_list:
        raise ValueError("取得したデータがありません")
    
    # データをDataFrameに変換
    df = pd.DataFrame(json_list, columns=[
        "openTime", "open", "high", "low", "close", "volume"
    ])
    
    # データ型の変換
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    
    # openTimeをJST（UTC+9）のdatetime型に変換
    temp_df = df.copy()
    temp_df['openTime'] = pd.to_datetime(temp_df['openTime'].astype(np.int64), unit='ms', utc=True)
    temp_df['openTime'] = temp_df['openTime'].dt.tz_convert('Asia/Tokyo')
    
    # 6時間オフセットを適用して日付を調整（午前6時を日付の区切りとする）
    temp_df['adjusted_date'] = temp_df['openTime'] - pd.Timedelta(hours=6)
    
    # 調整後の日付で月ごとにグループ化
    for name, group_indices in temp_df.groupby(temp_df['adjusted_date'].dt.to_period('M')).groups.items():
        # 元のデータ型のままのデータを抽出
        month_data = df.iloc[group_indices]
        
        # 保存先ディレクトリの作成
        save_dir = os.path.join("src", "data", symbol, interval, "months")
        os.makedirs(save_dir, exist_ok=True)
        
        # ファイル名の生成（YYYYMM形式）
        filename = f"{name.strftime('%Y%m')}.pkl"
        filepath = os.path.join(save_dir, filename)
        
        # Pickle形式で保存
        with open(filepath, "wb") as f:
            pickle.dump(month_data, f)
        
        print(f"{name.strftime('%Y年%m月')}のデータを {filepath} に保存しました（JST基準、午前6時始まり）")

# 使用例
if __name__ == "__main__":
    # テスト用の日時を設定
    start = datetime(2024, 1, 1)
    end = datetime(2024, 2, 1)
    
    # BTCのデータを取得
    fetch_ohlcv_data("BTC", start, end) 