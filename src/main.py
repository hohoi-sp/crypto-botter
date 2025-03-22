import numpy as np
import pandas as pd
import requests

# ==================================================
# GMO コインから BTC の OHLCV データ取得
# ==================================================
symbol = "BTC"
interval = "1min"
date_str = "20250320"
end_point = "https://api.coin.z.com/public"
path = f"/v1/klines?symbol={symbol}&interval={interval}&date={date_str}"
url: str = end_point + path

# GMO コインから OHLCV データ取得
response = requests.get(url) # return: <Response [200]>
if response.status_code == 200:
  data: dict = response.json()
else:
  raise Exception(f"API リクエスト失敗: {response.status_code}")

# OHLCV データを DataFrame に変換
ohlcv_data: list = data["data"]
df = pd.DataFrame(ohlcv_data)
df.columns = ["openTime", "open", "high", "low", "close", "volume"]
df["close"] = df["close"].astype(float)  # 終値をfloat型に変換

# ==================================================
# クロスシグナル判定
# ==================================================
# 単純移動平均(SMA)の計算
short_window = 7 # 短期移動平均の期間
mid_window = 20 # 中期移動平均の期間

df["SMA_short"] = df["close"].rolling(window=short_window).mean()
df["SMA_mid"] = df["close"].rolling(window=mid_window).mean()
print(df.head(21))

# クロス判定ロジック
signals: list = []
for i in range(1, len(df)):
  # ゴールデンクロス(買いシグナル)
  if df['SMA_short'].iloc[i] > df['SMA_mid'].iloc[i] and df['SMA_short'].iloc[i-1] <= df['SMA_mid'].iloc[i-1]:
    signals.append("buy")
  # デッドクロス(売りシグナル) 
  elif df['SMA_short'].iloc[i] < df['SMA_mid'].iloc[i] and df['SMA_short'].iloc[i-1] >= df['SMA_mid'].iloc[i-1]:
    signals.append('sell')
  else:
    signals.append('hold')  # シグナルなし
signals.insert(0, 'hold')  # 最初の行はシグナルなし

df["signal"] = signals

# ==================================================
# バックテスト
# ==================================================
# 初期設定
initial_balance = 10000  # 初期資金（円）
balance = initial_balance  # 現金残高
position = 0               # 保有ポジション（BTC量）
trade_log = []             # トレード履歴

# バックテスト実行
for i in range(1, len(df)):
    if df['signal'].iloc[i] == 'buy' and position == 0:  # 買いエントリー
        entry_price = df['close'].iloc[i]
        position = balance / entry_price  # BTC量計算
        balance = 0                       # 現金残高をゼロにする
        trade_log.append({'action': 'buy', 'price': entry_price, 'time': df.index[i]})
    elif df['signal'].iloc[i] == 'sell' and position > 0:  # 売りエグジット
        exit_price = df['close'].iloc[i]
        balance = position * exit_price  # 現金残高更新
        position = 0                     # ポジション解消
        trade_log.append({'action': 'sell', 'price': exit_price, 'time': df.index[i]})

# 未決済ポジションの評価（バックテスト終了時点）
if position > 0:
    balance += position * df['close'].iloc[-1]  # 最終価格で評価
    trade_log.append({'action': 'sell', 'price': df['close'].iloc[-1], 'time': df.index[-1]})
    position = 0

# バックテスト結果の表示
print(f"初期資金: {initial_balance:.2f}円")
print(f"最終残高: {balance:.2f}円")
print(f"損益: {balance - initial_balance:.2f}円")

# トレード履歴の表示
print("\nトレード履歴:")
for trade in trade_log:
    print(trade)
