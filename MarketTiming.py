import pandas as pd
import FinanceDataReader as fdr

# 현재 코스닥 지수 가져오기
index_code = 'KOSDAQ'
df_index = fdr.DataReader(index_code)

# 지수 종가를 기준으로 이동평균 계산
df_index['MA_3'] = df_index['Close'].rolling(window=3).mean()
df_index['MA_5'] = df_index['Close'].rolling(window=5).mean()
df_index['MA_7'] = df_index['Close'].rolling(window=7).mean()
df_index['MA_10'] = df_index['Close'].rolling(window=10).mean()

# 현재 코스닥 지수와 각 이동평균 값을 비교하여 True 또는 False 값을 반환하는 함수
def check_current_ma(df):
    current_close = df.iloc[-1]['Close']    
    ma_3 = df.iloc[-1]['MA_3']
    ma_5 = df.iloc[-1]['MA_5']
    ma_7 = df.iloc[-1]['MA_7']
    ma_10 = df.iloc[-1]['MA_10']
    return ( current_close >= ma_3 or current_close >= ma_5 or current_close >= ma_10 ) and ( current_close >= ma_7 or current_close >= ma_10 )

# 현재 코스닥 지수가 이동평균 중 최소 하나 이상보다 높은지 여부 확인
MarketTiming = check_current_ma(df_index)
print(MarketTiming)        
