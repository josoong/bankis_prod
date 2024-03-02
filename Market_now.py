import FinanceDataReader as fdr
import os
import sqlite3
import pandas as pd
import datetime
from pandas import DataFrame
from datetime import datetime

df_total = fdr.StockListing('KRX')
df_krx_1 = df_total.sort_values("Amount", ascending=False).head(200)
df_krx_2 = df_krx_1[~df_krx_1['Name'].str.contains('스팩')]
df_krx_3 = df_krx_2[~df_krx_2['Name'].str.contains('우B')]
df_krx_4 = df_krx_3[~df_krx_3['Name'].str.endswith('우')]
df_krx_5 = df_krx_4[~df_krx_4['Market'].str.contains('KONEX')]
df_krx_6 = df_krx_5[~df_krx_5['Dept'].str.contains('투자주의')]
df_krx   = df_krx_6[~df_krx_6['Dept'].str.contains('관리')]

df = pd.DataFrame(df_krx)

conn = sqlite3.connect('tickers.db')
today = datetime.today()
formatted_today = today.strftime("%Y%m%d")
df['Date'] = formatted_today
result = df.set_index(['Date'],  drop=True, append=True, inplace=False)
result.to_sql(con=conn, name='Market_now', if_exists='replace', index=False)

conn.commit()
conn.close()

