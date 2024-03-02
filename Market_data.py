import FinanceDataReader as fdr
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pandas import DataFrame
from datetime import datetime

df_total = fdr.StockListing('KRX')
df_krx_1 = df_total.sort_values("Amount", ascending=False).head(500)
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
result.to_sql(con=conn, name='Market_List', if_exists='append', index=True)
conn.commit()
conn.close()

conn = sqlite3.connect('tickers.db')
query = f"DELETE FROM Market_List WHERE DATE < (SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 4);" 
conn.execute(query)
conn.commit()
conn.close()