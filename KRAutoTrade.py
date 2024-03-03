import requests
import json
import datetime
import time
import yaml
import pandas as pd
import subprocess
import sqlite3

with open('config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

def send_message_multi(msg1, msg2):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg1)} {str(msg2)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

def get_access_token():
    """토큰 발급"""
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
    "appkey":APP_KEY, 
    "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    ACCESS_TOKEN = res.json()["access_token"]
    return ACCESS_TOKEN
    
def hashkey(datas):
    """암호화"""
    PATH = "uapi/hashkey"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
    'content-Type' : 'application/json',
    'appKey' : APP_KEY,
    'appSecret' : APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey

def get_current_price(code="005930"):
    """현재가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['stck_prpr'])

def get_price_sign(code="005930"):
    """상한가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['prdy_vrss_sign'])

def get_short_over(code="005930"):
    """상한가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return res.json()['output']['short_over_yn']


def get_warn_code(code="005930"):
    """현재가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['mrkt_warn_cls_code'])

def get_stock_balance():
    """주식 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    stock_dict = {}
    send_message(f"                              ")
    send_message(f"=========주식 보유잔고=========")    
    for stock in stock_list:
        if int(stock['hldg_qty']) > 0:
            stock_dict[stock['pdno']] = stock['hldg_qty']
            send_message(f"{stock['prdt_name']}({stock['pdno']}): {stock['hldg_qty']}주")
            time.sleep(0.1)
    send_message(f"주식 평가 금액: {evaluation[0]['scts_evlu_amt']}원")
    time.sleep(0.1)
    send_message(f"평가 손익 합계: {evaluation[0]['evlu_pfls_smtl_amt']}원")
    time.sleep(0.1)
    send_message(f"총 평가 금액: {evaluation[0]['tot_evlu_amt']}원")
    time.sleep(0.1)
    send_message(f"==============================")
    send_message(f"                              ")
    return stock_dict

def get_balance():
    """현금 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8908R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": "005930",
        "ORD_UNPR": "65500",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "Y"
    }
    res = requests.get(URL, headers=headers, params=params)
    cash = res.json()['output']['nrcvb_buy_amt']
    send_message(f"주문 가능 현금 잔고: {cash}원")
    return int(cash)

def get_tot_eval():
    """총 평가금액"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(URL, headers=headers, params=params)
    evaluation = res.json()['output2']
    send_message(f"총 평가 금액: {evaluation[0]['tot_evlu_amt']}원")
    return int(evaluation[0]['tot_evlu_amt'])


def buy(code="005930", qty="1"):
    """주식 시장가 매수"""  
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": str(int(qty)),
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC0802U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"[매수 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매수 실패]{str(res.json())}")
        return False

def sell(code="005930", qty="1"):
    """주식 시장가 매도"""
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": qty,
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC0801U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"[매도 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매도 실패]{str(res.json())}")
        return False

# 자동매매 시작
try:
    ACCESS_TOKEN = get_access_token()
    bought_list = [] # 매수 완료된 리스트
    stock_dict = get_stock_balance() # 보유 주식 조회                  
    total_cash = get_balance() # 보유 현금 조회       
    total_eval = get_tot_eval() # 총 평가금액        
    send_message_multi("보유종목수 :" , len(stock_dict))   
    for sym in stock_dict.keys():
        bought_list.append(sym)
    target_buy_count_15 = 2 # 종가 매매할 종목 수
    buy_percent = 0.42 # 종목당 매수 금액 비율
    buy_amount = total_eval * buy_percent  # 종목별 주문 금액 계산        
    soldout = False
    buy_jb = False    
    jb_1430 = False    
    Market_data = False 
    MarketTiming = "False"
    python_MT = 'MarketTiming.py'                
    python_Trade_now = "Market_now.py"    

    send_message("                           ")
    send_message("===========================")
    send_message("===========START===========")
    send_message("===========================")
    send_message("                           ")
    
    while True:
        t_now        = datetime.datetime.now()                
        t_9          = t_now.replace(hour=9, minute=0, second=0, microsecond=0)                 
        t_sell_start = t_now.replace(hour=9, minute=7, second=0, microsecond=0)
        t_sell_end   = t_now.replace(hour=9, minute=9, second=0, microsecond=0)        
        t_buy_0      = t_now.replace(hour=9, minute=10, second=0, microsecond=0)
        t_12         = t_now.replace(hour=12, minute=0, second=0, microsecond=0)
        t_1430       = t_now.replace(hour=14, minute=30, second=0, microsecond=0)
        t_sell_fin   = t_now.replace(hour=14, minute=55, second=0, microsecond=0)
        t_buy_1      = t_now.replace(hour=15, minute=11, second=0, microsecond=0)
        t_buy_2      = t_now.replace(hour=15, minute=19, second=0, microsecond=0)
        t_exit       = t_now.replace(hour=19, minute=35, second=0,microsecond=0)         
        today        = datetime.datetime.today().weekday()     

        if t_now < t_9 : 
            send_message("매매준비!! 종배 종목 매도 시간 9:7 ~ 9:9 ")                               
            time.sleep(300)  

        if  t_9 < t_now < t_sell_start :
            send_message("매매준비!! 종배 종목 매도 시간 9:7 ~ 9:9 ")                  
            time.sleep(30)       

        if t_sell_start < t_now < t_sell_end and soldout == False: 
            stock_dict = get_stock_balance() 
            send_message("======종배 종목 매도 시작=====")    
            for sym, qty in stock_dict.items():
                sell(sym, qty)
            soldout = True
            bought_list = []
            stock_dict = get_stock_balance()         
            send_message("=======종베 종목 매도 완료========")    
            send_message_multi("보유종목수 :" , len(stock_dict))                           
            send_message("========주문가능 현금 조회========")    
            total_cash = get_balance() # 주문가능 현금 조회  
            total_eval = get_tot_eval() # 총 평가금액             
            buy_amount = total_eval * buy_percent  # 종목별 주문 금액 계산                 
            send_message_multi("종목별 주문 금액 :" , buy_amount)       

        if t_buy_0 < t_now < t_12 :  # 9:10 ~ 12:00 : 매수
     
            stock_dict = get_stock_balance()     
            send_message_multi("보유종목수 :" , len(stock_dict))    

            if len(stock_dict) < 2 :    # 보유주식 2개 미만일 때 매매 시도            
                send_message("오전장 매매 - 09:10 ~ 12:00")       
                send_message("========보유 현금 조회========")    
                total_cash = get_balance() # 주문가능 현금 조회
                total_eval = get_tot_eval() # 총 평가금액
                buy_amount = total_eval * buy_percent  # 종목별 주문 금액 계산
                send_message_multi("종목별 주문 금액 :" , buy_amount)        

                result = subprocess.run(["python", python_MT], capture_output=True, text=True)
                MarketTiming = result.stdout.rstrip('\n')
                if MarketTiming == "False": 
                    send_message_multi("MarketTiming :", MarketTiming)    
                    send_message("MarketTiming 으로 인해 거래하지 않습니다.")
                    send_message("===========5분 Waiting===========")                 
                    time.sleep(300) 
                if MarketTiming == "True" :   
                    send_message_multi("MarketTiming :", MarketTiming)    
                    time.sleep(5)     

                    subprocess.run(["python", python_Trade_now], capture_output=True, text=True)       #현재데이터 산출

                    conn = sqlite3.connect('tickers.db')

                    query = ("SELECT Code, Name from " 
                            + "( SELECT a.Code, d.Name, a.rank + b.rank + c.rank + d.rank + e.rank f_rank from"
                            + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(Amount) DESC) AS rank FROM Market_List where Date >= ( SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 2) group by Code ) a, "
                            + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(ChagesRatio) DESC) AS rank FROM Market_List where Date >= ( SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 2) group by Code ) b,"    
                            + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(ChagesRatio) DESC) AS rank FROM Market_List where Date >= ( SELECT MIN(DATE) FROM Market_List ) and ChagesRatio <= 30 group by Code ) c,	"                                              
                            + "    ( SELECT Code, Name,  ROW_NUMBER() OVER (ORDER BY Amount DESC) AS rank FROM Market_now where ChagesRatio < 1 and ChagesRatio > -6 and Marcap < 30000000000000 and Close > 1100  ) d, "
                            + "    ( SELECT Code, Name,  ROW_NUMBER() OVER (ORDER BY ChagesRatio DESC) AS rank FROM Market_now where ChagesRatio < 1 and ChagesRatio > -6 and Marcap < 30000000000000 and Close > 1100  ) e "
                            + "where a.Code=b.Code and b.Code=c.Code and c.Code=d.Code and d.Code=e.Code order by f_rank);"
                    )                    

                    df_result = pd.read_sql_query(query, conn)

                    conn.commit()
                    conn.close()

                    df_name = df_result.head(7)
                    send_message(df_name)  # 매매 종목명 출력

                    df_code = df_result['Code'].head(7)
                    symbol_list = df_code.values.tolist()               

                    for sym in symbol_list:                          
                        if sym in bought_list:                       # 대상 종목이 보유 종목일 경우 skip 
                            continue                    
                        current_price = get_current_price(sym)
                        warn_code = get_warn_code(sym) # 시장 경고 코드 확인
                        send_message_multi('매수완료 수량 : ', len(stock_dict))
                        if  warn_code == 0: # 시장 경고 코드 없을 경우에만 매수 진행
                            buy_qty = 0  # 매수할 수량 초기화
                            buy_qty = int(buy_amount // current_price)                            
                            if buy_qty > 0:
                                send_message_multi(f"{sym} 매수를 시도합니다. 매수 수량 : ", buy_qty)
                                result = buy(sym, buy_qty)
                                if result:
                                    soldout = False
                                    buy_jb  = True
                                    bought_list.append(sym)  
                                    get_stock_balance()       
                        else :
                            send_message_multi(f"{sym} 매수 skip 되었습니다. : 시장경고코드 ( 0 : 정상 )- ", warn_code)                                          
                    time.sleep(5) 

                    get_stock_balance()      
            else :
                send_message_multi("종목 매수가 완료 되었습니다. 보유종목수 :" , len(stock_dict))                                                     
                time.sleep(60) 
            time.sleep(300) 
        if t_1430 < t_now < t_sell_fin and jb_1430 == False:  # 종가베팅 1차 종목선정 14:30~14:50
            send_message("종가베팅 1차 종목선정 (MarketTiming 무관)- 14:30~14:45")   

            subprocess.run(["python", python_Trade_now], capture_output=True, text=True)                  
            conn = sqlite3.connect('tickers.db')

            query = ("SELECT Code, Name from " 
                    + "( SELECT a.Code, d.Name, a.rank + b.rank + c.rank + d.rank + e.rank f_rank from"
                    + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(Amount) DESC) AS rank FROM Market_List where Date >= ( SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 3) group by Code ) a, "
                    + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(ChagesRatio) DESC) AS rank FROM Market_List where Date >= ( SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 3) group by Code ) b,"    
                    + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(ChagesRatio) DESC) AS rank FROM Market_List where Date >= ( SELECT MIN(DATE) FROM Market_List ) and ChagesRatio <= 30 group by Code ) c,	"                                              
                    + "    ( SELECT Code, Name,  ROW_NUMBER() OVER (ORDER BY Amount DESC) AS rank FROM Market_now where ChagesRatio < 29 and ChagesRatio > 8 and Marcap < 30000000000000 and Close > 1100  ) d, "
                    + "    ( SELECT Code, Name,  ROW_NUMBER() OVER (ORDER BY ChagesRatio DESC) AS rank FROM Market_now where ChagesRatio < 29 and ChagesRatio > 8 and Marcap < 30000000000000 and Close > 1100  ) e "
                    + "where a.Code=b.Code and b.Code=c.Code and c.Code=d.Code and d.Code=e.Code order by f_rank);"
            )
            
            df_result = pd.read_sql_query(query, conn)

            df_Trade_Target = df_result.head(7)
            send_message(df_Trade_Target)                                      
            jb_1430 = True       

            conn.commit()
            conn.close()

            send_message("===========14:45까지 Waiting ===========")       
            time.sleep(10)                   

        if t_sell_fin < t_now < t_buy_1 and soldout == False: # 오전장 매매 수량 매도 14:55 ~ 15:10
            send_message("======오전장 종목 매도 시작 14:55 ~ 15:10 =====")      
            get_stock_balance()         
            for sym, qty in stock_dict.items():
                sell(sym, qty)
            soldout = True
            bought_list = []
            stock_dict = get_stock_balance()        
            send_message("======오전장 종목 매도 완료=====")      
            send_message_multi("보유종목수 :" , len(stock_dict))    
            send_message("========주문가능 현금 조회========")    
            total_cash = get_balance() # 주문가능 현금 조회  
            total_eval = get_tot_eval() # 총 평가금액             
            buy_amount = total_eval * buy_percent  # 종목별 주문 금액 계산                 
            send_message_multi("종목별 주문 금액 :" , buy_amount)              

        if t_buy_1 < t_now < t_buy_2 :  # 15:11 ~ 15:19 : 매수
            
            stock_dict = get_stock_balance()     
            send_message_multi("보유종목수 :" , len(stock_dict))    

            if len(stock_dict) < 2 :
                send_message("=== 종가베팅 매수진행 시간- 15:11 ~ 15:19 ===")  
                send_message("========보유 현금 조회========")    
                total_cash = get_balance() # 주문가능 현금 조회
                total_eval = get_tot_eval() # 총 평가금액
                buy_amount = total_eval * buy_percent  # 종목별 주문 금액 계산
                send_message_multi("종목별 주문 금액 :" , buy_amount)            

                result = subprocess.run(["python", python_MT], capture_output=True, text=True)

                MarketTiming = result.stdout.rstrip('\n')
                if MarketTiming == "False": 
                    send_message_multi("MarketTiming :", MarketTiming)    
                    send_message("MarketTiming 으로 인해 종가베팅은 하지 않습니다.")
                    send_message("===========5분 Waiting===========")             
                    time.sleep(300)
                if MarketTiming == "True":
                    send_message_multi("MarketTiming :", MarketTiming)                               

                    subprocess.run(["python", python_Trade_now], capture_output=True, text=True)                  

                    conn = sqlite3.connect('tickers.db')

                    query = ("SELECT Code, Name from " 
                            + "( SELECT a.Code, d.Name, a.rank + b.rank + c.rank + d.rank + e.rank f_rank from"
                            + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(Amount) DESC) AS rank FROM Market_List where Date >= ( SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 3) group by Code ) a, "
                            + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(ChagesRatio) DESC) AS rank FROM Market_List where Date >= ( SELECT DISTINCT DATE FROM Market_List ORDER BY DATE DESC LIMIT 1 OFFSET 3) group by Code ) b,"    
                            + "    ( SELECT Code, ROW_NUMBER() OVER (ORDER BY sum(ChagesRatio) DESC) AS rank FROM Market_List where Date >= ( SELECT MIN(DATE) FROM Market_List ) and ChagesRatio <= 30 group by Code ) c,	"                                              
                            + "    ( SELECT Code, Name,  ROW_NUMBER() OVER (ORDER BY Amount DESC) AS rank FROM Market_now where ChagesRatio < 29 and ChagesRatio > 8 and Marcap < 30000000000000 and Close > 1100  ) d, "
                            + "    ( SELECT Code, Name,  ROW_NUMBER() OVER (ORDER BY ChagesRatio DESC) AS rank FROM Market_now where ChagesRatio < 29 and ChagesRatio > 8 and Marcap < 30000000000000 and Close > 1100  ) e "
                            + "where a.Code=b.Code and b.Code=c.Code and c.Code=d.Code and d.Code=e.Code order by f_rank);"
                    )

                    df_result = pd.read_sql_query(query, conn)

                    conn.commit()
                    conn.close()

                    df_name = df_result.head(10)
                    send_message(df_name)  # 매매 종목명 출력

                    df_code = df_result['Code'].head(10)
                    symbol_list = df_code.values.tolist()                          

                    for sym in symbol_list:                    
                        if sym in bought_list:
                            continue
                        current_price = get_current_price(sym) #현재가 확인                   
                        price_sign = get_price_sign(sym) # 전일대비부호(1:상한, 2:상승, 3:보합, 4:하한, 5:하락)
                        warn_code = get_warn_code(sym) # 시장 경고 코드 확인       
                        short_over = get_short_over(sym) #단기 과열 여부 확인
                        if  warn_code == 0 and price_sign == 2 and short_over == 'N': # 시장 경고 코드 없을 경우, 단기과열이 아닌 경우, 전일대비 상승일 경우만 매수
                            buy_qty = 0  # 매수할 수량 초기화
                            buy_qty = int(buy_amount // current_price)       
                                                
                            if buy_qty > 0:
                                send_message_multi(f"{sym} 매수를 시도합니다. 매수 수량 : ", buy_qty )                            
                                result = buy(sym, buy_qty)
                                if result:
                                    soldout = False
                                    buy_jb = True
                                    bought_list.append(sym)                                                               
                            time.sleep(2)                 
                        else :                  
                            if warn_code != 0 :      
                                send_message_multi(f"{sym} 매수 skip 되었습니다.  시장경고코드 : ", warn_code)                          
                            if short_over == 'Y':
                                send_message_multi(f"{sym} 매수 skip 되었습니다.  단기과열여부 : ", short_over)       
                            if price_sign !=2 :
                                send_message_multi(f"{sym} 매수 skip 되었습니다.  전일대비부호(1:상한, 2:상승, 3:보합, 4:하한, 5:하락) : ", price_sign)     
                    time.sleep(5)            
            else :
                send_message_multi("종목 매수가 완료 되었습니다. 보유종목수 :" , len(stock_dict))                                                     
                time.sleep(300) 
        if t_buy_2 < t_now < t_exit:  # 15:19 ~ 15:35 : balance check
            if soldout == False:
                stock_dict = get_stock_balance()
                bought_list = []
                time.sleep(300)
        if t_exit < t_now :  # 15:35 ~ 16:35 :프로그램 종료
            send_message("==============================")
            send_message("=========15:30 장 마감========")
            send_message("==============================")
            send_message("                              ")
            get_stock_balance()
            send_message_multi("보유종목수 :" , len(stock_dict))   
            time.sleep(10)
            break
except Exception as e:
    send_message(f"[오류 발생]{e}")
    time.sleep(10)