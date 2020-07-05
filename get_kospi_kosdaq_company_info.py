#출처: https://wendys.tistory.com/173 [웬디의 기묘한 이야기]
#출처: https://excelsior-cjh.tistory.com/109 [EXCELSIOR]

##### 모듈 1 - 한국 상장 회사 및 코드 가져오기 #####
import pandas as pd

# 종목 타입에 따라 download url이 다름. 종목코드 뒤에 .KS .KQ등이 입력되어야해서 Download Link 구분 필요
stock_type = {
'kospi': 'stockMkt',
'kosdaq': 'kosdaqMkt'
}

# download url 조합
def get_download_stock(market_type=None):
    market_type = stock_type[market_type]
    download_link = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType='
    download_link = download_link + market_type
    
    df = pd.read_html(download_link, header=0)[0]
    return df

# kospi 종목코드 목록 다운로드
def get_download_kospi():
    df = get_download_stock('kospi')
    df.종목코드 = df.종목코드.map('{:06d}'.format) # 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
    return df

# kosdaq 종목코드 목록 다운로드
def get_download_kosdaq():
    df = get_download_stock('kosdaq')
    df.종목코드 = df.종목코드.map('{:06d}'.format) # 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
    return df

# df에서 필요한 정보만을 정리하는 함수
def arrange_df(code_df):
    code_df = code_df[['회사명', '종목코드', '업종', '주요제품']] # 필요한 column들만 포함
    code_df = code_df.rename(columns={'회사명': 'name', '종목코드': 'code', '업종': 'sector', '주요제품': 'main_product'}) # 한글로된 컬럼명을 영어로 바꿔준다.
    return code_df

# kospi, kosdaq 종목코드 각각 다운로드
kospi_df = get_download_kospi()
kosdaq_df = get_download_kosdaq()

# kospi, kosdaq df를 정리하기
arranged_kospi_df = arrange_df(kospi_df)
arranged_kosdaq_df = arrange_df(kosdaq_df)

# 결과 print 및 csv 파일로 export
print("kospi 출력 결과:")
print(arranged_kospi_df.head())
print("kosdaq 출력 결과:")
print(arranged_kosdaq_df.head())
# arranged_kospi_df.to_csv('kospi_info.csv', index=True, header=True, encoding="euc-kr")
# arranged_kosdaq_df.to_csv('kosdaq_info.csv', index=True, header=True, encoding="euc-kr")


 ##### 모듈 2 - KOSPI 및 KOSDAQ 6개월 간 주식 변동 가져오기 #####
import pandas_datareader as pdr

#변수 준비
kospi_list = arranged_kospi_df[['name']].values.tolist() #for loop에 사용할 list를 생성
kosdaq_list = arranged_kosdaq_df[['name']].values.tolist() #for loop에 사용할 list를 생성
pages = 16 #함수를 통해 회사별로 (16-1)*10개의 날짜에 따른 주식 데이터를 획득하게 됨
recent_date = '2020.03.30' #수집 데이터 최신일

#kospi 또는 kosdaq 주식 데이터를 획득하는 function
def get_stock_price_from_naver_finance(name_list, arranged_df, pages, recent_date):
    #함수 내 변수 생성
    final_df = pd.DataFrame() #빈 df를 생성
    tmp = 0 #date를 입력시키기 위한 tmp

    #회사별 날짜에 따른 주식 데이터를 획득
    for name in name_list:
        #neglect unexpeceted errors with try-except
        try:
            item_name = name[0]
            url = get_url(item_name, arranged_df)
            df = pd.DataFrame()
            for page in range(1, pages): #1에서 15페이지까지 출력
                pg_url = '{url}&page={page}'.format(url=url, page=page)
                df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)
                df = df.dropna() # NaN 데이터가 포함된 row를 버린다
                #page 1을 이용한 유효성 검사
                if page == 1 :
                    #empty dataframe이면 inner for loop를 나온다
                    if df.empty:
                        print('df is empty-braking the inner for loop')
                        break
                    else:
                        print('테스트', name, ':', str(df.iloc[0]['날짜']))
                        # print(df)
                        # final date가 2020.03.27이 아니면 inner for loop를 나온다
                        if (str(df.iloc[0]['날짜']) != recent_date):
                            print('final date is not ', recent_date, '-braking the inner loop')
                            break #inner for loop를 나온다.
                        else:
                            tmp += 1
                            print(tmp,'번째 유효한 회사 데이터 획득')
                # print('테스트', item_name, ', page ', page, 'done')
            
            df.rename(columns={'날짜':'date', '종가':'close_price', '전일비':'difference_by_day', '시가':'open_price', '고가':'highest_price', '저가':'lowest_price', '거래량':'trading_volume'}, inplace=1)
            if df.empty:
                print('df is empty-continuing the outer for loop')
                continue
            elif (str(df.iloc[0]['date']) == recent_date):
                #첫번째 경우만 날짜 복사
                if tmp == 1: 
                    tmp_list = []
                    for date in df[['date']].values.tolist():
                        tmp_list.append(date[0])
                    final_df['_date'] = tmp_list

                tmp_list2 = []
                for close_price in df[['open_price']].values.tolist():
                    tmp_list2.append(int(close_price[0]))
                final_df['code_'+get_code(arranged_df, item_name)] = tmp_list2

                print(item_name, ' written in final_df')
            else:
                print('final date is not ', recent_date, '-continuing the outer loop')

        except:
            print('error occured at', item_name, 'continue anyway')
            continue

    return final_df

# code로 url을 획득
def get_url(item_name, code_df):
    code = get_code(code_df, item_name)
    code = code.strip() # sript()로 앞뒤 공백 제거
    url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code) 
    # print("요청 종목: ",item_name)
    # print("요청 URL = {}".format(url))
    return url
    
# name으로 code를 획득, code는 string이다.
def get_code(df, name):
    code = df.query("name=='{}'".format(name))['code'].to_string(index=False)
    code = code.strip() # sript()로 앞뒤 공백 제거
    return code

#get_stock_price_from_nave_finance를 통해 naver finance에서 Kospi 또는 Kosdaq 데이터를 획득하고, 
# 획득한 데이터를 csv로 export
final_kospi_df = get_stock_price_from_naver_finance(kospi_list, arranged_kospi_df, pages,recent_date)
final_kospi_df.to_csv('kospi_recent.csv', index=True, header=True, encoding="euc-kr")
print('final_kospi_df', 'export complete!!')

final_kosdaq_df = get_stock_price_from_naver_finance(kosdaq_list, arranged_kosdaq_df, pages,recent_date)
final_kosdaq_df.to_csv('kosdaq_recent.csv', index=True, header=True, encoding="euc-kr")
print('final_kosdaq_df', 'export complete!!') 

""" ##### 모듈 3 - Naver Finance에서 KOSPI 가져오기 #####
import pandas_datareader as pdr

kospi_total_url = 'https://finance.naver.com/sise/sise_index_day.nhn?code=KOSPI'

# 일자 데이터를 담을 df라는 DataFrame 정의
kospi_total_df = pd.DataFrame()

# 1페이지에서 21페이지의 데이터만 가져오기
for page in range(1, 22):
    pg_url = (kospi_total_url + '&page={page}').format(url=kospi_total_url, page=page)
    kospi_total_df = kospi_total_df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

# df.dropna()를 이용해 결측값 있는 행 제거
kospi_total_df = kospi_total_df.dropna()

# 상위 5개 데이터 확인하기 및 csv로 export
print(kospi_total_df.head())
kospi_total_df.to_csv('kospi_total.csv', index=True, header=True, encoding="euc-kr")

##### 모듈 4 - Yahoo Finance에서 S&P 500 가져오기 #####
import pandas_datareader as pdr

# 날짜 설정
from datetime import datetime
start = datetime(2019,9,27)
end = datetime(2020,3,27)

# get_data_yahoo API를 통해서 yahho finance의 주식 종목 데이터를 가져온다.
#S&P500
sp500_df = pdr.get_data_yahoo('^GSPC',start, end)
print(sp500_df.tail(10))
sp500_df.to_csv('sp500.csv', index=True, header=True, encoding="euc-kr") """

