import pandas as pd
import pandas_datareader as pdr

##### Naver Finance에서 KOSPI 가져오기 #####
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
#kospi_total_df.to_csv('kospi_total.csv', index=True, header=True, encoding="euc-kr")

##### Yahoo Finance에서 S&P 500 가져오기 #####
import pandas_datareader as pdr

# 날짜 설정
from datetime import datetime
start = datetime(2019,9,27)
end = datetime(2020,3,27)

# get_data_yahoo API를 통해서 yahho finance의 주식 종목 데이터를 가져온다.
#S&P500
sp500_df = pdr.get_data_yahoo('^GSPC',start, end)
print(sp500_df.tail(10))
sp500_df.to_csv('sp500.csv', index=True, header=True, encoding="euc-kr")