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


##### 모듈 2 - 한국 상장 회사 및 코드 가져오기 #####
import pandas_datareader as pdr

# name으로 code를 획득, code는 string이다.
def get_code(df, name):
    code = df.query("name=='{}'".format(name))['code'].to_string(index=False)
    code = code.strip() # sript()로 앞뒤 공백 제거
    return code

kosdaq_recent_df = pd.read_csv("kosdaq_recent.csv", index=True, encoding="euc-kr") 
print(kosdaq_recent_df.head())