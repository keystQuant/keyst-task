from celery import shared_task

import time
import requests
from .cache import RedisClient
from task.send_ohlcv_cache import KeystTask

GOBBLE_URL = 'http://45.77.31.8:3000/task/'
API_URL = 'http://45.76.202.71:3000/api/v1/stocks/task/?type='

DATE = 'http://45.77.31.8:3000/task/DATE'
TICKER = 'http://45.77.31.8:3000/task/TICKER'
STOCKINFO = 'http://45.77.31.8:3000/task/STOCKINFO'
INDEX = 'http://45.77.31.8:3000/task/INDEX'
ETF =  'http://45.77.31.8:3000/task/ETF'
OHLCV = 'http://45.77.31.8:3000/task/OHLCV'
MARKETCAPITAL = 'http://45.77.31.8:3000/task/MARKETCAPITAL'
BUYSELL = 'http://45.77.31.8:3000/task/BUYSELL'
FACTOR = 'http://45.77.31.8:3000/task/FACTOR'
UPDATE = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=SET_UPDATE_TASKS'

cache_ticker_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_TICKER_DATA'
get_ticker_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=_GET_TICKERS'
cache_index_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_INDEX_DATA'
cache_ohlcv_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_OHLCV_DATA'
cache_full_ohlcv_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_FULL_OHLCV_DATA'
cache_buysell_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_BUYSELL_DATA'

k = KeystTask()

def update_tasks(redis_client):
    task_ran = redis_client.key_exists('TASK_IN_PROGRESS')
    if not task_ran:
        return 1

    task_in_progress = redis_client.get_key('TASK_IN_PROGRESS')
    task_in_progress = 1 if task_in_progress == 'True' else 0
    if not task_in_progress:
        # task가 실행중이 아니면, 실행
        return 1
    else:
        return 0

def starting_tasks(redis_client):
    redis_client.set_key('TASK_IN_PROGRESS', 'True')

def ending_tasks(redis_client):
    redis_client.set_key('TASK_IN_PROGRESS', 'False')

@shared_task
def data_update_task():
    print('데이터 수집 작업 시작')

    r = RedisClient()
    update = update_tasks(r)

    if update == 1:
        print('같은 프로세스가 현재 실행중이지 않습니다. 태스크 시작')
        starting_tasks(r)
        ### 날짜부터 업데이트한다
        ### 하지만 날짜가 업데이트되기 전에 레디스에서 그 상태를 알 수 있도록 업데이트 안 되었다고 한다
        print('날짜 정보를 수집합니다')
        r.set_key('DATE_JUST_UPDATED_TO_DB', 'False')
        task = requests.get(GOBBLE_URL + 'DATE')

        set_other_tasks_ready = False
        while not set_other_tasks_ready:
            date_just_updated_in_db = r.get_key('DATE_JUST_UPDATED_TO_DB')
            if date_just_updated_in_db == 'True':
                print('날짜가 업데이트되었습니다. 다음 태스크로 넘어갑니다.')
                set_other_tasks_ready = True
            else:
                print('dates still not updated, waiting...')
                continue
            time.sleep(4) # 4초씩 쉬고 레디스 서버에 요청을 보낸다

        print('업데이트가 필요한 정보를 DB에서 확인합니다.')
        task = requests.get(API_URL + 'SET_UPDATE_TASKS')

        if task.json()['status'] == 'DONE':
            # 날짜가 업데이트되었다면, 업데이트할 모든 데이터를 FnGuide로부터 수집해온다
            task = requests.get(GOBBLE_URL + 'FNGUIDE')

        ending_tasks(r)
    else:
        print('TASK ALREADY IN PROGRESS: 같은 태스크가 이미 실행중입니다. 조금 있다 다시 시도합니다.')
    return True

@shared_task
def temp_update_check():
    date = requests.get(DATE)
    time.sleep(120)
    print("DATE")
    update = requests.get(UPDATE)
    print("Update")
    return True

@shared_task
def temp_data_crawler():
    ticker = requests.get(TICKER)
    time.sleep(120)
    print("TICKER")
    index = requests.get(INDEX)
    time.sleep(120)
    print("INDEX")
    ohlcv = requests.get(OHLCV)
    time.sleep(120)
    print("OHLCV")
    info = requests.get(STOCKINFO)
    time.sleep(120)
    print("STOCKINFO")
    mktcap = requests.get(MARKETCAPITAL)
    time.sleep(120)
    print("MARKETCAPITAL")
    buysell = requests.get(BUYSELL)
    time.sleep(120)
    print("BUYSELL")
    factor = requests.get(FACTOR)
    time.sleep(120)
    print("Data Crawler Quit")
    return True

# 밤 사이에 ETF 데이터만 업데이트가 되지 않아서 추가한 Task
@shared_task
def temp_etf_crawler():
    etf = requests.get(ETF)
    time.sleep(120)
    print("ETF")
    return True

@shared_task
def temp_send_cache():
    cache_ticker = requests.get(cache_ticker_data)
    get_ticker = requests.get(get_ticker_data)
    cache_index = requests.get(cache_index_data)
    cache_ohlcv = requests.get(cache_ohlcv_data)
    cache_full_ohlcv = requests.get(cache_full_ohlcv_data)
    cache_buysell = requests.get(cache_buysell_data)
    print("all process is completed!")
    return True

@shared_task
def send_ohlcv_cache():
    k.send_ohlcv_data()
    return True
