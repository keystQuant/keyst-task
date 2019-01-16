import redis, time
import requests
import pandas  as pd
import time

from task.cache import RedisClient


KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'
ETF_TICKERS = 'ETF_FULL_TICKERS'

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'
ETF_OHLCV = 'ETF_OHLCV'

KOSPI_VOL = 'KOSPI_VOL'
KOSDAQ_VOL = 'KOSDAQ_VOL'
ETF_VOL = 'ETF_VOL'


class KeystTask(object):

    def __init__(self):
        # self.mktcap_url = 'http://45.76.202.71:3000/api/v1/stocks/mktcap/?date={}&page={}'
        self.cache_ip = '198.13.60.19'
        self.cache_pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'
        # self.r = redis.StrictRedis(host=self.cache_ip, port=6379, password=self.cache_pw)
        self.redis = RedisClient()
        self.kp_tickers = [ticker.decode() for ticker in self.redis.redis_client.lrange(KOSPI_TICKERS, 0 ,-1)]
        self.kd_tickers = [ticker.decode() for ticker in self.redis.redis_client.lrange(KOSDAQ_TICKERS, 0 ,-1)]
        self.etf_tickers = self.redis.get_list(ETF_TICKERS)
        self.mkt_tickers = self.redis.get_list('MKTCAP_TICKERS')
        print("Task is ready", len(self.kp_tickers), len(self.kd_tickers), len(self.etf_tickers))

    # def make_refined_data(self):
    #     samsung_ohlcv = pd.read_msgpack(self.redis.RedisClient.get('005930_OHLCV'))
    #     recent_date = int(samsung_ohlcv.tail(1)['date'])
    #     print(recent_date)
    #     refined_ticker = []
    #     status_code = 200
    #     i = 0
    #     while True:
    #         i += 1
    #         req = requests.get(self.mktcap_url.format(recent_date, i))
    #         status_code = req.status_code
    #         if status_code == 404:
    #             break
    #         mkcap_ticker = [r['code'] for r in req.json()['results']]
    #         print("mkt_ticker_length:",len(mkcap_ticker))
    #         refined_ticker += mkcap_ticker
    #     return refined_ticker

    def make_ticker_data(self, kp_tickers, kd_tickers, mode=None):
        etf_tickers_list = self.etf_tickers
        if mode == 'except_etf':
            mkt_ticker = self.mkt_tickers
            print(len(mkt_ticker))
            refined_ticker = [m for m in mkt_ticker if m not in etf_tickers_list]
        else:
            refined_ticker = self.mkt_tickers
        kp_tickers_list = [ticker.split('|')[0] for ticker in kp_tickers if ticker.split('|')[0] in refined_ticker]
        kd_tickers_list = [ticker.split('|')[0] for ticker in kd_tickers if ticker.split('|')[0] in refined_ticker]
        return kp_tickers_list, kd_tickers_list

    def make_redis_ohlcv_df(self, mode, kp_tickers_list, kd_tickers_list,etf_tickers_list):
        make_data_start = True
        if mode == 'kp':
            tickers_list = kp_tickers_list
        elif mode == 'kd':
            tickers_list =  kd_tickers_list
        elif mode == 'etf':
            tickers_list =  etf_tickers_list
        else:
            print('choose kp or kd')
        print("{}:".format(mode), len(tickers_list))
        global total_ohlcv
        global total_vol
        i = 0
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
            i += 1
            if i % 100 == 0:
                print(ticker)
            key = ticker + '_OHLCV'
            try:
                ohlcv = pd.read_msgpack(self.redis.redis_client.get(key))
            except ValueError:
                print(ticker)
                continue
            ohlcv.set_index('date', inplace=True)
            ohlcv.index = pd.to_datetime(ohlcv.index)
            ohlcv_df = ohlcv[['adj_prc']]
            vol_df = ohlcv[['trd_qty']]
            ohlcv_df.rename({'adj_prc':ticker}, axis='columns', inplace=True)
            vol_df.rename({'trd_qty':ticker}, axis='columns', inplace=True)

            if make_data_start:
                total_ohlcv = ohlcv_df
                total_vol = vol_df
                make_data_start = False
                print(make_data_start)
            else:
                total_ohlcv = pd.concat([total_ohlcv, ohlcv_df], axis=1)
                total_vol = pd.concat([total_vol, vol_df], axis=1)
            if i % 100 == 0:
                print("df_size_{}".format(mode), total_ohlcv.shape, total_vol.shape)
        return total_ohlcv, total_vol

    def make_redis_mktcap_df(self):
        start = time.time()
        make_data_start = True
        tickers_list = self.mkt_tickers
        print("{}:".format("mkt ticker length"), len(tickers_list))
        global total_mkt_cap
        i = 0
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
            i += 1
            if i % 100 == 0:
                print(ticker)
            key = ticker + '_MKTCAP'
            mkt_capital = pd.read_msgpack(self.redis.redis_client.get(key))
            mkt_capital.set_index('date', inplace=True)
            mkt_capital.index = pd.to_datetime(mkt_capital.index)
            mkt_capital_df = mkt_capital[['comm_stk_qty']]
            mkt_capital_df.rename({'comm_stk_qty':ticker}, axis='columns', inplace=True)

            if make_data_start:
                total_mkt_cap = mkt_capital_df
                make_data_start = False
                print(make_data_start)
            else:
                total_mkt_cap = pd.concat([total_mkt_cap, mkt_capital_df], axis=1)
            if i % 100 == 0:
                print("df_size_MKT:", total_mkt_cap.shape)
        end = time.time()
        print(end-start)
        return total_mkt_cap

    def send_ohlcv_data(self):
        start = time.time()
        success=False
        kp_tickers_list, kd_tickers_list = self.make_ticker_data(self.kp_tickers, self.kd_tickers, mode="except_etf")
        print("ticker:",len(kp_tickers_list), len(kd_tickers_list), len(self.etf_tickers))
        kp_ohlcv, kp_vol = self.make_redis_ohlcv_df('kp', kp_tickers_list, kd_tickers_list, self.etf_tickers)
        print("kodpi_data:",kp_ohlcv.shape, kp_vol.shape)
        kd_ohlcv, kd_vol = self.make_redis_ohlcv_df('kd', kp_tickers_list, kd_tickers_list, self.etf_tickers)
        print("kosdaq_data:",kd_ohlcv.shape, kd_vol.shape)
        etf_ohlcv, etf_vol = self.make_redis_ohlcv_df('etf', kp_tickers_list, kd_tickers_list, self.etf_tickers)
        print("etf_data:",etf_ohlcv.shape, etf_vol.shape)

        for key in [KOSPI_OHLCV, KOSDAQ_OHLCV, ETF_OHLCV, KOSPI_OHLCV, KOSDAQ_VOL, ETF_VOL]:
            response = self.redis.key_exists(key)
            if response != False:
                self.redis.redis_client.delete(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))

        self.redis.redis_client.set(KOSPI_OHLCV, kp_ohlcv.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(KOSDAQ_OHLCV, kd_ohlcv.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(ETF_OHLCV, etf_ohlcv.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(KOSPI_VOL, kp_vol.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(KOSDAQ_VOL, kd_vol.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(ETF_VOL, etf_vol.to_msgpack(compress='zlib'))
        end = time.time()
        success=True
        print(end-start)
        return success, "Data send complete"

    def send_mkt_data(self):
        start = time.time()
        success=False
        mkt_df = self.make_redis_mktcap_df()
        print(mkt_df.shape)
        mkt_df_key = "MKTCAP_DF"

        response = self.redis.redis_client.exists(mkt_df_key)
        if response != False:
            self.redis.redis_client.delete(mkt_df_key)
            print('{} 이미 있음, 삭제하는 중...'.format(mkt_df_key))

        self.redis.redis_client.set('MKT_CAPITAL', mkt_df.to_msgpack(compress='zlib'))
        end = time.time()
        success=True
        print(end-start)
        return success, "Data send complete"
