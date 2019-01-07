import redis, time
import requests
import pandas  as pd

KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'
ETF_TICKERS = 'ETF_TICKERS'

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'
ETF_OHLCV = 'ETF_OHLCV'

KOSPI_VOL = 'KOSPI_VOL'
KOSDAQ_VOL = 'KOSDAQ_VOL'
ETF_VOL = 'ETF_VOL'


class KeystTask(object):

    def __init__(self):
        self.mktcap_url = 'http://45.76.202.71:3000/api/v1/stocks/mktcap/?date={}&page={}'
        self.cache_ip = '198.13.60.19'
        self.cache_pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'
        self.r = redis.StrictRedis(host=self.cache_ip, port=6379, password=self.cache_pw)
        self.kp_tickers = [ticker.decode() for ticker in self.r.lrange(KOSPI_TICKERS, 0 ,-1)]
        self.kd_tickers = [ticker.decode() for ticker in self.r.lrange(KOSDAQ_TICKERS, 0 ,-1)]
        self.etf_tickers = [ticker.decode() for ticker in self.r.lrange(ETF_TICKERS, 0 ,-1)]
        print("Task is ready", len(self.kp_tickers), len(self.kd_tickers), len(self.etf_tickers))

    def make_refined_data(self):
        samsung_ohlcv = pd.read_msgpack(self.r.get('005930_OHLCV'))
        recent_date = int(samsung_ohlcv.tail(1)['date'])
        refined_ticker = []
        status_code = 200
        i = 0
        while True:
            i += 1
            req = requests.get(self.mktcap_url.format(recent_date, i))
            status_code = req.status_code
            if status_code == 404:
                break
            mkcap_ticker = [r['code'] for r in req.json()['results']]
            refined_ticker += mkcap_ticker
        return refined_ticker

    def make_ticker_data(self, kp_tickers, kd_tickers, etf_tickers, mode=None):
        kp_tickers_dict = dict()
        kd_tickers_dict = dict()
        etf_tickers_dict = dict()
        etf_tickers_list = [ticker.split('|')[0] for ticker in etf_tickers]
        if mode == 'except_etf':
            mkt_ticker = self.make_refined_data()
            refined_ticker = [i for i in mkt_ticker if i not in etf_tickers_list]
        else:
            refined_ticker = self.make_refined_data()
        kp_tickers_list = [ticker.split('|')[0] for ticker in kp_tickers if ticker.split('|')[0] in refined_ticker]
        kd_tickers_list = [ticker.split('|')[0] for ticker in kd_tickers if ticker.split('|')[0] in refined_ticker]
        return kp_tickers_list, kd_tickers_list, etf_tickers_list

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
        print(len(tickers_list))
        i = 0
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
            i += 1
            if i % 100 == 0:
                print(ticker)
            key = ticker + '_OHLCV'
            ohlcv = pd.read_msgpack(self.r.get(key))
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
            else:
                total_ohlcv = pd.concat([total_ohlcv, ohlcv_df], axis=1)
                total_vol = pd.concat([total_vol, vol_df], axis=1)
            print(len(total_ohlcv, total_vol))
        return total_ohlcv, total_vol

    def send_ohlcv_data(self):
        success=False
        kp_tickers_list, kd_tickers_list, etf_tickers_list = self.make_ticker_data(self.kp_tickers, self.kd_tickers, self.etf_tickers)
        print(len(kp_tickers_list), len(kd_tickers_list), len(etf_tickers_list))
        kp_ohlcv, kp_vol = self.make_redis_ohlcv_df('kp', kp_tickers_list, kd_tickers_list, etf_tickers_list)
        kd_ohlcv, kd_vol = self.make_redis_ohlcv_df('kd', kp_tickers_list, kd_tickers_list, etf_tickers_list)
        etf_ohlcv, etf_vol = self.make_redis_ohlcv_df('etf', kp_tickers_list, kd_tickers_list, etf_tickers_list)
        print(kp_ohlcv.shape, kd_ohlcv.shape, kp_vol.shape, kd_vol.shape, etf_ohlcv.shape, etf_vol.shape)

        for key in [KOSPI_OHLCV, KOSDAQ_OHLCV, ETF_OHLCV, KOSPI_OHLCV, KOSDAQ_VOL, ETF_VOL]:
            response = self.r.exists(key)
            if response != False:
                self.r.delete(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))

        self.r.set(KOSPI_OHLCV, kp_ohlcv.to_msgpack(compress='zlib'))
        self.r.set(KOSDAQ_OHLCV, kd_ohlcv.to_msgpack(compress='zlib'))
        self.r.set(ETF_OHLCV, etf_ohlcv.to_msgpack(compress='zlib'))
        self.r.set(KOSPI_VOL, kp_vol.to_msgpack(compress='zlib'))
        self.r.set(KOSDAQ_VOL, kd_vol.to_msgpack(compress='zlib'))
        self.r.set(ETF_VOL, etf_vol.to_msgpack(compress='zlib'))
        success=True
        return success, "Data send complete"
