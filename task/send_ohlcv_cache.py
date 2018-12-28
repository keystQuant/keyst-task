import redis, time
import requests
import pandas  as pd

KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'

KOSPI_VOL = 'KOSPI_VOL'
KOSDAQ_VOL = 'KOSDAQ_VOL'


class KeystTask(object):

    def __init__(self):
        self.mktcap_url = 'http://45.76.202.71:3000/api/v1/stocks/mktcap/?date={}&page={}'
        self.today_date = time.strftime('%Y%m%d')
        self.cache_ip = '198.13.60.19'
        self.cache_pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'
        self.r = redis.StrictRedis(host=self.cache_ip, port=6379, password=self.cache_pw)
        self.kp_tickers = [ticker.decode() for ticker in self.r.lrange(KOSPI_TICKERS, 0 ,-1)]
        self.kd_tickers = [ticker.decode() for ticker in self.r.lrange(KOSDAQ_TICKERS, 0 ,-1)]
        print("Task is ready")

    def make_refined_data(self):
        refined_ticker = []
        status_code = 200
        i = 0
        while True:
            i += 1
            req = requests.get(self.mktcap_url.format(self.today_date, i))
            status_code = req.status_code
            if status_code == 404:
                break
            mkcap_ticker = [r['code'] for r in req.json()['results']]
            refined_ticker += mkcap_ticker
        return refined_ticker

    def make_ticker_data(self, kp_tickers, kd_tickers):
        refined_ticker = self.make_refined_data()
        kp_tickers_dict = dict()
        kd_tickers_dict = dict()
        kp_tickers_list = [ticker.split('|')[0] for ticker in self.kp_tickers if ticker.split('|')[0] in refined_ticker]
        kd_tickers_list = [ticker.split('|')[0] for ticker in self.kd_tickers if ticker.split('|')[0] in refined_ticker]
        for ticker in kp_tickers:
            kp_tickers_dict[ticker.split('|')[0]] = ticker.split('|')[1]
        for ticker in kd_tickers:
            kd_tickers_dict[ticker.split('|')[0]] = ticker.split('|')[1]
        return kp_tickers_list, kd_tickers_list, kp_tickers_dict, kd_tickers_dict

    def make_redis_ohlcv_df(self, mode, kp_tickers_list, kd_tickers_list):
        make_data_start = True
        global total_ohlcv, total_vol
        if mode == 'kp':
            tickers_list = kp_tickers_list
        elif mode == 'kd':
            tickers_list =  kd_tickers_list
        else:
            print('choose kp or kd')
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
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
        return total_ohlcv, total_vol

    def send_ohlcv_data(self):
        success=False
        kp_tickers_list, kd_tickers_list, kp_tickers_dict, kd_tickers_dict = self.make_ticker_data(self.kp_tickers, self.kd_tickers)
        print(len(kp_tickers_list), len(kd_tickers_list))
        kp_ohlcv, kp_vol = self.make_redis_ohlcv_df('kp', kp_tickers_list, kd_tickers_list)
        kd_ohlcv, kd_vol = self.make_redis_ohlcv_df('kd', kp_tickers_list, kd_tickers_list)

        print(kp_ohlcv.shape, kd_ohlcv.shape, kp_vol.shape, kd_vol.shape)
        r.set(KOSPI_OHLCV, kp_ohlcv.to_msgpack(compress='zlib'))
        r.set(KOSDAQ_OHLCV, kd_ohlcv.to_msgpack(compress='zlib'))
        r.set(KOSPI_VOL, kp_vol.to_msgpack(compress='zlib'))
        r.set(KOSDAQ_VOL, kd_vol.to_msgpack(compress='zlib'))
        success=True
        return success, "Data send complete"
