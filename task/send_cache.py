import redis, time
import requests
import pandas  as pd
import time

from task.cache import RedisClient

KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'
ETF_TICKERS = 'ETF_FULL_TICKERS'
ETN_TICKERS = 'ETN_LIST'

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'
ETF_OHLCV = 'ETF_OHLCV'

KOSPI_VOL = 'KOSPI_VOL'
KOSDAQ_VOL = 'KOSDAQ_VOL'
ETF_VOL = 'ETF_VOL'


class KeystTask(object):

    def __init__(self):
        self.redis = RedisClient()
        self.kp_tickers = [ticker.decode() for ticker in self.redis.redis_client.lrange(KOSPI_TICKERS, 0 ,-1)]
        self.kd_tickers = [ticker.decode() for ticker in self.redis.redis_client.lrange(KOSDAQ_TICKERS, 0 ,-1)]
        self.etf_list = self.redis.get_list(ETF_TICKERS)
        self.etn_list = self.redis.get_list(ETN_TICKERS)
        self.etf_tickers = list(set(self.etf_list + self.etn_list))
        self.mkt_tickers = self.redis.get_list('MKTCAP_TICKERS')
        print("Task is ready", len(self.kp_tickers), len(self.kd_tickers), len(self.etf_tickers))

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
                print(key)
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

    def make_redis_buysell_df(self):
        start = time.time()
        make_data_start = True
        tickers_list = self.mkt_tickers
        print("{}:".format("buysell ticker length"), len(tickers_list))
        global total_pri_sell
        global total_frg_net
        global total_ins_net
        i = 0
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
            i += 1
            if i % 100 == 0:
                print(ticker)
            key = ticker + '_BUYSELL'
            try:
                buysell = pd.read_msgpack(self.redis.redis_client.get(key))
            except ValueError:
                print("Nonexistent Key:",key)
                continue
            buysell.set_index('date', inplace=True)
            buysell.index = pd.to_datetime(buysell.index)
            pri_sell = buysell[['private_s']]
            ins_net = buysell[['inst_sum_n']]
            frg_net = buysell[['forgn_n']]
            pri_sell.rename({'private_s':ticker}, axis='columns', inplace=True)
            ins_net.rename({'inst_sum_n':ticker}, axis='columns', inplace=True)
            frg_net.rename({'forgn_n':ticker}, axis='columns', inplace=True)

            if make_data_start:
                total_pri_sell = pri_sell
                total_frg_net = ins_net
                total_ins_net = frg_net
                make_data_start = False
                print(make_data_start)
            else:
                total_pri_sell = pd.concat([total_pri_sell, pri_sell], axis=1)
                total_frg_net = pd.concat([total_frg_net, ins_net], axis=1)
                total_ins_net = pd.concat([total_ins_net, frg_net], axis=1)
            if i % 100 == 0:
                print("df_size_buysell:", total_pri_sell.shape, total_frg_net.shape, total_ins_net.shape)
        end = time.time()
        print(end-start)
        return total_pri_sell, total_frg_net, total_ins_net

    def make_factor_df(self):
        start = time.time()
        make_data_start = True
        tickers_list = self.mkt_tickers
        print("{}:".format("buysell ticker length"), len(tickers_list))
        global total_pbr
        global total_per
        global total_pcr
        global total_psr
        i = 0
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
            i += 1
            if i % 100 == 0:
                print(ticker)
            key = ticker + '_FACTOR'
            try:
                factor_df = pd.read_msgpack(self.redis.redis_client.get(key))
            except ValueError:
                print("Nonexistent Key:",key)
                continue
            factor_df.set_index('date', inplace=True)
            factor_df.index = pd.to_datetime(factor_df.index)
            factor_pbr = factor_df[['pbr']]
            factor_per = factor_df[['per']]
            factor_pcr = factor_df[['pcr']]
            factor_psr = factor_df[['psr']]
            factor_pbr.rename({'pbr':ticker}, axis='columns', inplace=True)
            factor_per.rename({'per':ticker}, axis='columns', inplace=True)
            factor_pcr.rename({'pcr':ticker}, axis='columns', inplace=True)
            factor_psr.rename({'psr':ticker}, axis='columns', inplace=True)
            if make_data_start:
                total_pbr = factor_pbr
                total_per = factor_per
                total_pcr = factor_pcr
                total_psr = factor_psr
                df_length = factor_pbr.shape[0]
                make_data_start = False
                print(make_data_start)
            else:
                try:
                    if factor_pbr.shape[0] <= df_length:
                        total_pbr = pd.concat([total_pbr, factor_pbr], axis=1)
                        total_per = pd.concat([total_per, factor_per], axis=1)
                        total_pcr = pd.concat([total_pcr, factor_pcr], axis=1)
                        total_psr = pd.concat([total_psr, factor_psr], axis=1)
                    else:
                        print("LenthError Key:", key, "ticker: ", ticker)
                        print(factor_pbr.shape[0], df_length)
                        continue
                except ValueError:
                    print("ValueError Key:", key, "ticker: ", ticker)
                    continue
            if i % 100 == 0:
                print("df_size_factor:", total_pbr.shape, total_per.shape, total_pcr.shape, total_psr.shape)
        end = time.time()
        print(end-start)
        return total_pbr, total_per, total_pcr, total_psr

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

        self.redis.redis_client.set(mkt_df_key, mkt_df.to_msgpack(compress='zlib'))
        end = time.time()
        success=True
        print(end-start)
        return success, "Data send complete"

    def send_buysell_data(self):
        start = time.time()
        success=False
        pri_sell_df, frg_net_df, ins_net_df = self.make_redis_buysell_df()
        print(pri_sell_df.shape, frg_net_df.shape, ins_net_df.shape)
        PRI_DF_KEY = "PRI_SELL_DF"
        FRG_DF_KEY = "FRG_NET_DF"
        INS_DF_KEY = "INS_NET_DF"

        for key in [PRI_DF_KEY, FRG_DF_KEY, INS_DF_KEY]:
            response = self.redis.redis_client.exists(key)
            if response != False:
                self.redis.redis_client.delete(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))

        self.redis.redis_client.set(PRI_DF_KEY, pri_sell_df.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(FRG_DF_KEY, frg_net_df.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(INS_DF_KEY, ins_net_df.to_msgpack(compress='zlib'))
        end = time.time()
        success=True
        print(end-start)
        return success, "Data send complete"


    def send_factor_data(self):
        start = time.time()
        success=False
        pbr_df, per_df, pcr_df, psr_df = self.make_factor_df()
        print(pbr_df.shape, per_df.shape, pcr_df.shape, psr_df.shape)
        PBR_DF_KEY = "PBR_DF"
        PER_DF_KEY = "PER_DF"
        PSR_DF_KEY = "PSR_DF"
        PCR_DF_KEY = "PCR_DF"

        for key in [PBR_DF_KEY, PER_DF_KEY, PSR_DF_KEY, PCR_DF_KEY]:
            response = self.redis.redis_client.exists(key)
            if response != False:
                self.redis.redis_client.delete(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))

        self.redis.redis_client.set(PBR_DF_KEY, pbr_df.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(PER_DF_KEY, per_df.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(PSR_DF_KEY, psr_df.to_msgpack(compress='zlib'))
        self.redis.redis_client.set(PCR_DF_KEY, pcr_df.to_msgpack(compress='zlib'))
        end = time.time()
        success=True
        print(end-start)
        return success, "Data send complete"
