""" I am contineously working on code it is not in functionl condition right now on later stage i am planning to pass class veriable as class attribute. kiteext module is used for personal use not for any finencial use.
if you want any part of code changed or have issue with it please write a mail to albeegupta121@gmail.com. just an initial stage. it is ment for public once complete. all contribution are welcome.
i will also try to add greeks in options via  another static method. Any suggession is welcome
optimising this .... 
Name of Contributers:

1.
2.
3.

"""
import pandas as pd
from kiteext import KiteExt
from GetIVGreeks import *
import time as tm

class ZerodhaDataframes:
    #stop_get_oi_thread = False
    def __init__(self):
        self.kite = KiteExt()
        self.login()
        self.df_instrument = self.get_instruments()
        self.bnifty_weekly_expiry = pd.to_datetime("11-30-2023").strftime('%Y-%m-%d')
        self.nifty_weekly_expiry = pd.to_datetime("11-30-2023").strftime('%Y-%m-%d')
        self.b_n_month_exp = pd.to_datetime("11-30-2023").strftime('%Y-%m-%d')
        self.atm_nifty = {'last_price': round(self.kite.quote('NSE:NIFTY 50')['NSE:NIFTY 50']['last_price'] / 50) * 50}
        self.nifty_atm = float(self.atm_nifty['last_price'])
        self.atm_bnifty = {'last_price': round(self.kite.quote('NSE:NIFTY BANK')['NSE:NIFTY BANK']['last_price'] / 100) * 100}
        self.bnifty_atm = float(self.atm_bnifty['last_price'])

    def login(self):
        self.kite.login_using_enctoken(userid="GS7875", enctoken=open("enc.txt", "r").read(), public_token=None)

    def get_instruments(self):
        print('wait 1 min...')
        instruments = self.kite.instruments()
        df_instrument = pd.DataFrame(instruments, index=None)
        df_instrument['expiry'] = pd.to_datetime(df_instrument['expiry']).dt.strftime('%Y-%m-%d')
        return df_instrument
        
    def get_bnifty_weekly_ce(self):
        bnifty_wce = self.df_instrument.loc[(self.df_instrument["name"] == 'BANKNIFTY') & (self.df_instrument["expiry"] == self.bnifty_weekly_expiry) & (self.df_instrument["instrument_type"] == 'CE')]
        bnifty_wce = bnifty_wce.reset_index(drop=True)
        bnifty_wce['symb'] = bnifty_wce.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        bnifty_atm_pos = bnifty_wce[bnifty_wce['strike'] == self.bnifty_atm].index[0]
        bnifty_wce = bnifty_wce.iloc[bnifty_atm_pos - 10 : bnifty_atm_pos + 11]
        return bnifty_wce
        
    def get_bnifty_weekly_pe(self):
        bnifty_wpe = self.df_instrument.loc[(self.df_instrument["name"] == 'BANKNIFTY') & (self.df_instrument["expiry"] == self.bnifty_weekly_expiry) & (self.df_instrument["instrument_type"] == 'PE')]
        bnifty_wpe = bnifty_wpe.reset_index(drop=True)
        bnifty_wpe['symb'] = bnifty_wpe.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        bnifty_atm_pos = bnifty_wpe[bnifty_wpe['strike'] == self.bnifty_atm].index[0]
        bnifty_wpe = bnifty_wpe.iloc[bnifty_atm_pos - 10 : bnifty_atm_pos + 11]
        return bnifty_wpe

    def get_bnifty_monthly_ce(self):
        bnifty_mce = self.df_instrument.loc[(self.df_instrument["name"] == 'BANKNIFTY') & (self.df_instrument["expiry"] == self.b_n_month_exp) & (self.df_instrument["instrument_type"] == 'CE')]
        bnifty_mce['symb'] = bnifty_mce.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        bnifty_atm_pos = bnifty_mce[bnifty_mce['strike'] == self.bnifty_atm].index[0]
        bnifty_mce = bnifty_mce.iloc[bnifty_atm_pos - 10 : bnifty_atm_pos + 11]
        return bnifty_mce

    def get_bnifty_monthly_pe(self):
        bnifty_mpe = self.df_instrument.loc[(self.df_instrument["name"] == 'BANKNIFTY') & (self.df_instrument["expiry"] == self.b_n_month_exp) & (self.df_instrument["instrument_type"] == 'PE')]
        bnifty_mpe['symb'] = bnifty_mpe.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        bnifty_atm_pos = bnifty_mpe[bnifty_mpe['strike'] == self.bnifty_atm].index[0]
        bnifty_mpe = bnifty_mpe.iloc[bnifty_atm_pos - 10 : bnifty_atm_pos + 11]
        return bnifty_mpe
        
    def get_nifty_weekly_ce(self):
        nifty_wce = self.df_instrument.loc[(self.df_instrument["name"] == 'NIFTY 50') & (self.df_instrument["expiry"] == self.nifty_weekly_expiry) & (self.df_instrument["instrument_type"] == 'CE')]
        nifty_wce['ce_symb'] = nifty_wce.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        nifty_atm_pos = nifty_wce[nifty_wce['strike'] == self.nifty_atm].index[0]
        nifty_wce = nifty_wce.iloc[nifty_atm_pos - 10 : nifty_atm_pos + 11]
        return nifty_wce

    def get_nifty_weekly_pe(self):
        nifty_wpe = self.df_instrument.loc[(self.df_instrument["name"] == 'NIFTY 50') & (self.df_instrument["expiry"] == self.nifty_weekly_expiry) & (self.df_instrument["instrument_type"] == 'PE')]
        nifty_wpe['symb'] = nifty_wpe.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        nifty_atm_pos = nifty_wpe[nifty_wpe['strike'] == self.nifty_atm].index[0]
        nifty_wpe = nifty_wpe.iloc[nifty_atm_pos - 10 : nifty_atm_pos + 11]
        return nifty_wpe

    def get_nifty_monthly_ce(self):
        nifty_mce = self.df_instrument.loc[(self.df_instrument["name"] == 'NIFTY 50') & (self.df_instrument["expiry"] == self.b_n_month_exp) & (self.df_instrument["instrument_type"] == 'CE')]
        nifty_mce['symb'] = nifty_mce.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        nifty_atm_pos = nifty_mce[nifty_mce['strike'] == self.nifty_atm].index[0]
        nifty_mce = nifty_mce.iloc[nifty_atm_pos - 10 : nifty_atm_pos + 11]
        return nifty_mce

    def get_nifty_monthly_pe(self):
        nifty_mpe = self.df_instrument.loc[(self.df_instrument["name"] == 'NIFTY 50') & (self.df_instrument["expiry"] == self.b_n_month_exp) & (self.df_instrument["instrument_type"] == 'PE')]
        nifty_mpe['symb'] = nifty_mpe.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        nifty_atm_pos = nifty_mpe[nifty_mpe['strike'] == self.nifty_atm].index[0]
        nifty_mpe = nifty_mpe.iloc[nifty_atm_pos - 10 : nifty_atm_pos + 11]
        return nifty_mpe

    @staticmethod
    def get_prev_day_oi(data, kite, stop_get_oi_thread, lot_size):
        try:
            prev_day_oi = {}
            max_retries = 5  # Define the maximum number of retry attempts
            for symbol, v in data.items():
                retries = 0
                while retries < max_retries:
                    try:
                        if symbol in prev_day_oi:
                            break
                        if "instrument_token" in v:  # Ensure "token" is present in the data
                            #print(v["instrument_token"])
                            pre_day_data = kite.historical_data(v["instrument_token"], (dt.now() - timedelta(days=5)).date(), (dt.now() - timedelta(days=1)).date(), "day", oi=True)
                            if pre_day_data:
                                prev_day_oi[symbol] = pre_day_data[-1]["oi"]
                            else:
                                prev_day_oi[symbol] = 0
                        else:
                            print(f"Token missing for symbol {symbol}")
                        break
                    except Exception as e:
                        print(f"Error fetching data for symbol {symbol}: {e}")
                        retries += 1
                        tm.sleep(0.5)
            return prev_day_oi
        except Exception as e:
            print(e)
            pass
        
    @staticmethod
    def generate_live_dataframe(data_frame, prev_day_oi, lot_size, kite):
        live_dict = {}  # Empty dictionary to store live data
        for k,v in kite.quote(list(data_frame['symb'])).items():
            try:
                print(live_dict[k])
            except Exception as e:
                live_dict[k] = {}
            live_dict[k]["lastPrice"] = v.get("last_price", None)
            live_dict[k]["totalTradedvolume"] = int(int(v.get("volume", 0)) / lot_size)
            live_dict[k]["openInterest"] = int(v.get("oi", 0) / lot_size)
            live_dict[k]["BIDQTY"] = v.get('depth', {}).get('buy', [{}])[0].get('quantity', None)
            live_dict[k]["BIDPRICE"] = v.get('depth', {}).get('buy', [{}])[0].get('price', None)
            live_dict[k]["ASKPRICE"] = v.get('depth', {}).get('sell', [{}])[0].get('price', None)
            live_dict[k]["ASKQTY"] = v.get('depth', {}).get('sell', [{}])[0].get('quantity', None)
            live_dict[k]["change"] = v.get("last_price", 0) - v.get("ohlc", {}).get("close", 0)
            if v["last_price"] != 0:
                live_dict[k]["Pchange"] = ((v["last_price"] - v["ohlc"]["open"]) * 100 / v["last_price"])
            else:
                live_dict[k]["Pchange"] = 0
            live_dict[k]["open"] = v["ohlc"]["open"]
            live_dict[k]["high"] = v["ohlc"]["high"]
            live_dict[k]["low"] = v["ohlc"]["low"]
            live_dict[k]["PrevClose"] = v["ohlc"]["close"]
            live_dict[k]["vwap"] = v["average_price"]
            live_dict[k]["volume"] = v["volume"]
            live_dict[k]["last_quantity"] = v["last_quantity"]
            live_dict[k]["sell_quantity"] = v["sell_quantity"]
            live_dict[k]["buy_quantity"] = v["buy_quantity"]
            live_dict[k]["oi"] = v["oi"]
            live_dict[k]["oi_day_high"] = v["oi_day_high"]
            live_dict[k]["oi_day_low"] = v["oi_day_low"]
            live_dict[k]["ChangeP"] = (v['last_price'] - v["ohlc"]["close"])*100 / v["ohlc"]["close"]
            live_dict[k]["tradedvalue"] = (v["volume"] * v["average_price"])/100000000
            live_dict[k]["gapp"] = (v["ohlc"]["open"] - v["ohlc"]["close"])*100 / v["ohlc"]["close"]
            if v["last_price"] != 0:
                live_dict[k]["price_volity"] = (v['last_price'] - v["ohlc"]["open"])*100 / v["ohlc"]["open"]
            else:
                live_dict[k]["price_volity"] = 0
            if v['average_price'] !=0 and v["ohlc"]["open"] != 0:
                live_dict[k]["vwap_c_open"] = (v['average_price'] - v["ohlc"]["open"])*100 / v["ohlc"]["open"]
            else:
                live_dict[k]["vwap_c_open"] = 0
            try:
                live_dict[k]["changeinopeninterest"] = int((v.get("oi", 0) - prev_day_oi.get(k, 0)) / lot_size)
            except Exception as e:
                live_dict[k]["changeinopeninterest"] = 0
            tm.sleep(0.1)
        # Create a DataFrame from the live data dictionary
        live_data_df = pd.DataFrame(live_dict).transpose()            
        return live_data_df

zdf = ZerodhaDataframes()
instrument_data = zdf.get_instruments()
bnif_wce = zdf.get_bnifty_weekly_ce()

# Fetch and set prev_day_oi (fetch it once)
prev_day_oi = ZerodhaDataframes.get_prev_day_oi(bnif_wce.to_dict(orient='index'), zdf.kite, stop_get_oi_thread=False, lot_size=15)
print("Prev Day OI:", prev_day_oi)

# Uncoment Below will do all at once as DOWNLOADING INStruments TAke TIME Recomended to run below commented code in next cell
"""
while True:  # This loop will continuously fetch live data
    # Fetch live data for the specific symbols of interest
    live_data_wce = zdf.generate_live_dataframe(bnif_wce, prev_day_oi, 15, zdf.kite)

    # Process the live data as needed
    print("Live Data WCE:", live_data_wce)
    tm.sleep(2)
    """
