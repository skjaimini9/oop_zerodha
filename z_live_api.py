""" I am contineously working on code it is not in functionl condition right now on later stage i am planning to pass class veriable as class attribute. kiteext module is used for personal use not for any finencial use.
if you want any part of code changed or have issue with it please write a mail to albeegupta121@gmail.com. just an initial stage. it is ment for public once complete. all contribution are welcome.
Name of Contributers:

1.
2.
3.

"""








import pandas as pd
from kiteext import KiteExt
import re
import time as tm

class ZerodhaDataframes:
    def __init__(self):
        self.kite = KiteExt()
        self.login()
        self.df_instrument = self.get_instruments()
        self.bnifty_weekly_expiry = pd.to_datetime("11-01-2023").strftime('%Y-%m-%d')
        self.nifty_weekly_expiry = pd.to_datetime("11-02-2023").strftime('%Y-%m-%d')
        self.b_n_month_exp = pd.to_datetime("10-26-2023").strftime('%Y-%m-%d')
        self.atm_nifty = self.get_atm_strike('NSE:NIFTY 50')
        self.atm_bnifty = self.get_atm_strike('NSE:NIFTY BANK')
        self.n_symbol = self.lot_symb('NSE:NIFTY 50')
        self.b_symbol = self.lot_symb('NSE:NIFTY BANK')
        self.r_symbol = self.lot_symb('NSE:RELIANCE')
        # self.symbol_atm = self.get_atm(
        self.ce = 'CE'
        self.pe = 'PE'

    def login(self):
        self.kite.login_using_enctoken(userid="GS1415", enctoken=open("enc.txt", "r").read(), public_token=None)

    def get_instruments(self):
        print('wait 1 min...')
        instruments = self.kite.instruments()
        df_instrument = pd.DataFrame(instruments, index=None)
        df_instrument['expiry'] = pd.to_datetime(df_instrument['expiry']).dt.strftime('%Y-%m-%d')
        df_instrument['z_symb'] = df_instrument.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        # df_instrument.to_csv('ek_bar.csv')
        return df_instrument

    def lot_symb(self, symbol):
        symbol = re.findall('[a-zA-Z]+', symbol)
        lot_symbol = ''.join(symbol.upper() for symbol in reversed(symbol))[:-3]
        print(lot_symbol)
        return lot_symbol
    
    
    def lot_size(self, symbol):
        try:
            symbol = self.lot_symb(symbol)
            lot_size = self.df_instrument.loc[(self.df_instrument["name"] == symbol) & (self.df_instrument["exchange"] == "NFO")]
            lot_size = lot_size.reset_index(drop=True)
            lot_size.to_csv('lot.csv')
            lot_size = list(lot_size['lot_size'])[0]
            print(lot_size)
        except:
            symbol = self.lot_symb(symbol) 
            lot_size = self.df_instrument.loc[(self.df_instrument["name"] == symbol) & (self.df_instrument["exchange"] == "NFO")]
            lot_size = lot_size.reset_index(drop=True)
            lot_size.to_csv('lot.csv')
            lot_size = list(lot_size['lot_size'])[0]
            lot_size = int(lot_size)
        return lot_size

    def get_atm_strike(self, symbol):
        try:
            last_price = self.kite.quote(symbol)[symbol]['last_price']
            lot_size = self.lot_size(symbol)
            return float(round(last_price / lot_size) * lot_size)
        except IndexError:
            print("Lot size not found for the symbol:", symbol)
            return None  # Handle this case as needed
    
    def get_option_data(self, symbol, expiry, instrument_type):
        data = self.df_instrument.loc[
            (self.df_instrument["name"] == symbol) &
            (self.df_instrument["expiry"] == expiry) &
            (self.df_instrument["instrument_type"] == instrument_type)
        ]
        data = data.reset_index(drop=True)
        atm_pos = data[data['strike'] == self.get_atm_strike(symbol) and symbol == 'NSE:NIFTY 50' else self.atm_bnifty].index[0]
        return data.iloc[atm_pos - 10:atm_pos + 11]

    def get_bnifty_weekly_ce(self):
        return self.get_option_data('BANKNIFTY', self.bnifty_weekly_expiry, self.ce)

    def get_bnifty_weekly_pe(self):
        return self.get_option_data('BANKNIFTY', self.bnifty_weekly_expiry, self.pe)

    def get_bnifty_monthly_ce(self):
        return self.get_option_data('BANKNIFTY', self.b_n_month_exp, self.ce)

    def get_bnifty_monthly_pe(self):
        return self.get_option_data('BANKNIFTY', self.b_n_month_exp, self.pe)

    def get_nifty_weekly_ce(self):
        return self.get_option_data('NSE:NIFTY 50', self.nifty_weekly_expiry, self.ce)

    def get_nifty_weekly_pe(self):
        return self.get_option_data('NSE:NIFTY 50', self.nifty_weekly_expiry, self.pe)

    def get_nifty_monthly_ce(self):
        return self.get_option_data('NSE:NIFTY 50', self.b_n_month_exp, self.ce)

    def get_nifty_monthly_pe(self):
        return self.get_option_data('NSE:NIFTY 50', self.b_n_month_exp, self.pe)

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
    def generate_dataframefrom_df_instrument(df_instrument, name, expiry, instrument_type):
        df = bnifty_wce.loc[
            (df_instrument["name"] == name) &
            (df_instrument["expiry"] == expiry) &
            (df_instrument["instrument_type"] == instrument_type)
        ]
        df = df.reset_index(drop=True)
        df['symb'] = df.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        atm_price = ZerodhaDataframes().get_atm_price('NSE:' + name)  # Assuming you have a get_atm_price method
        atm_pos = df[df['strike'] == atm_price].index[0]
        df = df.iloc[atm_pos - 10 : atm_pos + 11]
        return df

    
    @staticmethod
    def generate_live_dataframe(data_frame, prev_day_oi, lot_size, kite):
        live_dict = {}  # Empty dictionary to store live data
        for k,v in kite.quote(list(data_frame['symb'])).items():
            try:
                live_dict[k]
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
            live_dict[k]["tradedvalue"] = (v["volume"] * v["average_price"])/10000000
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
    tm.sleep(0.2)  # Adjust the delay as needed
"""
