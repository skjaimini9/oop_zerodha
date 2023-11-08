""" I am contineously working on code it is not in functionl condition right now on later stage i am planning to pass class veriable as class attribute. kiteext module is used for personal use not for any finencial use.
if you want any part of code changed or have issue with it please write a mail to albeegupta121@gmail.com. just an initial stage. it is ment for public once complete. all contribution are welcome.
i will also try to add greeks in options via  another static method. Any suggession is welcome

Name of Contributers:

1.
2.
3.

"""




import pandas as pd
from kiteext import KiteExt
import time as tm
from datetime import datetime, timedelta
import threading
import numpy as np
import datetime as dt
import warnings
warnings.filterwarnings("ignore")


df_instrument = pd.read_csv("https://api.kite.trade/instruments")
df_instrument['expiry'] = pd.to_datetime(df_instrument['expiry']).dt.strftime('%Y-%m-%d')
df_instrument['z_symb'] = df_instrument.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)



class ZerodhaDataframes:
    global df_instrument
    def __init__(self, symbol, expiry=None, instrument_segment=None, exch=None):
        self.df_instrument = df_instrument
        self.lot_symbol = symbol
        self.expiry = pd.to_datetime(expiry).strftime('%Y-%m-%d') if expiry is not None else None
        self.instrument_segment = instrument_segment
        self.exch = exch
        self.atm_base = "FUT" #"SPOT"
        self.lot_size = None
        self.get_oi_thread = None  # Add a thread attribute
        self.kite = KiteExt()
        self.login()

    def login(self):
        #Create your login method based on KiteExt using api with threaded=True i am going to pass it for now so to keep program working for Symbol and Token Fetching
        pass
    
    def lot_symb(self):
        if self.exch != self.lot_symbol[:3]:
            symbol_map = {
                "NSE:NIFTY BANK": "BANKNIFTY",
                "NSE:NIFTY 50": "NIFTY"
            }
            symbol = self.lot_symbol
            mapped_symbol = symbol_map.get(symbol)
            if mapped_symbol:
                return mapped_symbol
            else:
                return symbol[4:].replace(" ", "")
        return self.lot_symbol

    def filtered_instrument(self):
        symbol = self.lot_symb()
        if self.expiry is None and self.instrument_segment == "NFO-FUT":
            filterd_instruments = self.df_instrument[
                (self.df_instrument["name"] == symbol) & (self.df_instrument["segment"] == self.instrument_segment)
            ]
        elif self.expiry is not None and self.instrument_segment is not None:
            filterd_instruments = self.df_instrument[
                (self.df_instrument["name"] == symbol) & (self.df_instrument["segment"] == self.instrument_segment) & (self.df_instrument["expiry"] == self.expiry)
            ]
        else:
            filterd_instruments = self.df_instrument[
                (self.df_instrument["z_symb"] == symbol) & (self.df_instrument["exchange"] == self.lot_symbol[:3])
            ]
        filterd_instruments.reset_index(drop=True, inplace=True)
        filterd_instruments = pd.DataFrame(filterd_instruments)
        return filterd_instruments
    
    @staticmethod
    def filter_instruments_for_symbols(symbols, instrument_segment=None, expiry=None):
        dataframes = [ZerodhaDataframes(symbol, instrument_segment=instrument_segment, expiry=expiry) for symbol in symbols]
        filtered_dataframes = [zdf.filtered_instrument() for zdf in dataframes]
        combined_df = pd.concat(filtered_dataframes, ignore_index=True)
        return combined_df
    
    @staticmethod
    def load_lot_size_data(data):
        data.set_index('instrument_token')
        lot_size_dict = data['lot_size'].to_dict()
        return lot_size_dict

    def get_atm_strike(self, data):
        if data['segment'][0] == "NFO-OPT" and self.atm_base in ["SPOT", "FUT"]:
            name = data['name'][0]
            exchange = data['exchange'][0]
            segment = data['segment'][0]
            if segment.endswith("-OPT"):
                # Extract the base segment
                base_segment = segment[:-4]
                base_suffix = "FUT"
                if self.atm_base == "SPOT":
                    if name == "NIFTY":
                        ltp_symb = self.kite.ltp('NSE:NIFTY 50')['NSE:NIFTY 50']['last_price']
                    elif name == "BANKNIFTY":
                        ltp_symb = self.kite.ltp('NSE:NIFTY BANK')['NSE:NIFTY BANK']['last_price']
                    else:
                        lot_symb = f'{exchange}:{name}'
                        ltp_symb = self.kite.ltp(lot_symb)[lot_symb]['last_price']
                    data['strike_gap'] = abs(data['strike'] - ltp_symb)
                elif self.atm_base == "FUT":
                    fut_df = self.df_instrument[(self.df_instrument["name"] == name) & (self.df_instrument["segment"] == f'{exchange}-{base_suffix}')]
                    fut_df['expiry'] = pd.to_datetime(fut_df['expiry'])
                    td = dt.datetime.now()
                    nearest_exp = (fut_df['expiry'] - td).abs().idxmin()
                    fut_df = fut_df.loc[[nearest_exp]]  # Use .loc to select specific rows
                    fut_symb = fut_df['z_symb'].iloc[0]
                    fut_price = self.kite.ltp(fut_symb)[fut_symb]['last_price']
                    data['strike_gap'] = abs(data['strike'] - fut_price)
                
                Min_gap = data['strike_gap'].min()
                atm_strike = data[data.strike_gap == Min_gap].iloc[0]['strike']
                return atm_strike
        else:
            print('Issue in getting ltp Data')
            pass


sample_fut_z_symb_df = pd.read_csv('sample_cash_z_symb.csv')
fut_z_symb_list = list(sample_cash_z_symb_df['z_symb'])

# Filter instruments for a list of symbols with NFO-FUT instrument segment
combined_df = ZerodhaDataframes.filter_instruments_for_symbols(fut_z_symb_list, instrument_segment="NFO-FUT")
print(combined_df)

# Filter instruments for a list of symbols with expiry date "11-30-2023"
combined_df = ZerodhaDataframes.filter_instruments_for_symbols(fut_z_symb_list, expiry="11-30-2023", instrument_segment="NFO-FUT")
print(combined_df)


zdf_cash_bse = ZerodhaDataframes("BSE:SENSEX", exch="BSE")
print(zdf_cash_bse.filtered_instrument())
zdf_cash_nse = ZerodhaDataframes("NSE:NIFTY BANK", exch="NSE")
print(zdf_cash_nse.filtered_instrument())
zdf_fut = ZerodhaDataframes("NSE:NIFTY 50", instrument_segment="NFO-FUT")
print(zdf_fut.filtered_instrument())
zdf_option = ZerodhaDataframes("NSE:NIFTY 50", expiry="11-30-2023", instrument_segment="NFO-OPT")
print(zdf_option.filtered_instrument())
