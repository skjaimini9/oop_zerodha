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

    def lot_symb(self):
        if self.exch != self.lot_symbol[:3]:
            symbol_map = {
                "NSE:NIFTY BANK": "BANKNIFTY",
                "NSE:NIFTY 50": "NIFTY"
            }
            symbol = self.lot_symbol
            if symbol in symbol_map:
                return symbol_map[symbol]
            else:
                return symbol[4:].replace(" ", "")
        return self.lot_symbol

    def filtered_instrument(self):
        symbol = self.lot_symb()
        if self.expiry is None and self.instrument_segment == "NFO-FUT":
            filterd_instruments = self.df_instrument[(self.df_instrument["name"] == symbol) & (self.df_instrument["segment"] == self.instrument_segment)]
        elif self.expiry is not None and self.instrument_segment is not None:
            filterd_instruments = self.df_instrument[(self.df_instrument["name"] == symbol) & (self.df_instrument["segment"] == self.instrument_segment) & (self.df_instrument["expiry"] == self.expiry)]
        else:
            filterd_instruments = self.df_instrument[(self.df_instrument["z_symb"] == symbol) & (self.df_instrument["exchange"] == self.lot_symbol[:3])]
        filterd_instruments = filterd_instruments.reset_index(drop=True)
        return filterd_instruments

    @staticmethod
    def filter_instruments_for_symbols(symbols, instrument_segment=None, expiry=None):
        filtered_dataframes = []
        for symbol in symbols:
            zdf = ZerodhaDataframes(symbol, instrument_segment=instrument_segment, expiry=expiry)
            filtered_df = zdf.filtered_instrument()
            filtered_dataframes.append(filtered_df)
        combined_df = pd.concat(filtered_dataframes, ignore_index=True)
        return combined_df

    @staticmethod
    def load_lot_size_data(data):
        lot_size_dict = {}
        for index, row in data.iterrows():
            instrument_token = row['instrument_token']
            lot_size = row['lot_size']
            lot_size_dict[instrument_token] = lot_size
        return lot_size_dict


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
