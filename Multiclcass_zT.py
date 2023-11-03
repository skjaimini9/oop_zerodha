import pandas as pd
from kiteext import KiteExt
import kiteapp as kt

class ZerodhaDataframes:
    def __init__(self, symbol, expiry=None, instrument_segment=None, exch=None):
        self.kite = kite = kt.KiteApp("Shri Kant", "GS7875", open("enc.txt", "r").read())
        self.df_instrument = self.get_instruments()
        self.lot_symbol = symbol
        self.expiry = pd.to_datetime(expiry).strftime('%Y-%m-%d') if expiry is not None else None
        self.instrument_segment = instrument_segment
        self.exch = exch
        print(self.kite.quote('NSE:NIFTY 50'))
    
    def get_instruments(self):
        print('wait 1 min...')
        instruments = "https://api.kite.trade/instruments"
        df_instrument = pd.read_csv(instruments)
        df_instrument['expiry'] = pd.to_datetime(df_instrument['expiry']).dt.strftime('%Y-%m-%d')
        df_instrument['z_symb'] = df_instrument.apply(lambda row: f"{row['exchange']}:{row['tradingsymbol']}", axis=1)
        return df_instrument

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

zdf_cash_bse = ZerodhaDataframes("BSE:SENSEX", exch="BSE")
print(zdf_cash_bse.filtered_instrument())
zdf_cash_nse = ZerodhaDataframes("NSE:NIFTY BANK", exch="NSE")
print(zdf_cash_nse.filtered_instrument())
zdf_fut = ZerodhaDataframes("NSE:NIFTY 50", instrument_segment="NFO-FUT")
print(zdf_fut.filtered_instrument())
zdf_option = ZerodhaDataframes("NSE:NIFTY 50", expiry="11-30-2023", instrument_segment="NFO-OPT")
print(zdf_option.filtered_instrument())