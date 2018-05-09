import os
import time

import numpy as np
import pandas as pd
from datetime import datetime

from zipline.utils.cli import maybe_show_progress
from zipline.data.bundles import core as bundles
import gdax

boDebug = True

def viagdax(symbols, start=None, end=None):
    symbols_tuple = tuple(symbols)

    def ingest(environ, asset_db_writer, minute_bar_writer,
               daily_bar_writer, adjustment_writer, calendar, start_session,
               end_session, cache, show_progress, output_dir):
        pass

def create_metadata(symbols_tuple):
    # Creating blank dfMetadata
    dfMetadata = pd.DataFrame(np.empty(len(symbols_tuple), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

    data_list = list()
    for iSid, symbol in enumerate(symbols_tuple):
        dfData = get_gdax_dataframe(symbol)
        data_list.append((iSid, dfData))

        # the start date is the date of the first trade and
        start_date = dfData.index[0]
        if boDebug:
            print("start_date",type(start_date),start_date)

        # the end date is the date of the last trade
        end_date = dfData.index[-1]
        if boDebug:
            print("end_date",type(end_date),end_date)

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)
        if boDebug:
            print( "ac_date",type(ac_date),ac_date)

        # Update our meta data
        dfMetadata.iloc[iSid] = start_date, end_date, ac_date, symbol
    return dfMetadata, data_list

def get_gdax_dataframe(product_id):
    today = datetime.today().date()
    today_iso = today.isoformat()

    now = datetime.now()
    now_iso = now.isoformat()

    public_client = gdax.PublicClient()
    result_list = public_client.get_product_historic_rates(product_id,
                                                           start=today_iso,
                                                           end=now_iso, granularity=300) 
    result_keys = ('time', 'low', 'high', 'open', 'close', 'volume')

    time_list = []
    data_list = []
    for item in result_list:
        temp_time = time.localtime(item[0])
        time_list.append(datetime(*temp_time[:6]))
        data_list.append(item[1:])

    df = pd.DataFrame(data=result_list, index=time_list, columns=result_keys)
    return df

if __name__ == "__main__":
    gdax_symbols = ['BTC', 'BCH', 'ETH', 'LTC']
    for i, symbol in enumerate(gdax_symbols):
        gdax_symbols[i] = symbol + '-USD'

    dfMetadata, data_list = create_metadata(tuple(gdax_symbols))

    #df = get_gdax_dataframe('ETH-USD')
