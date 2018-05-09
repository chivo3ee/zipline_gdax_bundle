#!python3
'''
Author: Joshua Goldberg

This is an extension to zipline for ingesting GDAX data to obtain crypto
currency data.
'''

import os
import time

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from zipline.utils.cli import maybe_show_progress
from zipline.data.bundles import core as bundles
import gdax

boDebug = True

def viagdax(symbols, start=None, end=None):
    symbols_tuple = tuple(symbols)

    def ingest(environ, asset_db_writer, minute_bar_writer,
               daily_bar_writer, adjustment_writer, calendar, start_session,
               end_session, cache, show_progress, output_dir):

        # TODO: Is start_session and end_session in iso format???
        dfMetadata, data_list = create_metadata(symbols_tuple, start_session,
                                                end_session, 86400)

        # TODO: Probably want to use a generator for data_list here.
        daily_bar_writer.write(data_list, show_progress=False)

def epoch_to_datetime(epoch):
    '''
    Converts epoch to python datetime format.
    '''
    temp_time = time.localtime(int(epoch))
    return datetime(*temp_time[:6])

def create_data(symbols_tuple, start, end, granularity):
    '''
    Obtains data from GDAX and places it into a format usable by zipline.

    Args:
        start: Start time in ISO 8601
        end: End time in ISO 8601
        granularity: Desired time slice in seconds. Valid values for
                     granularity are 60, 300, 900, 3600, 21600, and 86400.
    Returns:
        dfMetadata: A pandas dataframe for metadata.
        data_list: A list of pandas dataframes - one for each symbol.
    '''
    # Creating blank dfMetadata
    dfMetadata = pd.DataFrame(np.empty(len(symbols_tuple), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

    data_list = list()
    for iSid, symbol in enumerate(symbols_tuple):
        dfData = get_gdax_dataframe(symbol, start, end, granularity)
        data_list.append((iSid, dfData))

        # the start date is the date of the first trade and
        start_date = dfData.index[0]
        if boDebug:
            print("start_date", type(start_date), start_date)

        # the end date is the date of the last trade
        end_date = dfData.index[-1]
        if boDebug:
            print("end_date", type(end_date), end_date)

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)
        if boDebug:
            print( "ac_date", type(ac_date), ac_date)

        if boDebug:
            print( "symbol", type(symbol), symbol)

        # Update our meta data
        dfMetadata.iloc[iSid] = start_date, end_date, ac_date, symbol

        # GDAX will only allow 3 requests per second. This will prevent us from
        # requesting too quickly.
        time.sleep(0.350)

    # Hardcode the exchange to "GDAX" for all assets and (elsewhere) register
    # "GDAX" to resolve to the crypto currency calendar.
    # TODO: Create a crypto currency calendar. This should be easy since
    # trading is always open.
    dfMetadata['exchange'] = "GDAX"

    return dfMetadata, data_list

def get_gdax_dataframe(product_id, start, end, granularity):
    '''
    Uses the gdax python package to download data from GDAX and place it into a
    pandas dataframe.

    Args:
        start: Start time in ISO 8601
        end: End time in ISO 8601
        granularity: Desired time slice in seconds. Valid values for
                     granularity are 60, 300, 900, 3600, 21600, and 86400.
    Returns:
        df: The GDAX information for the product_id in a pandas dataframe.
    '''
    public_client = gdax.PublicClient()
    result_list = public_client.get_product_historic_rates(product_id,
                                                           start=start,
                                                           end=end,
                                                           granularity=granularity)
    result_keys = ('time', 'low', 'high', 'open', 'close', 'volume')

    time_list = []
    data_list = []
    for item in result_list:
        #temp_time = time.localtime(item[0])
        #time_list.append(datetime(*temp_time[:6]))
        time_list.append(epoch_to_datetime(item[0]))
        data_list.append(item[1:])

    df = pd.DataFrame(data=result_list, index=time_list, columns=result_keys)
    return df

if __name__ == "__main__":
    gdax_symbols = ['BTC', 'BCH', 'ETH', 'LTC']
    for i, symbol in enumerate(gdax_symbols):
        gdax_symbols[i] = symbol + '-USD'

    yesterday = datetime.today().date() - timedelta(days=1)
    yesterday_iso = yesterday.isoformat()

    now = datetime.now()
    now_iso = now.isoformat()

    dfMetadata, data_list = create_data(tuple(gdax_symbols), yesterday_iso, now_iso, 86400)
    df = get_gdax_dataframe('ETH-USD', yesterday_iso, now_iso, 86400)
