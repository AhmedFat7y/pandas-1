import arbitrage_using_kedro.utils as utils
import os
import re
import functools
import multiprocessing
import math
import pandas as pd
import networkx
import dask.dataframe as dd


def save_data_files_paths_to_csv(raw_folder: str) -> pd.DataFrame:
    file_names = os.listdir(raw_folder)
    file_paths = [os.path.join(raw_folder, file_name)
                  for file_name in file_names if file_name.endswith('json')]
    if not len(file_paths):
        raise Exception('No raw json files found in'.format(raw_folder))
    return pd.DataFrame(data={'file_path': file_paths})


def combine_raw_multiple_json_to_single_df(files_df: pd.DataFrame, obj_keys_map: [str]) -> pd.DataFrame:
    '''Get dataframe containg json files and needed columns'''
    cpus = utils.get_cpu_count()
    files_list = list(files_df['file_path'])
    df_list = []
    map_func = functools.partial(
        utils.extract_required_data_points_wrapper, obj_keys_map)
    with multiprocessing.Pool(cpus) as pool:
        df_list = (
            pool.map(map_func, files_list))
    for i in range(1, len(df_list)):
        pre_df = df_list[i - 1]
        df = df_list[i]
        df['event_id'] = df['event_id'] + (pre_df['event_id'].max() + 1)
    print('Read', len(df_list), 'Data Frames')
    df = pd.concat(df_list)
    print('Concatenated Them')
    return df


def generate_from_to_columns(df: pd.DataFrame, supported_currencies_list: [str]) -> pd.DataFrame:
    currency_regex = utils.construct_regex_from_list(supported_currencies_list)
    fromtodf = df['name'].str.extract(currency_regex)
    df['from'] = fromtodf['from']
    df['to'] = fromtodf['to']
    return df.dropna().reset_index().drop('index', axis=1)


def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['best_bid_price'] = df['best_bid_price'].astype(float)
    df['best_bid_quantity'] = df['best_bid_quantity'].astype(float)
    df['best_bid_price'] = df['best_bid_price'].astype(float)
    df['best_ask_price'] = df['best_ask_price'].astype(float)
    df['best_ask_quantity'] = df['best_ask_quantity'].astype(float)
    return df


def generate_edges(df: pd.DataFrame) -> pd.DataFrame:
    bids = df.copy(deep=True)
    asks = df.copy(deep=True)
    asks['from'] = bids['to']
    asks['to'] = bids['from']
    bids['cost'] = 1 / bids['best_bid_price']
    bids['quantity'] = bids['best_bid_quantity'] * bids['best_bid_price']
    asks.rename(columns={'best_ask_price': 'cost',
                         'best_ask_quantity': 'quantity'}, inplace=True)
    bids = bids[['event_id', 'from', 'to', 'cost', 'quantity']]
    asks = asks[['event_id', 'from', 'to', 'cost', 'quantity']]
    edgesdf = pd.concat([bids, asks], sort=False)
    rows_count, columns_count = edgesdf.shape
    edgesdf.index = pd.RangeIndex(start=0, stop=rows_count, step=1)
    return edgesdf


def generate_cycles_df(df: pd.DataFrame) -> pd.DataFrame:
    cpus = math.ceil(utils.get_cpu_count() * 1.5)
    cycles_df = (dd.from_pandas(df, npartitions=cpus)
                 .groupby('event_id')
                 .apply(
                     utils.get_cycles,
                     meta=dict(
                         currency1='str',
                         quantity1='float',
                         cost1='float',
                         currency2='str',
                         quantity2='float',
                         cost2='float',
                         currency3='str',
                         quantity3='float',
                         cost3='float',
                     ))
                 .compute()
                 )
    return cycles_df.reset_index(level='event_id')


def generate_arbitrage_df(df: pd.DataFrame) -> pd.DataFrame:
    cycle_length = 3
    cycle = [(df['currency' + str(i)], df['quantity' + str(i)], df['cost' + str(i)])
             for i in range(1, cycle_length + 1)]
    # append the starting currency for easier loop
    starting_currency_count = 1
    currency_count = starting_currency_count
    for item in cycle:
        name, quantity, price = item
        fee = price * ((currency_count / 100) * 0.1)
        currency_count = (currency_count * price) - fee
    arbitrage_df = df.copy(True)[['event_id', 'currency1']]
    arbitrage_df['start_quantity'] = starting_currency_count
    arbitrage_df['final_quantity'] = currency_count
    arbitrage_df['diff'] = ((arbitrage_df['final_quantity'] -
                             arbitrage_df['start_quantity']) / arbitrage_df['start_quantity'])
    return arbitrage_df
