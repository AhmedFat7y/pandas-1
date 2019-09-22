import os
import re
from re import Pattern
import multiprocessing
import pandas as pd
import networkx
from typing import Any, Dict, Callable
import dask.dataframe as dd


def get_cpu_count() -> int:
    try:
        cpus = multiprocessing.cpu_count()
    except NotImplementedError:
        cpus = 2   # arbitrary default
    return cpus


def construct_regex_from_list(str_list: [str]) -> Pattern:
    strs_joined = '|'.join(str_list)
    str_regex = re.compile(
        '(?P<name>(?P<from>' + strs_joined + ')(?P<to>' + strs_joined + '))')
    return str_regex


def extract_keys_from_obj(obj: Dict[str, Any], obj_keys_map: Dict[str, str]) -> pd.Series:
    return pd.Series(data={obj_keys_map[k]: obj[k]
                           for k in list(obj_keys_map.keys())})


def extract_required_data_points_wrapper(obj_keys_map: Dict[str, str], filename: str) -> pd.DataFrame:
    filename2, ext = os.path.splitext(filename)
    return (pd.read_json(filename, orient='records')
            .stack()
            .transform(extract_keys_from_obj, obj_keys_map=obj_keys_map)
            .reset_index()
            .rename(columns={'level_0': 'event_id'})
            .drop('level_1', axis=1))


def get_cycles(ddf: dd.DataFrame) -> pd.DataFrame:
    '''
        The group id (event_id) can be accessed by ddf.name
        For some reason, the event_id column doesn't exist
    '''
    graph = networkx.from_pandas_edgelist(ddf, source='from', target='to', edge_attr=[
                                          'cost', 'quantity'], create_using=networkx.DiGraph)
    cycles_list_generator = networkx.simple_cycles(graph)
    cycles_list = [dict(
        currency1=cycle[0],
        quantity1=graph[cycle[0]][cycle[1]]['quantity'],
        cost1=graph[cycle[0]][cycle[1]]['cost'],
        currency2=cycle[1],
        quantity2=graph[cycle[1]][cycle[2]]['quantity'],
        cost2=graph[cycle[1]][cycle[2]]['cost'],
        currency3=cycle[2],
        quantity3=graph[cycle[2]][cycle[0]]['quantity'],
        cost3=graph[cycle[2]][cycle[0]]['cost'])
        for cycle in cycles_list_generator if len(cycle) == 3]
    if (len(cycles_list)):
        return pd.DataFrame(data=cycles_list)
    else:
        return None
