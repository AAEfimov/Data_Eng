
import sqlite3
from sqlite3 import Error
import json
import os
from zipfile import ZipFile
from pymongo import MongoClient
import pandas as pd
import numpy as np 

def create_connection(db_file):
    connection = sqlite3.connect(db_file)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    return connection, cursor

def fill_database(conn, cursor, data, table_name):
    columns = ', '.join(data[0].keys())
    placeholders = ', '.join('?' * len(data[0]))
    sql_request = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, columns, placeholders)

    for d in data:
        values = [x for x in d.values()]
        cursor.execute(sql_request, values)

    conn.commit()

def add_to_dict(m, fdict):
    m = m.lower()
    if m in fdict:
        fdict[m] += 1
    else:
        fdict.setdefault(m, 1)

def dict_filter_gt(val, fil):
    return lambda x: x[fil] > val

def dict_filter_lt(val, fil):
    return lambda x: x[fil] < val

def write_data_to_json(data, filename):
    ext_dir = os.path.dirname(os.path.abspath(filename))
    if not os.path.exists(ext_dir):
        os.mkdir(ext_dir)

    with open(filename, mode='w', encoding="utf-8") as f_json:
        json.dump(data, f_json, default=str, ensure_ascii=False)

def print_sum_min_max(conn, cursor, table_name, fild, json_filename):
    result = cursor.execute(f"SELECT MIN({fild}) as min, MAX({fild}) as max, SUM({fild}) as sum, COUNT({fild}) as cnt, AVG({fild}) as avg from {table_name}")
    d = dict(result.fetchone())
    print("rating results : ", d)
    write_data_to_json(d, json_filename)

def print_freqes(conn, cursor, table_name, fild, json_filename):
    result = cursor.execute(f"SELECT {fild}, COUNT(*) as cnt from {table_name} GROUP BY {fild}")
    write_data_to_json([dict(r) for r in result.fetchall()], json_filename)

def get_data_limit(conn, cursor, table_name, val, ord_fild, json_filename):
    # DESC for UP to DOWN
    result = cursor.execute(f"SELECT * FROM {table_name} ORDER BY {ord_fild} LIMIT ?", [val])
    write_data_to_json([dict(r) for r in result.fetchall()], json_filename)

def get_data_filtered(conn, cursor, table_name, filter_cnt, filter_fild, out_cnt, sort_fild, json_filename):
    result = cursor.execute(f"SELECT * FROM {table_name} WHERE {filter_fild} > ? ORDER BY {sort_fild} DESC LIMIT ?", [filter_cnt, out_cnt])
    write_data_to_json([dict(r) for r in result.fetchall()], json_filename)


def get_next_file_from_zip(zip_arch, fp):
    with ZipFile(zip_arch, 'r') as zf:
        ext_dir = fp + "/extract"
        if not os.path.exists(ext_dir):
            os.mkdir(ext_dir)
        
        for f in zf.infolist():
            zf.extract(f.filename, ext_dir)
            ext_filename = os.path.join(ext_dir, f.filename)

            yield ext_filename

def connect_mongo(dbname):
    client = MongoClient()
    db = client[dbname]

    return db

def insert_data_mongo(connection, data):
    res = connection.insert_many(data)


def round_field_mongo(collection, field):
    data = collection.find()

    for p in data:
        p[field] = round(p[field], 2)

    collection.save(data)


"""_summary_
PANDAS DATASET FUNCTIONS
"""

def mem_usage(df):
    ub = df.memory_usage(deep = True)
    if isinstance(df, pd.DataFrame):
        ub = ub.sum()

    umb = ub / 1024 ** 2
    return umb

def mem_usage2(df):
    if isinstance(df, pd.DataFrame):
        ub = ub = df.memory_usage(deep = True).sum()
    else:
        ub = df.memory_usage(deep = True)

    umb = ub / 1024 ** 2
    return "{:02.2f} MB".format(umb)
    

def evaluate_memory(df, file, outfile, file_suffix = 'noopt'):

    d = {}
    fsz = os.path.getsize(file)
    mem_stat = df.memory_usage(deep = True)
    total_mem_usage = mem_stat.sum()

    d["file_size"] = int(fsz) // 1024
    d["in_memory_size"] = int(total_mem_usage) // 1024

    column_stats = []
    for key in df.dtypes.keys():
        column_stats.append({
            "column_name" : key,
            "memory_abs" : int(mem_stat[key]) // 1024,
            "memory_percent" : round((mem_stat[key] / total_mem_usage) * 100, 4),
            "dtype" : df.dtypes[key]

        })

    column_stats.sort(key = lambda x : x["memory_abs"], reverse = True)

    # INFO
    # RAM
    # df.info(memory_usage = 'deep')
    # size of dataset
    # print(df.shape)

    d["shape"] = df.shape

    types_stats = []
    for dtype in ['float', 'int', 'object']:
        selected_dtype = df.select_dtypes(include=[dtype])
        mem_usage_bytes = selected_dtype.memory_usage(deep=True).mean()
        mem_usage_mb = round(mem_usage_bytes / 1024**2, 4)
        types_stats.append({
                f"{dtype} use, MB" : mem_usage_mb
        })

    d["types_stat"] = types_stats
    d["column_stats"] = column_stats

    write_data_to_json(d, outfile.format(f"memusage_{file_suffix}"))

def convert_object_datatypes(df, outfile):

    # INFO
    # category_example
    # dow = df.day_of_week
    # print(mem_usage(dow))
    # opt_dow = dow.astype('category')
    # print(mem_usage(opt_dow))

    # Вывод соответсвий категорий и исходных значений
    # print(opt_dow.head().cat.codes)

    # objects
    # df_obj = df.select_dtypes(include=['object']).copy()
    # print(df_obj.describe())

    conv_df = pd.DataFrame()
    df_obj = df.select_dtypes(include=['object']).copy()

    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        if (num_unique_values / num_total_values) < 0.5:
            conv_df.loc[:, col] = df_obj[col].astype('category')
        else:
            conv_df.loc[:, col] = df_obj[col]

    print(mem_usage(df_obj))
    print(mem_usage(conv_df))

    d = {}
    d['objects_size'] = round(mem_usage(df_obj), 2)
    d['objects_astype'] = round(mem_usage(conv_df), 2)

    write_data_to_json(d, outfile.format("objects_memopt"))

    return conv_df


def type_size():
    int_types = ['uint8', 'int8', 'int16']
    for i in int_types:
        print(np.iinfo(i))

"""
‘integer’ or ‘signed’: smallest signed int dtype (min.: np.int8)

‘unsigned’: smallest unsigned int dtype (min.: np.uint8)

‘float’: smallest float dtype (min.: np.float32)
"""       

def int_downcast(df, outfile):


    df_int = df.select_dtypes(include=['int'])
    df_int_downcast = df_int.apply(pd.to_numeric, downcast='unsigned')
    print(mem_usage(df_int))
    print(mem_usage(df_int_downcast)) 

    compare_ints = pd.concat([df_int.dtypes, df_int_downcast.dtypes], axis = 1)
    compare_ints.columns = ['before', 'after']
    compare_ints.apply(pd.Series.value_counts)
    print(compare_ints)

    d = {}
    d['df_int_size'] = round(mem_usage(df_int), 2)
    d['df_int_downcast_size'] = round(mem_usage(df_int_downcast), 2)
    d['type_conversion'] = compare_ints.to_dict()

    write_data_to_json(d, outfile.format("int_downcast"))

    return df_int_downcast


def float_downcast(df, outfile):
    df_float = df.select_dtypes(include=['float'])
    df_float_downcast = df_float.apply(pd.to_numeric, downcast='float')
    print(mem_usage(df_float))
    print(mem_usage(df_float_downcast))
    compare_floats = pd.concat([df_float.dtypes, df_float_downcast.dtypes], axis = 1)
    compare_floats.columns = ['before', 'after']
    compare_floats.apply(pd.Series.value_counts)
    print(compare_floats)

    d = {}
    d['df_float_size'] = round(mem_usage(df_float), 2)
    d['df_float_downcast_size'] = round(mem_usage(df_float_downcast), 2)
    d['type_conversion'] = compare_floats.to_dict()

    write_data_to_json(d, outfile.format("float_downcast"))

    return df_float_downcast

# experemental
def df_type_downcast(df, df_type_from, df_type_to, outfile):
    df_type = df.select_dtypes(include=[df_type_from])
    df_type_downcast = df_type.apply(pd.to_numeric, downcast=df_type_to)
    print(mem_usage(df_type))
    print(mem_usage(df_type_downcast))
    compare_types = pd.concat([df_type.dtypes, df_type_downcast.dtypes], axis = 1)
    compare_types.columns = ['before', 'after']
    compare_types.apply(pd.Series.value_counts)
    print(compare_types)

    d = {}
    d[f'df_{df_type_from}_size'] = round(mem_usage(df_type), 2)
    d[f'df_{df_type_from}_downcast_size'] = round(mem_usage(df_type_downcast), 2)
    d['type_conversion'] = compare_types.to_dict()

    write_data_to_json(d, outfile.format(f"{df_type_from}_downcast"))

    return df_type_downcast

def chunks_read(filename, cols, types_list, datafiles_out, outfilename, ch_size=100_000):

    outf = datafiles_out.format(outfilename)
    if os.path.isfile(outf):
        os.remove(outf)

    for chunk in pd.read_csv(filename, usecols = lambda x : x in cols, dtype=types_list, chunksize=ch_size, parse_dates=['date'], infer_datetime_format=True):
        print(f"cunk_mem {mem_usage(chunk)}")
        # Добавляя через mode='a' мы каждый чанк записываем строку, которая описывает колонки
        # cols в данном случае

        chunk.to_csv(outf, mode="a")
