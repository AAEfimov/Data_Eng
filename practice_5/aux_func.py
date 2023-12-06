
import sqlite3
from sqlite3 import Error
import json
import os
from zipfile import ZipFile
from pymongo import MongoClient

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

    with open(filename, mode='w') as f_json:
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