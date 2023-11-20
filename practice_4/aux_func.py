
import sqlite3
from sqlite3 import Error
import json

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

def print_sum_min_max(conn, cursor, table_name, fild):
    result = cursor.execute(f"SELECT MIN({fild}) as min, MAX({fild}) as max, SUM({fild}) as sum, COUNT({fild}) as cnt, AVG({fild}) as avg from {table_name}")
    d = dict(result.fetchone())
    print("rating results : ", d)

def write_data_to_json(data, filename):
    with open(filename, mode='w') as f_json:
        json.dump(data, f_json)

def print_freqes(conn, cursor, table_name, fild, json_filename):
    result = cursor.execute(f"SELECT {fild}, COUNT(*) as cnt from {table_name} GROUP BY {fild}")
    write_data_to_json([dict(r) for r in result.fetchall()], json_filename)

def get_data_limit(conn, cursor, table_name, val, ord_fild, json_filename):
    # DESC for UP to DOWN
    result = cursor.execute(f"SELECT * FROM {table_name} ORDER BY {ord_fild} LIMIT {val} ")
    write_data_to_json([dict(r) for r in result.fetchall()], json_filename)

def get_data_filtered(conn, cursor, table_name, filter_cnt, filter_fild, out_cnt, sort_fild, json_filename):
    result = cursor.execute(f"SELECT * FROM {table_name} WHERE {filter_fild} > {filter_cnt} ORDER BY {sort_fild} DESC LIMIT {out_cnt}")
    write_data_to_json([dict(r) for r in result.fetchall()], json_filename)
