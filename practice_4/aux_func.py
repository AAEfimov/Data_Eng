
import sqlite3
from sqlite3 import Error
import json

def create_connection(db_file):
    connection = sqlite3.connect(db_file)
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
    result = cursor.execute(f"SELECT MIN({fild}) as min_v, MAX({fild}) as max_v, SUM({fild}) as sum, COUNT({fild}) as cnt from {table_name}")
    res = list(*result)
    print("rating results")
    print(f"MIN {res[0]}, MAX {res[1]}, SUM {round(res[2], 2)}, CNT {res[3]} AVG {round(res[2] / res[3], 2)}")

def print_freqes(conn, cursor, table_name, fild):
    
    df = {}
    res = cursor.execute(f"SELECT {fild} from {table_name}")
    for f in res.fetchall():
        add_to_dict(f[0], df)
    print(df)

def get_data_limit(conn, cursor, table_name, val, ord_fild, json_filename):
    # DESC for UP to DOWN
    result = cursor.execute("SELECT * FROM {} ORDER BY {} LIMIT {} ".format(table_name, ord_fild, val))
    with open(json_filename, mode='w') as f_json:
        json.dump([*result], f_json)


def get_data_filtered(conn, cursor, table_name, filter_cnt, filter_fild, out_cnt, sort_fild, json_filename):
    result = cursor.execute(f"SELECT * FROM {table_name} WHERE {filter_fild} > {filter_cnt} ORDER BY {sort_fild} DESC LIMIT {out_cnt}")
    with open(json_filename, mode='w') as f_json:
        json.dump([*result], f_json)