import sqlite3
from sqlite3 import Error
import numpy as np
import json
import csv
import pickle
from aux_func import *


varian = 40
filename1 = "tests/4/task_4_var_40_product_data.csv"
filename2 = "tests/4/task_4_var_40_update_data.pkl"

dbname = "tests/4/ex4.db"
json_out = "tests/4/{}.json"


def parse_data(fn):

    csv_del = ';'
    data = []
    with open(fn, mode='r') as csv_f:
        scvr = csv.reader(csv_f, delimiter=csv_del)
        for d in scvr:
            if d:
                nd = {'name' : d[0], 'price' : d[1], 'quantity' : d[2], 'category' : "", 'fromCity' : d[-3], 'isAvailable' : d[-2], 'views' : d[-1], 'counter' : 0}
                if len(d) == 7:
                    nd['category'] = d[3]

                data.append(nd)

    return data

def parse_upd_date(fn):
    with open(fn, mode='rb') as pkl_f:
        data = pickle.load(pkl_f)

    return data

#    s = set()
#    for d in data:
#        s.add(d['method'])       
#    print(s)
# {'available', 'quantity_sub', 'quantity_add', 'price_percent', 'remove', 'price_abs'}

def update_database(conn, cursor, table_name, data):
    for d in data:
        if d['method'] == 'available':
            response = cursor.execute(f"UPDATE {table_name} SET isAvailable = ? WHERE name == ?", [d['param'], d['name']])
        elif d['method'] == 'quantity_sub':
            response = cursor.execute("SELECT")
            pass
        elif d['method'] == 'quantity_add':
            pass
        elif d['method'] == 'price_percent':
            pass
        elif d['method'] == 'remove':
            pass
        elif d['method'] == 'price_abs':
            pass
        else:
            print("UNK method ", d['method'])

    conn.commit()

if __name__ == "__main__":

    table_name = 'products'
    conn, cursor = create_connection(dbname)

    cursor.execute(f'DROP TABLE {table_name}')

    try:
        cursor.execute('CREATE TABLE {} \
            	(id INTEGER PRIMARY KEY, \
                   name TEXT, \
                   price INTEGER, \
                   quantity INTEGER, \
                   category TEXT, \
                   fromCity TEXT, \
                   isAvailable INTEGER, \
                   views INTEGER, \
                   counter INTEGER)'.format(table_name))
    except sqlite3.OperationalError:
        print("Table exist")

    data = parse_data(filename1)
    fill_database(conn, cursor, data, table_name)

    upd_data = parse_upd_date(filename2)

    update_database(conn, cursor, table_name, upd_data)

    conn.close()