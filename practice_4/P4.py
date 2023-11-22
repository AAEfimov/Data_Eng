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

    return data[1:]

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
            response = cursor.execute(f"UPDATE {table_name} SET isAvailable = ?, counter = counter + 1 WHERE name == ?", ['True' if d['param'] else 'False', d['name']])
        elif d['method'] == 'quantity_sub':
            response = cursor.execute(f"UPDATE {table_name} SET quantity = MAX(quantity - ?, 0), counter = counter + 1 WHERE name = ?", [int(d['param']) , d['name']])
        elif d['method'] == 'quantity_add':
            response = cursor.execute(f"UPDATE {table_name} SET quantity = quantity + ?, counter = counter + 1 WHERE name = ?", [int(d['param']), d['name']])
        elif d['method'] == 'price_percent':
            response = cursor.execute(f"UPDATE {table_name} SET price = ROUND(price * (1 + ?), 2), counter = counter + 1 WHERE name = ?", [float(d['param']), d['name']])
        elif d['method'] == 'remove':
            response = cursor.execute(f"DELETE FROM {table_name} WHERE name = ?" ,[d['name']])
        elif d['method'] == 'price_abs':
            response = cursor.execute(f"UPDATE {table_name} SET price = MAX(price + ?, 0), counter = counter + 1 WHERE name = ?", [int(d['param']), d['name']])
        else:
            print("UNK method ", d['method'])

    conn.commit()

def get_top_updated(conn, cursor, table_name):
    response = cursor.execute(f"SELECT name, counter FROM {table_name} ORDER BY counter DESC LIMIT 10")
    write_data_to_json([dict(r) for r in response.fetchall()], json_out.format("ex1"))


def get_prices_by_category(conn, cursor, table_name):
    response = cursor.execute(f"SELECT category, COUNT(*) as cnt, MAX(price) as max, MIN(price) as min , \
                               ROUND(SUM(price), 2) as sum, ROUND(AVG(price), 2) as avg FROM {table_name} GROUP BY category")
    
    write_data_to_json([dict(r) for r in response.fetchall()], json_out.format("ex2"))

def get_quantity_by_category(conn, cursor, table_name):
    response = cursor.execute(f"SELECT category, MAX(quantity) as max, MIN(quantity) as min , \
                               SUM(quantity) as sum, AVG(quantity) as avg FROM {table_name} GROUP BY category")
    
    write_data_to_json([dict(r) for r in response.fetchall()], json_out.format("ex3"))

def get_data_from_citys(conn, cursor, table_name, city_list):
    placeholder = ' or '.join(['fromCity = ?'] * len(city_list))
    response = cursor.execute(f"SELECT fromCity, MAX(views) as max, MIN(views) as min FROM {table_name} WHERE {placeholder} GROUP BY fromCity", city_list)

    write_data_to_json([dict(r) for r in response.fetchall()], json_out.format("ex4"))

if __name__ == "__main__":

    table_name = 'products'
    conn, cursor = create_connection(dbname)

    try:
        cursor.execute(f'DROP TABLE {table_name}')
    except sqlite3.OperationalError:
        print("No table")

    try:
        cursor.execute('CREATE TABLE {} \
            	(id INTEGER PRIMARY KEY AUTOINCREMENT, \
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


    get_top_updated(conn, cursor, table_name)
    get_prices_by_category(conn, cursor, table_name)
    get_quantity_by_category(conn, cursor, table_name)
    get_data_from_citys(conn, cursor, table_name, ['Тбилиси', 'Москва', 'Тирана'])

    conn.close()
