

import sqlite3
from sqlite3 import Error
import numpy as np
import json
from aux_func import *


varian = 40
filename = "tests/1/task_1_var_40_item.text"
dbname = "tests/1/ex1.db"

json_out = "tests/1/{}.json"

def parse_data(file):

    with open(file, mode="r") as f:
        lines = f.readlines()

    elist = []
    item = {}
    for l in lines:
        if '=====' in l:
            item['pages'] = int(item['pages'])
            item['published_year'] = int(item['published_year'])
            item['rating'] = float(item['rating'])
            item['views'] = int(item['views'])
 
            elist.append(item)
            item = {}
        else:
            els = l.split("::")
            item[els[0].strip()] = els[1].strip()

    return elist

if __name__ == "__main__":
    table_name = 'books'   
    conn, cursor = create_connection(dbname)

    try:
        cursor.execute(f'DROP TABLE {table_name}')
    except sqlite3.OperationalError:
        print("No table")

    try:
        cursor.execute('CREATE TABLE {} \
            	(id INTEGER PRIMARY KEY AUTOINCREMENT, \
                   title TEXT, \
                   author TEXT, \
                   genre TEXT, \
                   pages INTEGER, \
                   published_year INTEGER, \
                   isbn TEXT, \
                   rating REAL, \
                   views INTEGER)'.format(table_name))
    except sqlite3.OperationalError:
        print("Table exist")

    data = parse_data(filename)
    fill_database(conn, cursor, data, table_name)

    ex1 = json_out.format("ex1")
    get_data_limit(conn, cursor, table_name, varian + 10,  'published_year', ex1)

    print_sum_min_max(conn, cursor, table_name, 'rating')

    ex3 = json_out.format("ex3")
    print_freqes(conn, cursor, table_name, 'author', ex3)

    ex4 = json_out.format("ex4")
    get_data_filtered(conn, cursor, table_name, 35000, 'views', varian + 10, 'pages', ex4)

    conn.close()



