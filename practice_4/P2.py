import sqlite3
from sqlite3 import Error
import numpy as np
import json
import csv
import pickle
import os
from aux_func import *

varian = 40
filename = "tests/2/task_2_var_40_subitem.csv"

dbname_1 = "tests/1/ex1.db"
dbname = "tests/2/ex2.db"
json_out = "tests/2/{}.json"



def read_data(fn):
    csv_del = ';'
    data = []
    with open(fn, mode='r') as csv_f:
        csvr = csv.DictReader(csv_f, delimiter=csv_del)
        for r in csvr:
            data.append(r)

    return data

# title;price;place;date

if __name__ == "__main__":

    table_name = 'books_up'
    conn, cursor = create_connection(dbname)
    conn1, cursor1 = create_connection(dbname_1)

    cursor.execute(f'DROP TABLE {table_name}')

    try:
        cursor.execute('CREATE TABLE {} \
            	(id INTEGER PRIMARY KEY, \
                   title TEXT, \
                   price INTEGER, \
                   place TEXT, \
                   date TEXT)'.format(table_name))
    except sqlite3.OperationalError:
        print("Table exist")


    data = read_data(filename)
    fill_database(conn, cursor, data, table_name)

    # get all books pricy by authors
    
    result = cursor1.execute("SELECT ")

