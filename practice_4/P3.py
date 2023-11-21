import sqlite3
from sqlite3 import Error
import numpy as np
import json
import csv
from aux_func import *


varian = 40
filename1 = "tests/3/task_3_var_40_part_1.csv"
filename2 = "tests/3/task_3_var_40_part_2.json"

dbname = "tests/3/ex3.db"
json_out = "tests/3/{}.json"


def parse_data(f1, f2):

    data = []
    with open(f2, mode='r') as json_f:
        data = json.load(json_f)
        
        for d in data:
            d['year'] = int(d['year'])
            d['duration_ms'] = int(d['duration_ms'])
            d['energy'] = 0
            d['key'] = 0
            d['loudness'] = 0

    csv_del = ';'
    with open(f1, mode='r') as csv_f:
        csvr = csv.DictReader(csv_f, delimiter=csv_del)

        for row in csvr:
            nd = {'artist' : "", 'song' : "", 'duration_ms' : "", 
                  'year' : "", 'tempo' : "",  'genre' : "",
                  'explicit' : 0, 'popularity' : 0, 'danceability' : 0,
                  'energy' : "", 'key' : "", 'loudness' : ""}

            nd.update(row)

            nd['year'] = int(nd['year'])
            nd['duration_ms'] = int(nd['duration_ms'])

            data.append(nd)

    return data

if __name__ == "__main__":

    table_name = 'music'
    conn, cursor = create_connection(dbname)

    try:
        cursor.execute(f'DROP TABLE {table_name}')
    except sqlite3.OperationalError:
        print("No table")

    try:
        cursor.execute('CREATE TABLE {} \
            	(id INTEGER PRIMARY KEY, \
                   artist TEXT, \
                   song TEXT, \
                   duration_ms INTEGER, \
                   year INTEGER, \
                   tempo TEXT, \
                   genre TEXT, \
                   explicit INTEGER, \
                   popularity INTEGER, \
                   danceability REAL, \
                   energy REAL, \
                   key INTEGER, \
                   loudness REAL)'.format(table_name))
    except sqlite3.OperationalError:
        print("Table exist")

    data = parse_data(filename1, filename2)
    fill_database(conn, cursor, data, table_name)

    ex1 = json_out.format("ex1")
    get_data_limit(conn, cursor, table_name, varian + 10, 'year', ex1)

    print_sum_min_max(conn, cursor, table_name, 'duration_ms')

    ex3 = json_out.format("ex3")
    print_freqes(conn, cursor, table_name, 'artist', ex3)

    ex4 = json_out.format("ex4")
    get_data_filtered(conn, cursor, table_name, 300000, 'duration_ms', varian + 15, 'song', ex4)

    conn.close()

