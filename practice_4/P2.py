import sqlite3
from sqlite3 import Error
import numpy as np
import json
import csv
import pickle
import os
from aux_func import *

varian = 40
dbname_1 = "tests/1/ex1.db"

filename = "tests/2/task_2_var_40_subitem.csv"
json_out = "tests/2/{}.json"

def read_data(fn):
    csv_del = ';'
    data = []
    with open(fn, mode='r') as csv_f:
        csvr = csv.DictReader(csv_f, delimiter=csv_del)
        for r in csvr:
            #r['books_id'] = f'SELECT id from books WHERE title = :title)'
            data.append(r)

    return data

def fill_database_2(conn, cursor, data):
    response = cursor.executemany("""
                    INSERT INTO books_up (books_id, title, price, place, date)
                    VALUES(
                              (SELECT id from books WHERE title = :title),
                              :title, :price, :place, :date
                            )
                         """, data)
    conn.commit()
 
def get_authors_books(conn, cursor, author): 
    result = cursor.execute("SELECT * FROM books_up WHERE books_id = (SELECT id FROM books WHERE author = ?)", [author])
    ex1 = json_out.format("ex1")
    write_data_to_json([dict(r) for r in result.fetchall()], ex1)

def get_avg_price(conn, cursor, author, online): 
    result = cursor.execute("SELECT AVG(price) as avg, MIN(price) as min, MAX(price) as max \
                            FROM books_up WHERE books_id = (SELECT id FROM books WHERE author = ?) AND place = ?", 
                            [author, online])

    d = dict(result.fetchone())
    print(d)    

def get_books_count_by_all(conn, cursor):
    result = cursor.execute("""SELECT id, author,
                            (SELECT COUNT(*) FROM books_up WHERE books_id = books.id) as counter
                            FROM books
                            ORDER BY counter DESC""")
    
    for d in result.fetchall():
        print(dict(d))

def get_books_count(conn, cursor, author):
    result = cursor.execute("""SELECT title, COUNT(title) as counter
                            FROM books_up WHERE books_id = (SELECT id FROM books WHERE author = ?)
                            ORDER BY counter DESC""", [author])
    
    for d in result.fetchall():
        print(dict(d))

if __name__ == "__main__":

    table_name = 'books_up'
    conn, cursor = create_connection(dbname_1)

    try:
        cursor.execute(f'DROP TABLE {table_name}')
    except sqlite3.OperationalError:
        print("No table")

    try:
        cursor.execute('CREATE TABLE {} \
            	(id INTEGER PRIMARY KEY AUTOINCREMENT, \
                   books_id REFERENCES books (id), \
                   title TEXT, \
                   price INTEGER, \
                   place TEXT, \
                   date TEXT)'.format(table_name))
    except sqlite3.OperationalError:
        print("Table exist")


    data = read_data(filename)
    fill_database_2(conn, cursor, data)

    get_authors_books(conn, cursor, 'Герберт Уэллс')
    get_avg_price(conn, cursor, 'Джордж Оруэлл', 'online')
    get_books_count_by_all(conn, cursor)

