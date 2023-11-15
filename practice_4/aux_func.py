
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    return connection, cursor


