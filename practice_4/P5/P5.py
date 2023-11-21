import sqlite3
from sqlite3 import Error
import numpy as np
import json
import csv
import pickle
import re
from  aux_func  import *
from create_dbs import *
import random

equipmen_zip = 'equipment-database.zip'
game_databe = 'game.db'

persons_namelist = 'pers_names.txt'
person_out = 'persons.csv'

if __name__ == '__main__':

    #generate_persons(persons_namelist, person_out)
    #create_and_fill_databases(game_databe, equipmen_zip, person_out)

    conn, cursor = create_connection(game_databe)

    conn.close()
