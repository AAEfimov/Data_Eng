import sqlite3
from sqlite3 import Error
import numpy as np
import json
import csv
import pickle
import re
from  aux_func  import *
import csv
import random


def get_data(filename):

    dict_data = {}

    slot_set = set()
    for cf in get_next_file_from_zip(filename, '.'):
        with open(cf, mode='r') as json_f:
            data = json.load(json_f)

        for d in data:
            slot_set.add(d['slot'])
            if d['slot'] in dict_data:
                dict_data[d['slot']].append(d)
            else:
                dict_data.setdefault(d['slot'], [d])

    return dict_data, list(slot_set)

# {'Barrow', 'Axe', 'Trolley', 'Head', 'Spear', 'Bracelet', 'Knife', 'Eikon', 'Orb', 'Katar', 'Armour', 
# 'Shoes', 'Mace', 'Tail', 'Robe', 'Mount', 'Book', 'Accessory', 'Mouth', 'Face', 'Knuckle', 'Sword', 
# 'Bow', 'Shield', 'Staff', 'mouth', 'Bracer', 'Wing'}

def get_person_data(fn):

    pl = []
    with open(fn, mode='r') as f:
        csv_r = csv.DictReader(f, delimiter=',')
        for p in csv_r:
            pl.append(p)

    return pl

def create_equip_tables(conn, cursor, slots):
    for s in slots:
        try:
            cursor.execute(f'DROP TABLE {s}')
        except sqlite3.OperationalError:
            pass

        try:
            resp = cursor.execute(f"CREATE TABLE {s} \
                              (id INTEGER PRIMARY KEY AUTOINCREMENT, \
                              name TEXT, \
                              slot TEXT, \
                              status TEXT, \
                              Def INTEGER, \
                              Atk INTEGER, \
                              Dmg INTEGER, \
                              Agi INTEGER, \
                              Str INTEGER, \
                              Luk INTEGER \
                                      )")
        except:
            pass
            #print(f"table {s} already exists")

    conn.commit()

def create_person_table(conn, cursor, tablename):

    fields = ['name','Def','Atk','Trolley','Weapon_name','Weapon_id','Head','Bracelet','Armour','Shoes','Robe','Accessory']
    try:
        cursor.execute(f'DROP TABLE {tablename}')
    except sqlite3.OperationalError:
        pass

    try:
        resp = cursor.execute(f"CREATE TABLE {tablename} \
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, \
                            name TEXT, \
                            Def INTEGER, \
                            Atk INTEGER, \
                            Trolley INTEGER, \
                            Weapon_name TEXT, \
                            Weapon_id INTEGER, \
                            Head INTEGER, \
                            Bracelet INTEGER, \
                            Armour INTEGER, \
                            Shoes INTEGER, \
                            Robe INTEGER, \
                            Accessory INTEGER \
                            )")
    except:
        print(f"table {tablename} already exists")




def add_data_to_database(conn, cursor, data):
   
    stat_set = set()

    stat_list = ['Def', 'Atk', 'Dmg', 'Agi', 'Str', 'Luk']

    dict_keys = ['name', 'slot', 'status', 'Def', 'Atk', 'Dmg', 'Agi', 'Str', 'Luk']
    db_field = dict.fromkeys(dict_keys, "")

    for d in data.items():
        for k in d[1]:
            db_field['name'] = k['name']
            db_field['slot'] = k['slot']
            db_field['status'] = k['status']
            db_field['Def'] = 0
            db_field['Atk'] = 0
            db_field['Dmg'] = 0
            db_field['Agi'] = 0
            db_field['Str'] = 0
            db_field['Luk'] = 0

            main_stat = k['status'].split(',')[0]
            numb = re.findall(r'\d+', main_stat)

            if main_stat:
                stat_name = main_stat.split()[0]
                if stat_name in stat_list:
                    db_field[stat_name] = int(numb[0])
            
            fill_database(conn, cursor, [db_field], d[0], False)  

    conn.commit()

def create_and_fill_databases(game_databe, equipmen_zip, persons):

    # Create Item DB
    conn, cursor = create_connection(game_databe)
    #data, slots = get_data(equipmen_zip)
    #create_equip_tables(conn, cursor, slots)
    #add_data_to_database(conn, cursor, data)

    person_data = get_person_data(persons)
    create_person_table(conn, cursor, 'chars')
    
    conn.close()

    return game_databe

def generate_persons(peroson_nl, person_out):
    fields = ['name','Def','Atk','Trolley','Weapon_name','Weapon_id','Head','Bracelet','Armour','Shoes','Robe','Accessory']
    wl = ['Axe', 'Spear', 'Knife', 'Katar', 'Katar', 'Mace', 'Staff', 'Sword', 'Bow', 'Knuckle']
    with open(peroson_nl, mode='r') as f, open(person_out, mode='w') as csv_f:
        cwr = csv.writer(csv_f)
        
        data = f.read().split()

        cwr.writerow(fields)
        for n in data:
            line = f"{n},{random.randint(1,10)},{random.randint(1,10)},{random.randint(0,1)},{wl[random.randint(0,len(wl) - 1)]},{random.randint(1,20)},{random.randint(1,20)},{random.randint(1,20)},{random.randint(1,20)},{random.randint(1,20)},{random.randint(1,20)},{random.randint(1,20)}"
            cwr.writerow(line.split(','))
