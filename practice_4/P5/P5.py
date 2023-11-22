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

json_out = "request_out/{}_out.json"

def get_chars_with_trolley(conn, cursor, limit = 10):
    result = cursor.execute("SELECT * FROM chars WHERE trolley == 1 ORDER BY def DESC LIMIT ?", [limit])
    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q1"))

def count_char_by_weapon(conn, cursor):
    result = cursor.execute("SELECT weapon_name, COUNT(*) AS count FROM chars GROUP BY weapon_name ORDER BY count DESC")
    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q2"))

def get_axe_weapon_names_by_chars(conn, cursor):
    result = cursor.execute("SELECT name, weapon_id, (SELECT name FROM Axe WHERE id = weapon_id ) AS Axe_name FROM chars WHERE weapon_name = 'Axe'")
    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q3"))


def get_chars_with_max_atk_mace(conn, cursor):
    # default Atk + Weapon Atk1
    """
    result = cursor.execute("SELECT name, weapon_name, weapon_id, Atk,  \
                             (SELECT Atk FROM Mace WHERE id = weapon_id) AS wepon_atk, \
                             Atk + (SELECT Atk FROM Mace WHERE id = weapon_id) as total_atk \
                             FROM chars WHERE weapon_name = 'Mace' ORDER BY total_atk DESC")
    """
    # NEW FORMAT
    result = cursor.execute("""SELECT 
                                    chars.name, 
                                    chars.weapon_name, 
                                    chars.weapon_id, 
                                    chars.Atk, 
                                    Mace.Atk as Weapon_atk,
                                    chars.Atk + Mace.Atk as total_atk 
                             FROM chars 
                                JOIN Mace 
                            WHERE weapon_name = 'Mace' 
                                AND chars.weapon_id = Mace.id  
                            ORDER BY total_atk DESC""")
          
    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q4"))

def get_chars_def(conn, cursor):

    """
    result = cursor.execute("SELECT name, Def, Head as head_id, Bracelet as brace_id, Armour as arm_id, Shoes as shoes_id , Robe as robe_id, Accessory as acc_id, \
                            Def + (SELECT Def FROM Head WHERE id = Head) + \
                            (SELECT Bracelet FROM Head WHERE id = Bracelet) + \
                            (SELECT Armour FROM Head WHERE id = Armour) + \
                            (SELECT Shoes FROM Head WHERE id = Shoes) + \
                            (SELECT Robe FROM Head WHERE id = Robe) + \
                            (SELECT Accessory FROM Head WHERE id = Accessory)\
                            AS Total_def FROM chars ORDER BY Total_def DESC")
    """

    result = cursor.execute("""SELECT chars.name, 
                                    chars.Def, 
                                    Head.Def as Head_def, 
                                    Bracelet.Def as Bracelet_def, 
                                    Armour.Def as Armour_def, 
                                    Shoes.Def as Shoes_def, 
                                    Robe.Def as Robe_def , 
                                    Accessory.Def as Accessory_def, 
                                    chars.Def + Head.Def +  Bracelet.Def + Armour.Def + Shoes.Def + Robe.Def + Accessory.Def AS Total_def 
                            FROM chars 
                                    JOIN Head 
                                    JOIN Bracelet 
                                    JOIN Armour 
                                    JOIN Shoes 
                                    JOIN Robe 
                                    JOIN Accessory 
                            WHERE 
                                Head.id = chars.Head 
                                AND Bracelet.id = chars.Bracelet 
                                AND Armour.id = chars.Armour 
                                AND Shoes.id = chars.Shoes 
                                AND Robe.id = chars.Robe 
                                AND Accessory.id = chars.Accessory \
                            ORDER BY Total_def DESC""")

    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q5"))

def get_chars_all_armour_names(conn, cursor, limit = 20):

    """
    result = cursor.execute("SELECT name, Head as head_id, Bracelet as brace_id, Armour as arm_id, Shoes as shoes_id , Robe as robe_id, Accessory as acc_id, \
                            (SELECT name FROM Head WHERE id = Head) as nHead, \
                            (SELECT name FROM Head WHERE id = Bracelet) as nBracelet, \
                            (SELECT name FROM Head WHERE id = Armour) as nArmour, \
                            (SELECT name FROM Head WHERE id = Shoes) as nShoes, \
                            (SELECT name FROM Head WHERE id = Robe) as nRobe, \
                            (SELECT name FROM Head WHERE id = Accessory) as nAccessory\
                            FROM chars ORDER BY name LIMIT  ?", [limit])
    """

    result = cursor.execute("""SELECT   chars.name, 
                                        Head.name as Head,  
                                        Bracelet.name as Bracelet, 
                                        Armour.name as Armour, 
                                        Shoes.name as Shoes,  
                                        Robe.name as Robe, 
                                        Accessory.name as Accessory
                            FROM chars 
                                    JOIN Head 
                                    JOIN Bracelet 
                                    JOIN Armour 
                                    JOIN Shoes JOIN Robe 
                                    JOIN Accessory 
                            WHERE Head.id = chars.Head 
                                    AND Bracelet.id = chars.Bracelet 
                                    AND Armour.id = chars.Armour 
                                    AND Shoes.id = chars.Shoes 
                                    AND Robe.id = chars.Robe 
                                    AND Accessory.id = chars.Accessory""")

    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q6"))


def update_chars_armour(conn, cursor):

    result = cursor.execute("UPDATE chars SET Armour = 50+RANDOM()%50")
    #cursor.commit()
    result = cursor.execute("SELECT * FROM chars")

    write_data_to_json([dict(r) for r in result.fetchall()], json_out.format("q7"))

if __name__ == '__main__':

    #generate_persons(persons_namelist, person_out)
    #create_and_fill_databases(game_databe, equipmen_zip, person_out)

    conn, cursor = create_connection(game_databe)

    get_chars_with_trolley(conn, cursor)

    count_char_by_weapon(conn, cursor)

    get_axe_weapon_names_by_chars(conn, cursor)

    get_chars_with_max_atk_mace(conn, cursor)

    get_chars_def(conn, cursor)

    get_chars_all_armour_names(conn, cursor)

    update_chars_armour(conn, cursor)

    conn.close()
