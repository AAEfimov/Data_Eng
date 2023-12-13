import os
import json
import pprint
import pickle
import csv
from aux_func import *
from P4_selection_1 import *
from P4_selection_2 import *
from P4_selection_3 import *

maindir = "tests"
filename1 = "Crash_Reporting.csv"
filename2 = "Crash_Reporting2.json"

datafile1 = os.path.join(maindir, filename1)
datafile2 = os.path.join(maindir, filename2)

# Use database from T1
database = "var40_task4"

json_out = "tests/out4/{}.json"

pp = pprint.PrettyPrinter()

def parse_data_csv(datafile):
    with open(datafile, mode='r', encoding="utf-8") as csv_f:
        csvr = csv.DictReader(csv_f, delimiter=',')
        data = []
        for row in csvr:
            data.append(row)

    return data


def parse_data_json(datafile):
    with open(datafile, mode='r', encoding="utf-8") as f:
        data = json.load(f)
    return data

if __name__ == "__main__":

    db = connect_mongo(database)
    collection = db.person

    # col_names = db.list_collection_names()

    # if col_names:
    #     for n in col_names:
    #         db.drop_collection(n)

    # data1 = parse_data_csv(datafile1)
    # data2 = parse_data_json(datafile2)

    # for d in (data1 + data2):

    #     vy = int(d['Vehicle Year'])
    #     if vy > 2023 :
    #         vy = 2023
    #     elif vy == 0:
    #         vy = 2000

    #     d['Vehicle Year'] = vy
        
    #     l = d['Crash Date/Time'].split()
    #     # Date m/d/y
    #     l_0 = l[0].split('/')

    #     d['Cm'] = int(l_0[0])
    #     d['Cd'] = int(l_0[1])
    #     d['Cy'] = int(l_0[2])

    #     # Time
    #     l_1 = l[1].split(':')

    #     d['H'] = int(l_1[0])
    #     d['M'] = int(l_1[1])
    #     d['S'] = int(l_1[2])

    #     d['Speed Limit'] = int(d['Speed Limit'])

    # insert_data_mongo(collection, data1)
    # insert_data_mongo(collection, data2)

    # ===== 1 ======
    sort_limit_data_by_Vehicle_Year(collection, 2015)
    get_Driver_At_Fault(collection, 'No')

    # Crashes from Jan to May at 2008
    D = {'m1' : 1, 'm2' : 5, 'y' : 2015}
    get_crash_by_date(collection, D)
    ford_winter_crash_by_filter(collection)
    toyota_count_by_filter(collection)

    # ===== 2 ======

    dump_min_max_avg_by_Crash_Year(collection, "p2_ex_1")
    dump_count_by_speed_limit(collection, "p2_ex_2")
    
    # Какие модели попадают в ДТП в среднем в каких месяцах
    dump_month_by(collection, 'Vehicle Make', "p2_ex_3")
    # Какие модели попадают в ДТП в среднем по году авпуска
    dump_Vehicle_Make_by(collection, 'Vehicle Year', "p2_ex_4")

    # В каком месяце случаются аварии при минимальном скоростном лимите
    dump_min_speed_limit_and_max_month(collection, "p2_ex_5")

    # ===== 3 ======

    drop_by_make(collection)
    decriace_year_by_one(collection)
    increace_speed_limit_by_make(collection, ['HONDA', 'FORD'])
    increace_time_for_nin_surface_condition(collection, ['DRY'])
    increace_Vehicle_Year_by(collection, ["South", "East"], ["GMC" "SUBA" "FORD"], [2016, 2017, 2018])
