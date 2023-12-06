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
    with open(datafile, mode='r') as csv_f:
        csvr = csv.DictReader(csv_f, delimiter=',')
        data = []
        for row in csvr:
            data.append(row)

    return data


def parse_data_json(datafile):
    with open(datafile, mode='r') as f:
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
    #     d['Vehicle Year'] = int(d['Vehicle Year'])
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


    sort_limit_data_by_Vehicle_Year(collection, 2015)
    get_Driver_At_Fault(collection, 'No')

    # Crashes from Jan to May at 2008
    D = {'m1' : 1, 'm2' : 5, 'y' : 2015}
    get_crash_by_date(collection, D)
    ford_winter_crash_by_filter(collection)
    toyota_count_by_filter(collection)

