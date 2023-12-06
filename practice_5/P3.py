import os
import json
import pprint
import pickle
from aux_func import *

maindir = "tests"
variant = "var_40"
filename = "task_3_item.pkl"

datafile = os.path.join(maindir, variant, filename)

# Use database from T1
database = "var40_base1"

json_out = "tests/out3/{}.json"

def parse_data(fn):
    with open(fn, mode='rb') as pkl_f:
        data = pickle.load(pkl_f)

    for d in data:
        d['salary'] = int(d['salary'])
        d['year'] = int(d['year'])
        d['id'] = int(d['id'])
        d['age'] = int(d['age'])

    return data


pp = pprint.PrettyPrinter()

if __name__ == "__main__":

    db = connect_mongo(database)
    collection = db.person

    # call once!
    # data = parse_data(datafile)
    # insert_data_mongo(collection, data)

    