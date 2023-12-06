import os
import json
from aux_func import *

maindir = "tests"
variant = "var_40"
filename = "task_1_item.json"

datafile = os.path.join(maindir, variant, filename)

database = "var40_base1"

json_out = "tests/out1/{}.json"

def sort_limit_data_by_salary_dump(collection, vlimit = 10, up_to_down = -1):
    data = collection.find({}, limit=vlimit).sort({'salary' : up_to_down})
    write_data_to_json([*data], json_out.format("ex_1"))


def sort_and_filter(collection, vlimit = 15, up_to_down = -1):
    data = collection.find({'age' : {"$lt" : 30}}, limit = vlimit).sort({'salary' : up_to_down})
    write_data_to_json([*data], json_out.format("ex_2"))


def sort_and_filter_hard(collection, city, job_list, vlimit = 10, up_to_down = 1):
    data = collection.find({'city' : city, "job" : {"$in" : job_list}}, limit = 10).sort({'age' : up_to_down})
    write_data_to_json([*data], json_out.format("ex_3"))

def count_by_filter(collection):
    data = collection.find({"age" : {"$gt" : 25, "$lt" : 35}, 
                                        "year" : {"$in" : [2019, 2020, 2021, 2022, 2001]},
                                        "$or" : [
                                                    {"salary" : {"$gt" : 50000, "$lte" : 75000}}, 
                                                    { "salary" : {"$gt" : 125000, "$lt" : 150000}}
                                                ]
                                    }).sort({'age' : 1})
    
    write_data_to_json([*data], json_out.format("ex_4"))

    cnt = collection.count_documents({"age" : {"$gt" : 25, "$lt" : 35}, 
                                        "year" : {"$in" : [2019, 2020, 2021, 2022]}, 
                                        "$or" : [
                                                    {"salary" : {"$gt" : 50000, "$lte" : 75000}}, 
                                                    { "salary" : {"$gt" : 125000, "$lt" : 150000}}
                                                ]
                                    })

    write_data_to_json(cnt, json_out.format("ex_4_cnt"))

def parse_data(filename):
    with open(filename, mode='r') as f:
        data = json.load(f)

    for d in data:
        d['salary'] = int(d['salary'])
        d['year'] = int(d['year'])
        d['id'] = int(d['id'])
        d['age'] = int(d['age'])

    return data

if __name__ == "__main__":

    db = connect_mongo(database)
    connection = db.person

    col_names = db.list_collection_names()

    if col_names:
        for n in col_names:
            db.drop_collection(n)

    data = parse_data(datafile)
    insert_data_mongo(connection, data)

    sort_limit_data_by_salary_dump(connection)
    sort_and_filter(connection)
    sort_and_filter_hard(connection, "Валенсия", ["Программист", "Повар", "Врач"])
    count_by_filter(connection)

