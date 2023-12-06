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

pp = pprint.PrettyPrinter()

def parse_data(fn):
    with open(fn, mode='rb') as pkl_f:
        data = pickle.load(pkl_f)

    for d in data:
        d['salary'] = int(d['salary'])
        d['year'] = int(d['year'])
        d['id'] = int(d['id'])
        d['age'] = int(d['age'])

    return data

def drop_by_salary(collection):
    q = {
            "$or" : [
                {"salary" : {"$lt" : 25_000}},
                {"salary" : {"$gt" : 175_000}},
            ]
        }

    result = collection.delete_many(q)

    print(result)

def increace_age_by_one(collection):
    filt = {}
    upd = { 
        "$inc" :   # $set, $inc
            {"age" : 1}
    }
    result = collection.update_many(filt, upd)
    print(result)

def increace_salary_by_job(collection, p_list):
    filt = {
        "job" : {"$in" : p_list}
    }
    upd = { 
        "$mul" : {
            "salary" : 1.05
            }
        }
    result = collection.update_many(filt, upd)
    print(result)    

def increace_salary_by_city(collection, p_list):
    # Operation not in $nin
    filt = {
        "city" : {"$nin" : p_list}
    }
    upd = { 
        "$mul" : {
            "salary" : 1.07
            }
        }
    result = collection.update_many(filt, upd)
    print(result)    

def increace_salary_by(collection, city_l, job_l, age_l):
    filt = {
        "city" : {"$in" : city_l},
        "job"  : {"$nin" : job_l},
        "age"  : {"$gte" : age_l[0], "$lte" : age_l[1]}
    }
    upd = { 
        "$mul" : {
            "salary" : 1.1
            }
        }
    
    result = collection.update_many(filt, upd)
    print(result)    

def drop_by_var(collection):
    q = {
            "$and" : [
                {"year" : {"$lt" : 2010}},
                {"salary" : {"$lte" : 100_000}},
            ]
        }

    result = collection.delete_many(q)

    print(result)

if __name__ == "__main__":

    db = connect_mongo(database)
    collection = db.person

    # call once!
    # data = parse_data(datafile)
    # insert_data_mongo(collection, data)

    drop_by_salary(collection)
    increace_age_by_one(collection)
    increace_salary_by_job(collection, ['Учитель', 'Врач', 'Медсестра'])
    increace_salary_by_city(collection, ['Рига', 'Кишинев', 'Бургос'])
    increace_salary_by(collection, ['Москва', 'Санкт-Петербург'], ['Инженер', 'IT-специалист'], (40, 50))
    drop_by_var(collection)