import os
import json
from aux_func import *

maindir = "tests"
variant = "var_40"
filename = "task_2_item.text"

datafile = os.path.join(maindir, variant, filename)

# Use database from T1
database = "var40_base1"

json_out = "tests/out2/{}.json"

def parse_data(filename):
    with open(filename, mode='r') as f:
        lines = f.readlines()

    data = []
    d = {}
    for l in lines:
        
        if '=====' in l:
            d['salary'] = int(d['salary'])
            d['year'] = int(d['year'])
            d['id'] = int(d['id'])
            d['age'] = int(d['age'])

            data.append(d)
            d = {}
        else:
            s = l.strip().split('::')
            d[s[0]] = s[1]

    return data

def dump_min_max_avg(collection):
    a = [
        {
            "$group" : {
                "_id" : "result",
                "max" : {"$max" : "$salary"},
                "min" : {"$min" : "$salary"},
                "avg" : {"$avg" : "$salary"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format("ex_1"))

def dump_count_by_job(collection):
    # pipeline
    a = [
        {
            "$group" : {
                "_id" : "$job",
                "count" : {"$sum" : 1},
            }
        },
        {
            "$sort" : {
                "count" : -1
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format("ex_2"))
   
def dump_salary_by(collection, column, outfile):
    a = [
        {
            "$group" : {
                "_id" : f"${column}",
                "max" : {"$max" : "$salary"},
                "min" : {"$min" : "$salary"},
                "avg" : {"$avg" : "$salary"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))  

def dump_age_by(collection, column, outfile):
    a = [
        {
            "$group" : {
                "_id" : f"${column}",
                "max" : {"$max" : "$age"},
                "min" : {"$min" : "$age"},
                "avg" : {"$avg" : "$age"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))  

def dump_by(collection, column, column_calc, outfile):
    a = [
        {
            "$group" : {
                "_id" : f"${column}",
                "max" : {"$max" : f"${column_calc}"},
                "min" : {"$min" : f"${column_calc}"},
                "avg" : {"$avg" : f"${column_calc}"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))  

def dump_max_salary_by_min_age(collection, outfile):
    # pipeline
    a = [
        {
            "$group" : {
                "_id" : "$age",
                "max_salary" : {"$max" : "$salary"}
            }
        },
        {
            "$group" : {
                "_id" : "result",
                "min_age" : {"$min" : "$_id"},
                "max_salary" : {"$max" : "$max_salary"}
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))


def dump_max_salary_by_age(collection, match_age, outfile):
    # pipeline
    a = [
        {
            "$match" : {
                "age" : match_age,
            }
        },
        {
            "$group" : {
                "_id" : "result",
                "min_age" : {"$min" : "$age"},
                "max_salary" : {"$max" : "$salary"}
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))

if __name__ == "__main__":

    db = connect_mongo(database)
    collection = db.person

    # call once!
    # data = parse_data(datafile)
    # insert_data_mongo(connection, data)

    dump_min_max_avg(collection)
    dump_count_by_job(collection)

    #dump_salary_by(collection, "city", "ex_3")
    #dump_salary_by(collection, "job", "ex_4")
    #dump_age_by(collection, "city", "ex_5")
    #dump_age_by(collection, "job", "ex_6")

    dump_by(collection, "city", "salary", "ex_3")
    dump_by(collection, "job", "salary", "ex_4")
    dump_by(collection, "city", "age", "ex_5")
    dump_by(collection, "job", "age", "ex_6")

    #dump_max_salary_by_min_age(collection, "ex_7")
    dump_max_salary_by_age(collection, 18, "ex_7")