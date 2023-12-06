import os
import json
import pprint
from aux_func import *

maindir = "tests"
variant = "var_40"
filename = "task_2_item.text"

datafile = os.path.join(maindir, variant, filename)

# Use database from T1
database = "var40_base1"

json_out = "tests/out2/{}.json"


pp = pprint.PrettyPrinter()

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

def dump_min_max_avg(collection, outfile):
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
    write_data_to_json([*data], json_out.format(outfile))

def dump_count_by_job(collection, outfile):
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
    write_data_to_json([*data], json_out.format(outfile))
   
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

    # Запрос с ошибкой. он выводит минимальный возраст и максимальную зарплату
    # Выводимые данные принадлежат разным записям!

    data = collection.aggregate(a)
    #pp.pprint([*data])
    #write_data_to_json([*data], json_out.format(outfile))


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

def dump_min_salary_by_max_age(collection, outfile):
    # pipeline
    a = [
        {
            "$group" : {
                "_id" : "$age",
                "min_salary" : {"$min" : "$salary"}
            }
        },
        {
            "$group" : {
                "_id" : "result",
                "max_age" : {"$max" : "$_id"},
                "min_salary" : {"$min" : "$min_salary"}
            }
        }
    ]

    # Запрос с ошибкой. он выводит минимальный возраст и максимальную зарплату
    # Выводимые данные принадлежат разным записям!

    data = collection.aggregate(a)
    #pp.pprint([*data])
    #write_data_to_json([*data], json_out.format(outfile))

def dump_min_salary_by_age(collection, match_age, outfile):
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
                "min_age" : {"$max" : "$age"},
                "max_salary" : {"$min" : "$salary"}
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))

def dump_params_by_city_by_salary(collection, outfile):
    a = [
        {
            "$match" : 
                { "salary" : {"$gt" : 50000 }}
        },
        {
            "$group" : {
                "_id" : "$city",
                "max" : {"$max" : "$age"},
                "min" : {"$min" : "$age"},
                "avg" : {"$avg" : "$age"},  
            }
        },
        {
            "$sort" : {
                "avg" : -1
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

    dump_min_max_avg(collection, "ex_1")
    dump_count_by_job(collection, "ex_2")

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
    #dump_min_salary_by_max_age(collection, "ex_8")
    dump_min_salary_by_age(collection, 65, "ex_8")

    dump_params_by_city_by_salary(collection, "ex_9")