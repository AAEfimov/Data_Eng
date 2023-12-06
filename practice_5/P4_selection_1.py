
from aux_func import *

json_out = "tests/out4/{}.json"

def sort_limit_data_by_Vehicle_Year(collection, year, vlimit = 20, up_to_down = -1):

    q = {
        'Vehicle Year' : {"$lte" : year}
    }
    data = collection.find(q, limit=vlimit).sort({'Vehicle Year' : up_to_down})
    write_data_to_json([*data], json_out.format("ex_1"))

def get_Driver_At_Fault(collection, fault, vlimit = 20, up_to_down = -1):
    q = {
        'Driver At Fault' : {"$eq" : fault}
    }
    data = collection.find(q, limit=vlimit).sort({'Vehicle Year' : up_to_down})
    write_data_to_json([*data], json_out.format("ex_2"))


def get_crash_by_date(collection, D, vlimit = 20, up_to_down = -1):
    q = {
        'Cy' : {"$eq" : D['y']},
        'Cm' : {"$gte" : D['m1'] , "$lte" : D['m2']}
    }

    data = collection.find(q, limit = vlimit).sort({'Cy' : up_to_down})
    write_data_to_json([*data], json_out.format("ex_3"))

# Количество аварий в зимние месяцы на автомобилях FORD 
# Выпущеных с 2010 по 2015
def ford_winter_crash_by_filter(collection):
    q = {
            "Vehicle Make" : 'FORD', 
            "Vehicle Year" : {"$gte" : 2010, "$lte" : 2015},
            "Cm" : {"$in" : [11,12,1,2,3]}
        }

    data = collection.find(q).sort({'year' : 1})
    
    write_data_to_json([*data], json_out.format("ex_4"))

def toyota_count_by_filter(collection):
    q = {
            "Vehicle Make" : 'TOYT', 
            "Vehicle Year" : {"$gte" : 2010, "$lte" : 2015},
        }

    data = collection.count_documents(q)
    
    write_data_to_json(data, json_out.format("ex_5"))
