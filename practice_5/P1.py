from pymongo import MongoClient


database = "test_database"

def connect(dbname):
    client = MongoClient()
    db = client[dbname]

    return db.person

def insert_data(connection, data):
    res = connection.insert_many(data)

# 1 Request
# 1 - down to up sort
# -2 - up to down sort
# for collection in collection.find({}, limit=10).sort('salary' : -1)

# 2 Request
# Filter
# Конкретное значение
# for collection in collection.find({'age' : 18}, limit = 15).sort('salary' : -1)
# Сложное выражение - {"$lt" : 30}     age < 30
# for pers in collection.find({'age' : {"$lt" : 30}}, limit = 15).sort('salary' : -1)

# 3 Request
# Город и 3 профессии {"$in", ["PROF_A", "PROF_B", "PROF_C"]}

# for pers in collection.find({'city' : "CITY_NAME", "job" : {"$in" : ["PROF_A", "PROF_B", "PROF_C"]}}, limit = 10).sort('age' : 1)

# 4 Request
# collection.count_documents({"age" : {"$gt" : 25, "$lt" : 35}, 
#                             "years" : {"$in" : [2019, 2020, 2021, 2022]}, 
#                             "$or" : [
#                                       {"salary" : {"$gt" : 50000, "$lte" : 75000}}, 
#                                       { "salary" : {"$gt" : 125000, "$lt" : 150000}}
#                                     ]
#                           })

# EX_2 


# t1
# get stat by salary analog SQL group BY
# a = [
#       {"$group" : 
#                   {
#                       "_id" : "result", 
#                       "max" : {"$max" : "$salary"},
#                       "min" : {"$min" : "$salary"}, 
#                       "avg" : {"$avg" : "$salary"}
#                   }
#        }
#     ]
# for collect in collection.aggregate(q)

# t2



if __name__ == "__main__":
    connection = connect(database)

    data = {"a" : 1, "b" : 2}
    insert_data(connection, [data])


    print('Done')
