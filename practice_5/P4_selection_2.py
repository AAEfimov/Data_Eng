from aux_func import *

json_out = "tests/out4/{}.json"

def dump_min_max_avg_by_Crash_Year(collection, outfile):
    a = [
        {
            "$group" : {
                "_id" : "result",
                "max" : {"$max" : "$Cy"},
                "min" : {"$min" : "$Cy"},
                "avg" : {"$avg" : "$Cy"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))


# Количество при разных скоростных ограничениях
def dump_count_by_speed_limit(collection, outfile):
    # pipeline
    a = [
        {
            "$group" : {
                "_id" : "$Speed Limit",
                "count" : {"$sum" : 1},
            }
        },
        {
            "$sort" : {
                "_id" : -1
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))


def dump_month_by(collection, column, outfile):
    a = [
        {
            "$group" : {
                "_id" : f"${column}",
                "max" : {"$max" : "$Cm"},
                "min" : {"$min" : "$Cm"},
                "avg" : {"$avg" : "$Cm"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))

def dump_Vehicle_Make_by(collection, column, outfile):
    a = [
        {
            "$group" : {
                "_id" : "$Vehicle Make",
                "max" : {"$max" : f"${column}"},
                "min" : {"$min" : f"${column}"},
                "avg" : {"$avg" : f"${column}"},
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))


def dump_min_speed_limit_and_max_month(collection, outfile):
    # pipeline
    a = [
        {
            "$group" : {
                "_id" : "$Speed Limit",
                "max_monts" : {"$max" : "$Cm"}
            }
        },
        {
            "$group" : {
                "_id" : "result",
                "min_speed_limit" : {"$min" : "$_id"},
                "max_month" : {"$max" : "$max_monts"}
            }
        }
    ]

    data = collection.aggregate(a)
    write_data_to_json([*data], json_out.format(outfile))


