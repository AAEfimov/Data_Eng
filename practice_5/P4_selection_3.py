def drop_by_make(collection):
    q = {
            "$or" : [
                {"Vehicle Make" : {"$eq" : 'HUNDAI'}},
                {"Vehicle Make" : {"$eq" : 'KIA'}},
            ]
        }

    result = collection.delete_many(q)

    print(result)

def decriace_year_by_one(collection):
    filt = {}
    upd = { 
        "$inc" :   # $set, $inc
            {"Cy" : -1}
    }
    result = collection.update_many(filt, upd)
    print(result)

def increace_speed_limit_by_make(collection, p_list):
    filt = {
        "Vehicle Make" : {"$in" : p_list}
    }
    upd = { 
        "$mul" : {
            "Speed Limit" : 1.2
            }
        }
    result = collection.update_many(filt, upd)
    print(result) 


def increace_time_for_nin_surface_condition(collection, p_list):
    # Operation not in $nin
    filt = {
        "Surface Condition" : {"$nin" : p_list}
    }
    upd = { 
        "$inc" : {
            "H" : 1
            }
        }
    result = collection.update_many(filt, upd)
    print(result)


def increace_Vehicle_Year_by(collection, gd_l, vm_l, year_l):
    filt = {
        "Vehicle Going Dir" : {"$in" : gd_l},
        "Vehicle Make"  : {"$nin" : vm_l},
        "Cy" : {"$in" : year_l}
    }
    upd = { 
        "$inc" : {
            "Cy" : -2
            }
        }
    
    result = collection.update_many(filt, upd)
    print(result)   
