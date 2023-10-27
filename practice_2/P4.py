import json
import pickle

datafile_pi = "tests/z4_price_info_40.json"
datafile_prod = "tests/z4_products_40.pkl"

with open(datafile_pi, mode="r") as f:
    data = json.load(f)

with open(datafile_prod, mode="rb") as f:
    prods = pickle.load(f)

def price_update(product, price_info):
    method = price_info["method"]
    if method == "add":
        product["price"] +=  price_info["param"]
    elif method == "sub":
        product["price"] -= price_info["param"]
    elif method == "percent+":
        product["price"] *= (1 + price_info["param"])
    elif method == "percent-":
        product["price"] *= (1 - price_info["param"])

    #return product
    return 1


pid = {}

for v in data:
    pid[v['name']] = v

# V1
# [price_update(v, pid[v['name']]) for v in prods]

# V2
# t = map(lambda v: price_update(v, pid[v['name']]), prods)
# print(set(t))

# V3
for v in prods:
    price_update(v, pid[v['name']])

with open(datafile_prod + "_out", "wb") as f:
    pickle.dump(prods, f)


# DBG
#with open(datafile_prod + "_out", mode="rb") as f:
#    prods = pickle.load(f)
#print(prods)
