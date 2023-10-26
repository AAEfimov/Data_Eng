import json

datafile = ""

with open(datafile, mode="r") as f:
    data = json.load(f)

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


for prod, price_info in data.items():
    price_update(prod, price_info)

