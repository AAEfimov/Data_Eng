import json
import msgpack
import os

datafile = ""

with open(datafile) as f:
    data = json.load(f)

    prod = dict()

    for item in data:
        try:
            prod[item['name']].append(item['price'])
        except:
            prod[item['name']] = list()
            prod[item['name']].append(item['price'])

    res = []

    for name, prices in prod.items():
        sump, maxp, minp, size = 0, prices[0], prices[0], len(prices)
        for p in prices:
            sump += p
            maxp = max(maxp, p)
            minp = min(minp, p)

        res.append({
            "name" : name,
            "max" : maxp,
            "min" : minp,
            "avr" : sump / size
        })

out_json_datafile = datafile + "out_json"

with open(out_json_datafile, mode="w") as f_json:
    f_json.write(json.dump(res))

out_msgpack_datafile = datafile + "out_msgpack"

with open(out_msgpack_datafile, mode="wb") as f_msg:
    f_msg.write(msgpack.dump(res))


print("JSON FILE SIZE: ", os.path.getsize(out_json_datafile))
print("MSGPACK FILE SIZE: ", os.path.getsize(out_msgpack_datafile))
    



