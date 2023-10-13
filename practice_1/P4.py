

import csv

filename = "tests/text_4_var_40"

age_filter = 25 + (40 % 10)

d = {}
csv_del = ","

with open(filename, mode = "r", encoding="utf-8") as f:
    l = csv.reader(f, delimiter=csv_del)
    m = 0
    for r in l:
        d.update({r[0] : r[1:-1]})
        m += int(r[4][:-1])

m /= len(d)

# all filters here
ml = dict(sorted([[int(k) , v] if (int(v[-1][:-1]) >= m) and (int(v[2]) > age_filter) else [-1 , [-1]] for k, v in d.items()]))

try:
    ml.pop(-1)
except:
    pass

# try to use filter(lambda v: ...., d.items())

with open(filename + "out", mode = "w", encoding="utf-8") as f:
    d_w = csv.writer(f, delimiter=csv_del)
    for item in ml.items():
        d_w.writerow([item[0]] + item[1]) 
