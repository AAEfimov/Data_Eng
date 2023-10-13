
filename = "tests/text_1_var_40"

with open(filename) as f:
    lines = f.readlines()

d = dict()

for l in lines:
    for w in (l.strip()
         .replace("!", " ")
         .replace("?", " ")
         .replace(",", " ")
         .replace(".", " ")
         .strip()).split():
        try:
            d[w] += 1
        except:
            d.update({w : 1})

d = dict(sorted(d.items(),reverse = True, key=lambda item: item[1]))

with open(filename+"out", "w") as f:
    for w in d:
        f.write(f"{w}:{d[w]}\n")

# OLD
#    r = (l.strip()
#         .replace("!", " ")
#         .replace("?", " ")
#         .replace(",", " ")
#         .replace(".", " ")
#         .strip())
#    words = r.split()
#    for w in words:
#        d.setdefault(w,0)
#        d[w] += 1

