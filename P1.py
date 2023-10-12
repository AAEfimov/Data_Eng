

filename = "text_1_var_40"

with open(filename) as f:
    lines = f.readlines()

d = dict()

for l in lines:
    r = (l.strip()
         .replace("!", " ")
         .replace("?", " ")
         .replace(",", " ")
         .replace(".", " ")
         .strip("!"))

    words = r.split()

    for w in words:
        d.setdefault(w,0)
        d[w] += 1

d = dict(sorted(d.items(), key=lambda item: item[1]))

with open(filename+"out", "w") as f:
    for w in d:
        f.write(f"{w}:{d[w]}\n")


