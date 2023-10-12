

filename = "text_3_var_40"
det = ","

less_v = 50 + 40

with open(filename) as f:
    lines = f.readlines()

with open(filename+"out", "w") as f:
    for l in lines:
        tl = [w for w in l.strip().split(det)]
        lq = [int(w) if w.isdigit() else int(tl[i-1]) + int(tl[i+1]) for i,w in enumerate(tl)]
        [f.write(f"{i},") if i ** 0.5 >= less_v else "" for i in lq]
        f.write("\n")
