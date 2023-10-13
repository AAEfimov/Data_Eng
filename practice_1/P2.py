

filename = "tests/text_2_var_40"
det = ","

with open(filename) as f:
    lines = f.readlines()

with open(filename + "out", "w") as f:
    for l in lines:
        f.write(f"{sum(map(int, l.strip().split(det)))}\n")
       

# OLD
#        rsum = 0
#        for n in l.strip().split(det):
#            rsum += int(n)
#        f.write(f"{str(rsum)}\n")
