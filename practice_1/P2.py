

filename = "text_2_var_40"

det = ","

with open(filename) as f:
    lines = f.readlines()

with open(filename + "out", "w") as f:
    for l in lines:
        rsum = 0
        for n in l.strip().split(det):
            rsum += int(n)
            # SOME OPERATIONS
        f.write(f"{str(rsum)}\n")
        