import numpy as np

datafile = ""

matrix = np.load(datafile)

variant = 40
limit_p = 500 + variant

x,y,z = [],[],[]

size = len(matrix)

for i in range(size):
    for j in range(size):
        if matrix[i][j] > limit_p:
            x.append(i)
            y.append(j)
            z.append(matrix[i][j])

np.savez(datafile+"_points_z", x=x, y=y, z=z)
np.savez_compressed(datafile + "_compressed", x=x, y=y, z=z)