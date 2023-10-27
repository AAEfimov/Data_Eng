import numpy as np
import os

datafile = "tests/z2_matrix_40_2.npy"

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

outfile_point_z = datafile+"_points_z"
outfile_compressed = datafile + "_compressed"

np.savez(outfile_point_z, x=x, y=y, z=z)
np.savez_compressed(outfile_compressed, x=x, y=y, z=z)

print("Z file size: ", os.path.getsize(outfile_point_z+ ".npz"))
print("Compressed file size: ", os.path.getsize(outfile_compressed + ".npz"))

