import numpy as np
import json

datafile = "tests/z1_matrix_40.npy"

matrix = np.load(datafile)

msd = dict()

msd['sum'] = 0
msd['avr'] = 0
msd['sumMD'] = 0
msd['avrMD'] = 0
msd['sumSD'] = 0
msd['avrSD'] = 0
msd['max'] = 0
msd['min'] = 0

size = len(matrix)

for i in range(size):
    for j in range(size):
        msd['sum'] += matrix[i][j]
        msd['max'] = max(msd['max'], matrix[i][j])
        msd['min'] = min(msd['min'], matrix[i][j])
        if i == j:
            msd['sumMD'] += matrix[i][j]
        if (i + j) == size:
            msd['sumSD'] += matrix[i][j]


msd['avr'] = msd['sum'] / (size * size)
msd['avrMD'] = msd['sumMD'] / size
msd['avrSD'] = msd['sumSD'] / size

norm_matrix = np.ndarray((size,size), dtype=float)

np.save(datafile+"_norm", norm_matrix)
