import numpy as np
import json

datafile = "tests/z1_matrix_40.npy"

matrix = np.load(datafile)

size = len(matrix)
msd2 = dict()

# wtihout int cast got exteption
# TypeError: Object of type int64 is not JSON serializable

msd2['sum'] = int(matrix.sum())
msd2['avr'] = msd2['sum'] / (size * size)
msd2['sumMD'] = int(np.trace(matrix, offset = 0))
msd2['avrMD'] = msd2['sumMD'] / size
msd2['sumSD'] = int(np.trace(matrix[::-1], offset = 0))
msd2['avrSD'] = msd2['sumSD'] / size
msd2['max'] = int(matrix.max())
msd2['min'] = int(matrix.min())
norm_matrix2 = matrix / msd2['sum']

np.save(datafile+"_norm", norm_matrix2)

with open(datafile + "_json_out", mode="w") as f:
    f.write(json.dumps(msd2))


# OLD VERSION
"""
msd = dict()

msd['sum'] = 0
msd['avr'] = 0
msd['sumMD'] = 0
msd['avrMD'] = 0
msd['sumSD'] = 0
msd['avrSD'] = 0
msd['max'] = 0
msd['min'] = matrix[0][0]

for i in range(size):
    for j in range(size):
        msd['sum'] += matrix[i][j]
        msd['max'] = max(msd['max'], matrix[i][j])
        msd['min'] = min(msd['min'], matrix[i][j])
        if i == j:
            msd['sumMD'] += matrix[i][j]
        if (i + j + 1) == size:
            msd['sumSD'] += matrix[i][j]


#msd['sumMD'] = 0
#msd['sumSD'] = 0
#for i in range(size):
#    msd['sumMD'] += matrix[i][i]
#    msd['sumSD'] += matrix[i][size-i-1]

msd['avr'] = msd['sum'] / (size * size)
msd['avrMD'] = msd['sumMD'] / size
msd['avrSD'] = msd['sumSD'] / size

norm_matrix = np.ndarray((size,size), dtype=float)
for i in range(size):
    for j in range(size):
        norm_matrix[i][j] = matrix[i][j] / msd['sum']

np.save(datafile+"_norm", norm_matrix)
with open(datafile + "_json_out", mode="w") as f:
    json.dump(msd, f)

"""

