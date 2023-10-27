import json
import msgpack
import pickle
import csv
import os

import matplotlib.pyplot as plt

datafile = "tests/Z5_Data.csv"

datafile_out_json = datafile + "_out_json"
datafile_out_pickle = datafile + "_out_pickle"
datafile_out_msgpack = datafile + "_out_msgpack"

datafile_csv = datafile

csv_del = ","

stat = {}

stat['minM'] = 12
stat['maxM'] = 0
stat['avgM'] = 0
stat['sumM'] = 0
stat['MDeviation'] = 0

stat['minDOW'] = 12
stat['maxDOW'] = 0
stat['avgDOW'] = 0
stat['sumDOW'] = 0
stat['DOWDeviation'] = 0

stat['minH'] = 12
stat['maxH'] = 0
stat['avgH'] = 0
stat['sumH'] = 0
stat['HDeviation'] = 0


month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
month_s = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

stat['SEX'] = {}
stat['RACE'] = {}
stat['AGE_HIST'] = {}
stat['MONTH'] = {}
stat['DOW'] = {}
stat['HOUR'] = {}

agelist = []

def deviat_calc(elems, avg):
    #s_m = sum(list(map(lambda v: (v - avg) ** 2 , elems)))
    #return (1/size * s_m) ** 0.5

    return (1/size * sum([(v - avg) ** 2 for v in elems])) ** 0.5


def add_elemt(key, d, dk):
    if key in d[dk]:
        stat[dk][key] += 1
    else:
        stat[dk].setdefault(key, 1)


with open(datafile_csv) as f:
    # Full convert to JSON
    # a lot of file size
    """
    dataset = []
    csv_r = csv.DictReader(f, delimiter = ",")
    for row in csv_r:
        dataset.append(row)
    """
    dataset = {}
    size = 0
    hour_cnt = 0
    month_cnt = 0
    day_cnt = 0

    hour_list = []
    month_list = []
    day_list = []

    csv_r = csv.reader(f, delimiter = ",")
    next(csv_r)
    for row in csv_r:
        CASE_NUMBER = row[0]
        dataset[CASE_NUMBER] = {'INCIDENT_PRIMARY' : row[4],
                'GUNSHOT_INJURY_I' : row[5],
                'WARD' : row[8],
                'COMMUNITY_AREA' : row[9],
                'AREA' : row[11],
                'AGE' : row[14] if "UNKNOWN" not in row[14] else "UNKNOWN_AGE",
                'SEX' : row[15] if "UNKNOWN" not in row[15] else "UNKNOWN_SEX",
                'RACE' : row[16] if "UNKNOWN" not in row[16] else "UNKNOWN_RACE",
                'MONTH' : row[28] if "UNKNOWN" not in row[28] else "UNKNOWN_MONTH",
                'DAY_OF_WEEK' : row[29] if "UNKNOWN" not in row[29] else "UNKNOWN_DOW",
                'HOUR' : row[30] if "UNKNOWN" not in row[30] else "UNKNOWN_HOUR"
                }
        size += 1

        # Hist by age
        add_elemt(dataset[CASE_NUMBER]['AGE'], stat, 'AGE_HIST')
        add_elemt(dataset[CASE_NUMBER]['RACE'], stat, 'RACE')
        add_elemt(dataset[CASE_NUMBER]['SEX'], stat, 'SEX')
        add_elemt(dataset[CASE_NUMBER]['MONTH'], stat, 'MONTH')

        if dataset[CASE_NUMBER]['MONTH'].isdigit():
            month_cnt += 1
            nm = int(dataset[CASE_NUMBER]['MONTH'])
            stat['minM'] = min(stat['minM'], nm)
            stat['maxM'] = max(stat['maxM'], nm)
            stat['sumM'] += nm
            month_list.append(nm)

        add_elemt(dataset[CASE_NUMBER]['DAY_OF_WEEK'], stat, 'DOW')

        if dataset[CASE_NUMBER]['DAY_OF_WEEK'].isdigit():
            day_cnt += 1
            nm = int(dataset[CASE_NUMBER]['DAY_OF_WEEK'])
            stat['minDOW'] = min(stat['minDOW'], nm)
            stat['maxDOW'] = max(stat['maxDOW'], nm)
            stat['sumDOW'] += nm
            day_list.append(nm)

        add_elemt(dataset[CASE_NUMBER]['HOUR'], stat, 'HOUR')

        if dataset[CASE_NUMBER]['HOUR'].isdigit():
            hour_cnt += 1
            nm = int(dataset[CASE_NUMBER]['HOUR'])
            stat['minH'] = min(stat['minH'], nm)
            stat['maxH'] = max(stat['maxH'], nm)
            stat['sumH'] += nm
            hour_list.append(nm)



stat['avgM'] = stat['sumM'] / month_cnt
stat['MDeviation'] = deviat_calc(month_list, stat['avgM'])

stat['avgDOW'] = stat['sumDOW'] / day_cnt
stat['DOWDeviation'] = deviat_calc(day_list, stat['avgDOW'])

stat['avgH'] = stat['sumH'] / month_cnt
stat['HDeviation'] = deviat_calc(hour_list, stat['avgH'])

with open(datafile + "_comp_stat", mode="w") as f:
    json.dump(stat, f)

with open(datafile_out_json, mode="w") as f:
    f.write(json.dumps(dataset))

with open(datafile_out_msgpack, mode="wb") as f:
    msgpack.dump(dataset, f)

with open(datafile_out_pickle, mode="wb") as f:
    pickle.dump(dataset, f)

print("Filesizes:")
MB = 1024*1024
print(f"CSV {round(os.path.getsize(datafile) / MB, 2)} Mb")
print(f"JSON: {round(os.path.getsize(datafile_out_json) / MB, 2)} Mb")
print(f"MSGPACK: {round(os.path.getsize(datafile_out_msgpack) / MB, 2)} Mb")
print(f"PICKLE: {round(os.path.getsize(datafile_out_pickle) / MB, 2)} Mb")


# DBG
print(stat)

ml = [stat['MONTH'][str(v)] for v in range(1, 13)]
dl = [stat['DOW'][str(v)] for v in range(1, 8)]
hl = [stat['HOUR'][str(v)] for v in range(0, 24)]

fig, axs = plt.subplots(1, 3)

axs[0].plot(month_s, ml)
axs[0].set_title('INCIDENT PER MONTH')

axs[1].plot(days, dl)
axs[1].set_title('INCIDENT PER DOW')

x = [h for h in range(0, 24)]


axs[2].plot(x, hl)
axs[2].set_title('INCIDENT PER HOUR')

plt.show()
