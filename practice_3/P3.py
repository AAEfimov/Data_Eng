
import os
import json
import re
from aux_func import *
import numpy as np
from bs4 import BeautifulSoup

# pip install bs4 lxml

z_numb = 3
zip_file_name = 'zip_var_40.zip'
file_path = 'tests/' + str(z_numb)

filename = file_path + '/' + zip_file_name

json_out_filename = f"{file_path}/"

new_info = []
bld_name_freq = {}
rl = []

for next_file in get_next_file_from_zip(filename, file_path):
        
    item = {}

    with open(next_file, mode='r') as html_fp:
        soup = BeautifulSoup(html_fp, features="xml")
        
        star = soup.find("star")

        s_name = star.find("name").text
        item['star_name'] = s_name.strip()

        item['constellation'] = star.find("constellation").text.strip()

        add_to_dict(item['constellation'], bld_name_freq)

        spec_class = star.find("spectral-class").text
        item['spectral_class'] = spec_class.strip()

        radius = star.find('radius').text
        item['radius'] = int(radius.strip())

        rl.append(item['radius'])

        rotation = star.find('rotation').text.split()[0]
        item['rotation'] = float(rotation)

        age = star.find("age").text.split()
        item['age'] = {}
        item['age']['year'] = float(age[0])
        item['age']['mult'] = get_mult(age[1])

        distance = star.find("distance").text.split()
        item['distance'] = {}
        item['distance']['dist'] = float(distance[0])
        item['distance']['mult'] = get_mult(distance[1])

        magn = star.find('absolute-magnitude').text.split()
        item['magnitude'] = {}
        item['magnitude']['km'] = float(magn[0])
        item['magnitude']['mult'] = get_mult(magn[1])

    new_info.append(item)

print("DONE")

outfilename = "outfile.json"
json_out_filename_final = json_out_filename + "final_" + outfilename
json_out_filename_sorted = json_out_filename + "sorted_" + outfilename
json_out_filename_filter = json_out_filename + "filter_" + outfilename
json_out_filename_freq = json_out_filename + "freq_" + outfilename

#results

print("Results for star radius")
print(f"min {np.min(rl)}\nmax {np.max(rl)}\nmean {np.mean(rl)}\nstd {np.std(rl)} ")

# final

with open(json_out_filename_final, mode="w") as f_json:
    json.dump(new_info, f_json)

# sort

radius_sorted_key = lambda x: x['radius']
distance_sorted_key = lambda x: x['distance']['dist'] * x['distance']['mult']
age_sorted_key = lambda x: x['age']['year'] * x['age']['mult']

up_to_down = True

sorted_by_param = sorted(new_info, key=age_sorted_key, reverse = up_to_down)

with open(json_out_filename_sorted, mode="w") as f_json:
    json.dump(sorted_by_param, f_json)

# filter

def filter_wrapper(filter_value):
    return lambda x : x['age']['year'] * x['age']['mult'] > filter_value

filter_by_year = filter_wrapper(5 * (10 ** 9))
filtered_list = filter(filter_by_year, new_info)

with open(json_out_filename_filter, mode="w") as f_json:
    lst = [*filtered_list]
    json.dump(lst, f_json)

# FREQ
with open(json_out_filename_freq, mode="w") as f_json:
    json.dump(bld_name_freq, f_json)

