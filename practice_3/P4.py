
import os
import json
import re
import numpy as np
from aux_func import *
from bs4 import BeautifulSoup

# pip install bs4 lxml

z_numb = 4
zip_file_name = 'zip_var_40.zip'
file_path = 'tests/' + str(z_numb)

filename = file_path + '/' + zip_file_name

json_out_filename = f"{file_path}/"

bld_name_freq = {}
new_info = []
pl = []

for next_file in get_next_file_from_zip(filename, file_path):

    with open(next_file, mode='r') as xml_fp:
        soup = BeautifulSoup(xml_fp, features="xml")
        
        clothing_items = soup.find("clothing-items")

        for ci in clothing_items.find_all("clothing"):
            tag_list = list(ci.children)
            item = {}

            for ct in tag_list:
                if ct.name:
                    tag_name = ct.name.strip()
                    tag_value = ct.text.strip()

                    if tag_name == 'price':
                        tag_value = int(tag_value)
                        pl.append(tag_value)
                    elif tag_name == 'reviews':
                        tag_value = int(tag_value)
                    elif tag_name == 'rating':
                        tag_value = float(tag_value)
                    elif tag_name == 'material':
                        add_to_dict(tag_value, bld_name_freq)

                    item[tag_name] = tag_value

            new_info.append(item)
      
print("DONE")

outfilename = "outfile.json"
json_out_filename_final = json_out_filename + "final_" + outfilename
json_out_filename_sorted = json_out_filename + "sorted_" + outfilename
json_out_filename_filter = json_out_filename + "filter_" + outfilename
json_out_filename_freq = json_out_filename + "freq_" + outfilename

#results

print("Results for price")
print(f"min {np.min(pl)}\nmax {np.max(pl)}\nmean {np.mean(pl)}\nstd {np.std(pl)} ")

# final

with open(json_out_filename_final, mode="w") as f_json:
    json.dump(new_info, f_json)

# sort

reviews_sorted_key = lambda x: x['reviews']
rating_sorted_key = lambda x: x['rating']
price_sorted_key = lambda x: x['price']

up_to_down = True

sorted_by_param = sorted(new_info, key=reviews_sorted_key, reverse = up_to_down)

with open(json_out_filename_sorted, mode="w") as f_json:
    json.dump(sorted_by_param, f_json)

# filter

def filter_wrapper(filter_value):
    return lambda x : x['price'] < filter_value

# filter + sort
filter_by_year = filter_wrapper(120000)
filtered_list = sorted(filter(filter_by_year, new_info), key=price_sorted_key, reverse=up_to_down)

with open(json_out_filename_filter, mode="w") as f_json:
    lst = [*filtered_list]
    json.dump(lst, f_json)

# FREQ
with open(json_out_filename_freq, mode="w") as f_json:
    json.dump(bld_name_freq, f_json)

