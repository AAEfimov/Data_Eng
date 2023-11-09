
import os
import json
import re
from bs4 import BeautifulSoup
from zipfile import ZipFile

z_numb = 2
zip_file_name = 'zip_var_40.zip'
file_path = 'tests/' + str(z_numb)

filename = file_path + '/' + zip_file_name

json_out_filename = f"{file_path}/"

with ZipFile(filename, 'r') as zf:
    ext_dir = file_path + "/extract"
    if not os.path.exists(ext_dir):
        os.mkdir(ext_dir)

    new_info = []
    bld_name_freq = {}

    for f in zf.infolist():
        zf.extract(f.filename, ext_dir)
        ext_filename = os.path.join(ext_dir, f.filename)

        item = {}

        with open(ext_filename, mode='r') as html_fp:
            soup = BeautifulSoup(html_fp, 'html.parser')

            item['filename'] = f.filename

            s_head = soup.head
            s_body = soup.body

        break

        new_info.append(item)

outfilename = "outfile.json"
json_out_filename_final = json_out_filename + "final_" + outfilename
json_out_filename_sorted = json_out_filename + "sorted_" + outfilename
json_out_filename_filter = json_out_filename + "filter_" + outfilename
json_out_filename_freq = json_out_filename + "freq_" + outfilename

#results


# final

with open(json_out_filename_final, mode="w") as f_json:
    json.dump(new_info, f_json)

# sort

views_sorted_key = lambda x: x['add_info']['views']
reit_sorted_key = lambda x: x['add_info']['reit']

up_to_down = True

sorted_by_views = sorted(new_info, key=views_sorted_key, reverse = up_to_down)

with open(json_out_filename_sorted, mode="w") as f_json:
    json.dump(sorted_by_views, f_json)

# filter

def filter_year_wrapper(filter_value):
    return lambda x : x['building_info']['year'] > filter_value

filter_by_year = filter_year_wrapper(2000)

filtered_list = filter(filter_by_year, new_info)

with open(json_out_filename_filter, mode="w") as f_json:
    lst = [*filtered_list]
    json.dump(lst, f_json)

# FREQ
with open(json_out_filename_freq, mode="w") as f_json:
    json.dump(bld_name_freq, f_json)

