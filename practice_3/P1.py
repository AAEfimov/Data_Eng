
import os
import json
import re
from aux_func import *
from bs4 import BeautifulSoup
import numpy as np

z_numb = 1
zip_file_name = 'zip_var_40.zip'
file_path = 'tests/' + str(z_numb)

filename = file_path + '/' + zip_file_name
json_out_filename = f"{file_path}/"

bld_name_freq = {}
new_info = []
ly = []

for next_file in get_next_file_from_zip(filename, file_path):

    item = {}
    item['filename'] = os.path.basename(next_file)
    
    with open(next_file, mode='r') as html_fp:
        soup = BeautifulSoup(html_fp, 'html.parser')

        bw = soup.find("div", {"class": "build-wrapper"})

        div_iter = iter(bw.find_all("div"))

        # city
        elem_city = next(div_iter)
        item['city'] = elem_city.find("span").text.split(':')[1].strip()

        # building address
        elem_building = next(div_iter)
        building = elem_building.find("h1", {"class" : "title", "id" : "1"})
        item['building'] = {}
        bld = building.text.split(':')[1]

        building_num = re.search(r'\d+', bld)
        building_name = bld.replace(building_num[0], '')

        bld_name = building_name.strip()

        item['building']['name'] = bld_name
        item['building']['number'] = int(building_num[0])

        add_to_dict(bld_name, bld_name_freq)

        #street
        item['address'] = {}
        street = elem_building.find("p", {'class' : 'address-p'})

        str_clean = re.sub("^\s+|\n|\r|\s+$", '', street.text)
        str_clean = str_clean.replace("Улица:", '').replace("Индекс:", ' ').split()

        item['address']['street'] = f"{str_clean[0]} {str_clean[1]}"
        item['address']['num'] = int(str_clean[2])
        item['address']['index'] = int(str_clean[3])

        elem_building_info = next(div_iter)
        ebi_span_iter = iter(elem_building_info.find_all("span"))

        item['building_info'] = {}

        eb_floors = next(ebi_span_iter)

        item['building_info']['floors'] = int(re.search("\d+", eb_floors.text)[0])

        eb_year = next(ebi_span_iter)

        item['building_info']['year'] = int(re.search("\d+", eb_year.text)[0])

        ly.append(item['building_info']['year'])

        eb_parking = next(ebi_span_iter)
        item['building_info']['parking'] = eb_parking.text.split(':')[1].strip()

        # img
        img_src = next(div_iter)

        img_url_f = img_src.find('img')
        img_url_a = ''

        if img_url_f:
            img_url_a = img_url_f.get('src')

        item['building_photo'] = img_url_a

        # reit / view

        revi = next(div_iter)

        revi_iter = iter(revi.find_all('span'))

        reit = next(revi_iter)
        item['add_info'] = {}
        item['add_info']['reit'] = float(reit.text.split(':')[1].strip())

        views = next(revi_iter)

        item['add_info']['views'] = int(views.text.split(':')[1].strip())

        new_info.append(item)

outfilename = "outfile.json"
json_out_filename_final = json_out_filename + "final_" + outfilename
json_out_filename_sorted = json_out_filename + "sorted_" + outfilename
json_out_filename_filter = json_out_filename + "filter_" + outfilename
json_out_filename_freq = json_out_filename + "freq_" + outfilename

#results

print(f"min_year {np.min(ly)}\nmax_year {np.max(ly)}\nsum_of_years {np.sum(ly)}\nmid_year {np.mean(ly)}\ncnt {len(ly)}")

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

