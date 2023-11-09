
import os
import json
import re
import numpy as np
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
    pl = []

    for f in zf.infolist():
        zf.extract(f.filename, ext_dir)
        ext_filename = os.path.join(ext_dir, f.filename)

        with open(ext_filename, mode='r') as html_fp:
            soup = BeautifulSoup(html_fp, 'html.parser')

            s_head = soup.head
            s_body = soup.body

            div_flex_wrap = s_body.find("div", {"class" : "list flex-wrap"})

            for div_pad in div_flex_wrap.find_all("div", {"class" : "pad"}):

                item = dict()
                div_prod_item = div_pad.find("div", {"class" : "product-item"})

                a_links = div_prod_item.find_all("a")
                data_id = a_links[0].get('data-id')
                a_href = a_links[1].get('href')

                item['link_attrib'] = {}
                item['link_attrib']['data_id'] = int(data_id)
                item['link_attrib']['href'] = a_href

                img_url = div_pad.find("img")
                item['img'] = {}
                item['img']['loading'] = img_url.get('loading')
                item['img']['src'] = img_url.get('src')

                full_param = div_pad.find("span").text.split()

                item['device_param'] = {}
                item['device_param']['inch'] = full_param[0][0:-1]
                item['device_param']['company'] = " ".join(full_param[1:-1])
                item['device_param']['flash'] = full_param[-1][0:-2]

                company_name = item['device_param']['company'].lower()
                if company_name in bld_name_freq:
                    bld_name_freq[company_name] += 1
                else:
                    bld_name_freq.setdefault(company_name, 1)

                price = "".join(div_pad.find('price').text.split()[:-1])

                item['price'] = int(price)

                pl.append(item['price'])

                strong_text = div_pad.find('strong').text

                bonus_val = re.search(r"\d+", strong_text)

                item['bonus'] = int(bonus_val[0])

                li = div_pad.find_all("li")
                
                item['prop'] = {}
                for p in li:
                    prop = p.get('type')
                    p_val = p.text.split()
                    match prop:
                        case 'processor':   
                            item['prop']['processor'] = {}
                            cpu = p_val[0].split('x')
                            item['prop']['processor']['cores'] = int(cpu[0])
                            item['prop']['processor']['freq'] = float(cpu[1])
                        case 'ram':
                            item['prop']['ram'] = int(p_val[0])
                        case 'matrix':
                            item['prop']['matrix'] = p_val[0]
                        case 'acc':
                            item['prop']['acc'] = int(p_val[0])
                        case 'resolution':
                            # p_val  # TODO x and y
                            item['prop']['resolution'] = p.text
                        case 'camera':
                            item['prop']['camera'] = int(p_val[0])
                        case 'sim':
                            item['prop']['sim'] = int(p_val[0])
                        case _:
                            print("ADD CASE FOR", prop, p.text)

                new_info.append(item)

outfilename = "outfile.json"
json_out_filename_final = json_out_filename + "final_" + outfilename
json_out_filename_sorted = json_out_filename + "sorted_" + outfilename
json_out_filename_filter = json_out_filename + "filter_" + outfilename
json_out_filename_freq = json_out_filename + "freq_" + outfilename

#results

print("Results by price")
print(f"min {np.min(pl)}\nmax {np.max(pl)}\nmean {np.mean(pl)}\nstd {np.std(pl)}")

# final

with open(json_out_filename_final, mode="w") as f_json:
    json.dump(new_info, f_json)

# sort

price_sorted_key = lambda x: x['price']
bonus_sorted_key = lambda x: x['bonus']
camera_sorted_key = lambda x: x['prop']['camera'] if 'camera' in x['prop'] else 0

up_to_down = True

sorted_by_param = sorted(new_info, key=camera_sorted_key, reverse = up_to_down)

with open(json_out_filename_sorted, mode="w") as f_json:
    json.dump(sorted_by_param, f_json)

# filter

def filter_wrapper(filter_value):
    return lambda x : x['price'] > filter_value

filter_by_year = filter_wrapper(200000)
filtered_list = filter(filter_by_year, new_info)

with open(json_out_filename_filter, mode="w") as f_json:
    lst = [*filtered_list]
    json.dump(lst, f_json)

# FREQ
with open(json_out_filename_freq, mode="w") as f_json:
    json.dump(bld_name_freq, f_json)

