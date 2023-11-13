
import os
import json
import re
import requests as req
import numpy as np
from bs4 import BeautifulSoup
from zipfile import ZipFile



url_main = "https://vetapteka45.ru"
url_korm = f"{url_main}/catalog/korma/"
suburl_korm_suh = "suhoj-korm/"
suburl_korm_vl= "vlazhnyj-korm-konservy/"
suburl_korm_lak = "lakomstva/"

suburl_korm = [suburl_korm_suh, suburl_korm_vl, suburl_korm_lak]

url_antib = f"{url_main}/catalog/antibiotiki/"

suburl_antib_inj = "inekcionnye/"
suburl_antib = [suburl_antib_inj]

z_numb = 5
file_path = 'tests/' + str(z_numb)
filename = file_path + '/'

json_out_filename = f"{file_path}/"


bld_name_freq = {}
new_info = []
pl = []

antib_name_freq = {}
antib_info = []
antib_pl = []

antib_country_freq = {}

def add_to_dict(m, fdict):
    m = m.lower()
    if m in fdict:
        fdict[m] += 1
    else:
        fdict.setdefault(m, 1)


def get_html_file(url, purl):
    url_get = url + purl
    print("request to : ", url_get)
    resp = req.get(url_get)

    html_filename = f"dl/{purl.split('/')[-2]}.html"
    # print(html_filename)
    if resp.status_code != 200:
        with open(f"{file_path}/{html_filename}", mode="r") as html_f:
            resp = html_f.readlines()

        return "".join(resp)
    else:
        dl_dir = file_path + "/dl"
        if not os.path.exists(dl_dir):
            os.mkdir(dl_dir)

        if not os.path.exists(html_filename):
            with open(f"{file_path}/{html_filename}", mode = "w") as html_f:
                html_f.write(resp.text)

    return resp.text

for curl in suburl_antib:
    resp = get_html_file(url_antib, curl)
    soup = BeautifulSoup(resp, 'html.parser')

    content = soup.find_all("div", {"class" : "catalog_item col-12 col-sm-6 col-md-6 col-lg-3"})
    for cv in content:

        item = {}

        link = cv.find('a', {"class" : "art"})
        href = link.get('href')
        p_name = cv.find("p", {"class" : "name"})

        page = get_html_file(url_main, href)


        item['url'] = href
        item['name'] = p_name.text

        ps = BeautifulSoup(page, 'html.parser')

        discr = ps.find("div", {"class" : "col-12 col-md-7 product_page_description"})

        p_manuf = discr.find("p", {"class" : "manufacturer"})
        p_ishere = discr.find("p", {"class" : "is_here"})

        mname = " ".join(p_manuf.text.split()[1:])
        item['manufacturer'] = mname

        add_to_dict(mname, antib_name_freq)
        
        count = 0
        if p_ishere:
            count = int(p_ishere.text.split()[1].replace(".", ""))

        item['count'] = count

        p_price = discr.find("span", {"class" : "price"})

        item['price'] = int(p_price.text.replace(" ", "")[0:-1])

        antib_pl.append(item['price'])

        tab_content = ps.find("div", {"class" : "tab-content"})
        t_features = tab_content.find("div", {"class" : "features"})

        span_li = iter(t_features.find_all("span"))

        item['features'] = {}
        item['features']['type'] = next(span_li).text
        item['features']['country'] = next(span_li).text

        add_to_dict(item['features']['country'], antib_country_freq)
    
        item['features']['weight'] = next(span_li).text
        
        f_count = 0
        try:
            f_count = int(next(span_li).text.replace(".", ""))
        except:
            pass
            
        item['features']['count'] = f_count

        antib_info.append(item)

outfilename = "antib_outfile.json"
json_out_filename_final = json_out_filename + "final_" + outfilename
json_out_filename_sorted = json_out_filename + "sorted_" + outfilename
json_out_filename_filter = json_out_filename + "filter_" + outfilename
json_out_filename_freq = json_out_filename + "freq_" + outfilename
json_out_filename_freq2 = json_out_filename + "freq2_" + outfilename

#results

print("Results by price")
print(f"min {np.min(antib_pl)}\nmax {np.max(antib_pl)}\nmean {np.mean(antib_pl)}\nstd {np.std(antib_pl)}")

# final

with open(json_out_filename_final, mode="w") as f_json:
    json.dump(antib_info, f_json)

# sort

price_sorted_key = lambda x: x['price']
up_to_down = True

sorted_by_param = sorted(antib_info, key=price_sorted_key, reverse = up_to_down)

with open(json_out_filename_sorted, mode="w") as f_json:
    json.dump(sorted_by_param, f_json)

# filter

def filter_wrapper(filter_value):
    return lambda x : x['count'] > filter_value

filter_by_year = filter_wrapper(50000)
filtered_list = filter(filter_by_year, antib_info)

with open(json_out_filename_filter, mode="w") as f_json:
    lst = [*filtered_list]
    json.dump(lst, f_json)

# FREQ
with open(json_out_filename_freq, mode="w") as f_json:
    json.dump(antib_country_freq, f_json)

with open(json_out_filename_freq2, mode="w") as f_json:
    json.dump(antib_name_freq, f_json)


for curl in suburl_korm:
    resp = get_html_file(url_korm, curl)
            
    soup = BeautifulSoup(resp, 'html.parser')

    content = soup.find_all("div", {"class" : "catalog_item col-12 col-sm-6 col-md-6 col-lg-3"})

    for cv in content:
        item = {}
        price = cv.find("p", {"class" : "price"})
        name = cv.find("p", {"class" : "name"})
        art = cv.find("a", {"class" : "art"})
        desc = cv.find("p", {"class" : "desc"})
        manuf = cv.find("p", {"class" : "manufacturer"})

        item['name'] = name.text
        item['price'] = float(price.text.split()[1])
        pl.append(item['price'])

        item['art'] = art.text.split()[1]
        item['desc'] = desc.text
        
        if manuf:
            m_name = manuf.text.split()[1]
        else:
            m_name = "non"

        item['manufacturer'] = m_name
        add_to_dict(m_name, bld_name_freq)

        pihr = cv.find("div", {"class" : "product_item_hover_right_side_semitems"})

        if pihr:
            variants = pihr.find_all("p")
            item['variants'] = {}
            v_cnt = 0

            for v in variants:
                vi = {}
                
                v_title = v.find("span", {"class" : "title"})
                v_pice = v.find("span", {"class" : "price"})
                v_art = v.find("a", {"class" : "articul"})

                vi['title'] = v_title.text
                vi['price'] = int(v_pice.text.split()[0])
                vi['articul'] = v_art.text.split()[1]

                item['variants'][v_cnt] = vi
                v_cnt += 1
        
        new_info.append(item)   


outfilename = "korm_outfile.json"
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
up_to_down = True

sorted_by_param = sorted(new_info, key=price_sorted_key, reverse = up_to_down)

with open(json_out_filename_sorted, mode="w") as f_json:
    json.dump(sorted_by_param, f_json)

# filter

def filter_wrapper(filter_value):
    return lambda x : x['manufacturer'].lower() == filter_value

filter_by_year = filter_wrapper('farmina')
filtered_list = filter(filter_by_year, new_info)

with open(json_out_filename_filter, mode="w") as f_json:
    lst = [*filtered_list]
    json.dump(lst, f_json)

# FREQ
with open(json_out_filename_freq, mode="w") as f_json:
    json.dump(bld_name_freq, f_json)



