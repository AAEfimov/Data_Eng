
import csv
from bs4 import BeautifulSoup

filename = "tests/text_5_var_40"

csv_del = ','

with open(filename, mode= "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, 'html.parser')

with open(filename+"out", mode= "w", encoding="utf-8") as f:
    table = soup.table
    rows = table.find_all('tr')
    c_w = csv.writer(f, delimiter=csv_del)
    for row in rows:
        col = row.find_all('td')
        if col:
            c_w.writerow([item.text.strip() for item in col])


# https://stackoverflow.com/questions/23377533/python-beautifulsoup-parsing-table
