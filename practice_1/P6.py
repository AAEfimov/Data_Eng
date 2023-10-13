
import json
import requests
from bs4 import BeautifulSoup, Tag

filename = "tests/news.html"

# https://newsapi.org/
api_key = "b0889e575d814923be3872606ffd4369"

news_about = "tesla"
from_date = "2023-09-13"
sortBy = "publishedAt"
lang = "en"

URL = f"https://newsapi.org/v2/everything?q={news_about}&language={lang}&from={from_date}&sortBy={sortBy}&apiKey={api_key}"

req = requests.get(URL)

data = req.json()

soup = BeautifulSoup()

html = Tag(soup, name="html")
table = Tag(soup, name="table")
table['border'] = 1
table['cellspacing'] = 1 
table['cellpadding'] = 1

soup.append(html)
html.append(table)

for art in data['articles']:
    tr = Tag(soup, name="tr")
    table.append(tr)
    for k, v in art.items():
        if k == 'source':
            continue
        td = Tag(soup, name="td")
        tr.append(td)
        if v :
            if k == 'urlToImage':
                img = Tag(soup, name="img")
                img['src'] = v
                img['style'] = "width:104px;height:142px;"
                td.append(img)
            elif k == 'url':
                url = Tag(soup, name="a")
                url['href'] = v
                url.append("News Link")
                td.append(url)
            else:
                td.append(v)
        else:
            td.append("-")

with open(filename, mode="w", encoding="utf8") as f:
    f.write(soup.prettify())


