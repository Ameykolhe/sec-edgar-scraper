from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

headers = {
            "User-Agent": f"Amey kolheamey99@gmail.com"
        }

resp2 = requests.get("https://www.sec.gov/Archives/edgar/data/1045810/000104581015000036/R9.htm", headers=headers)
soup2 = BeautifulSoup(resp2.content, "lxml")

table2 = soup2.find("table")
df2 = pd.read_html(StringIO(str(table2)))[0]

df2


resp3 = requests.get("https://www.sec.gov/Archives/edgar/data/1045810/000104581024000029/R9.htm", headers=headers)
soup3 = BeautifulSoup(resp3.content, "lxml")

table3 = soup3.find("table")

df3 = pd.read_html(StringIO(str(table3)))[0]

df3