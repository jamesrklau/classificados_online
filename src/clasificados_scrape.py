#clasificados_scrape

import json
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options
from selenium import webdriver
import datetime
import pandas as pd
import re
import argparse
import sqlalchemy
from tqdm import tqdm

def get_args():
    parser = argparse.ArgumentParser(
    description="SuperPages Scraper"
    )

    parser.add_argument("-i", "--infile", type=str,  help="Input table.")
    parser.add_argument("-db", "--database", type=str, required=True, help="Database.")
    parser.add_argument("-s", "--schema", type=str, help="Output file name.")
    parser.add_argument(
            "--log",
            type=lambda a: json.loads(a),
            help="Log results to log DB. Use LOG_API_URL environment variable to change url.",
        )

    return parser.parse_args()

import contextlib
args = get_args()
engine = sqlalchemy.create_engine(args.database)

try:
    data_alread_gathered = pd.read_sql_table(args.infile, engine,schema = args.schema )
    ids_in_db = data_alread_gathered['id'].unique()
except Exception:
    ids_in_db = []

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36"  # Set the user agent string

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument(f'user-agent={user_agent}')
driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)  # Initialize a Firefox WebDriver

url = 'https://clasificadosonline.com/UDREListing.asp?RESPueblos=%25&Category=%25&LowPrice=0&HighPrice=999999999&Bedrooms=%25&Area=&Repo=Repo&Opt=Opt&BtnSearchListing=Ver+Listado&redirecturl=%2Fudrelistingmap.asp&IncPrecio=1'

driver.get(url)

date_scraped = datetime.datetime.today()

tot_data = driver.find_element(By.XPATH, "//span[contains (@class, 'Tahoma16BrownNound')]").text
tot_data_int = int (re.findall(r'\d+', tot_data)[2])
for i in tqdm(range(0, tot_data_int, 30)):
    url = f'https://clasificadosonline.com/UDREListing.asp?RESPueblos=%25&Category=%25&Bedrooms=%25&LowPrice=0&HighPrice=999999999&IncPrecio=1&Area=&Repo=Repo&BtnSearchListing=Ver+Listado&redirecturl=%2Fudrelistingmap%2Easp&Opt=Opt&offset={i}'
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for table_elem in soup.find_all('div' , class_="dv-classified-row dv-classified-row-v2"):
        detail_url = table_elem.find ('input', class_="DetailUrl")['value']
        if detail_url not in ids_in_db:
            price = table_elem .find('font', style="color:#424141f2 !important; font-weight: bold;" ).text
            lat = table_elem.find ('input', class_="Lat")['value']
            lon = table_elem.find ('input', class_="Lon")['value']
            bdyba = re.findall( r'\d+', table_elem.find ('div', style="width: 100%; display: inline-block") .text)
            conds = table_elem.find ('input', class_="BarrioCond")['value']
            urb = table_elem.find_all ('span',style="color: blue !important;" )[0].text
            muni = table_elem.find_all ('span',style="color: blue !important;" )[1].text
            df1 = pd.DataFrame([price,lat,lon,conds,urb,muni, detail_url]).T
            df1.columns = ['price','lat','long','condominio','urb','muni', 'id']
            with contextlib.suppress(Exception):
                df1[['bd', 'ba']] = bdyba[0],bdyba[1]
            df1['date'] = date_scraped.strftime("%Y-%m-%d")
            df1.to_sql(args.infile, engine, if_exists= 'append', schema = args.schema)


