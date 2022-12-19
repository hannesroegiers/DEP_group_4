import queue
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine, MetaData, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
import time

options = webdriver.ChromeOptions()
ff_driver = webdriver.Chrome(options=options)

print("start connectie")
try:
    ff_driver.get("https://atd-vierdewereld.be/feed/")
except:
    pass

html = ff_driver.page_source
print(html + "\n")
soup = BeautifulSoup(html, "lxml")
b_lijst = soup.find_all('p')
print(str(b_lijst) + "\n")
website_tekst = "\n".join([x.get_text().strip() for x in b_lijst])
print(website_tekst)
ff_driver.close()

