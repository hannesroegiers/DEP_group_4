import os
import pandas as pd
import requests
from bs4 import BeautifulSoup


df = pd.read_excel("websites\kmo's_Vlaanderen_2021.xlsx",usecols="B,C,D,E,L",sheet_name="Lijst")
website=0
geen_website=0
counter=0
max=10
for index,row in df.iterrows():
    if isinstance(row[4],str):
        website+=1 
    
print(geen_website)
print(website)

        
