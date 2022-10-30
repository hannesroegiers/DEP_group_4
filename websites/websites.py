import os
import pandas as pd
import requests
from googlesearch import search
from bs4 import BeautifulSoup


df = pd.read_excel("websites\kmo's_Vlaanderen_2021.xlsx",usecols="B,C,D,E,L",sheet_name="Lijst")


website=0
geen_website=0
counter=0
max=10
for index,row in df.iterrows():
    if counter <= max:
        if isinstance(row[4],float):
            for j in search(row[0],num_results=2):
                print(j)
            print(f'{counter+1}\n')
            counter+=1



        
