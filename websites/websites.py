import os
import pandas as pd
import requests
from googlesearch import search
from bs4 import BeautifulSoup


df = pd.read_excel("websites\kmo's_Vlaanderen_2021.xlsx",usecols="B,C,D,E,L",sheet_name="Lijst")
blacklist = ["trendstop","bizzy","linkedin","bloomberg","goudengids","cylex","companyweb","febev","openingsuren","facebook","vdab","eita","dnb","wikipedia","kompass","viamichelin"]
counter=0

for index,row in df.iterrows():
    if isinstance(row[4],float):
        for j in search(row[0],num_results=6):
            aanwezig = False
            for item in blacklist:
                if item in j:
                    aanwezig=True
            if aanwezig ==False:
                print(j)
                break
        print(f"{counter+1} {row[0]}\n")
        counter+=1

