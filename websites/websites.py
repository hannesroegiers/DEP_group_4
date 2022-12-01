import os
from pathlib import Path

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import googlesearch
import pandas as pd
import datetime
import requests
import numpy as np
from googlesearch import search
from bs4 import BeautifulSoup

#cwd = os.getcwd()
#os.chdir(cwd+'/websites')
df = pd.read_excel("kmo's_Vlaanderen_2021.xlsx",usecols="B,C,D,E,L",sheet_name="Lijst")
blacklist = ["trendstop","bizzy","linkedin","bloomberg","goudengids","cylex","companyweb","febev","openingsuren","facebook","vdab","eita","dnb","wikipedia","kompass","viamichelin"]
counter=0
def eerst_try():
    for index,row in df.iterrows():
        if isinstance(row[4], float):
            for j in search(row[0], num=6):
                aanwezig = False
                for item in blacklist:
                    if item in j:
                        aanwezig = True
                if aanwezig == False:
                    print(j)
                    break
            #print(f"{counter+1} {row[0]}\n")
            #counter += 1


def lees_urls():
    timestamp = datetime.datetime.now()
    succes_log = open("succes_log/website_succes_" + timestamp.strftime("%d-%m-%y-%H%M%S"), "+w")
    error_log = open("error_log/website_errors_" + timestamp.strftime("%d-%m-%y-%H%M%S"), "+w")
    succes_log.write("bedrijfsnaam,matchende_url\n")
    error_log.write("bedrijfsnaam,matchende_urls\n")
    blacklist = ["bmm", "dieterenautoe",
                 "belgian", "coolblue", "trendstop","bizzy","linkedin","bloomberg","goudengids",
                 "cylex","companyweb","febev","openingsuren","facebook","vdab","eita","dnb","wikipedia","kompass",
                 "viamichelin", "staatsbladmonitor", "food.be", "wibilinga", "belgium", "wonderauto"]
    df = pd.read_excel("kmo's_Vlaanderen_2021.xlsx", usecols="B,C,D,E,L", sheet_name="Lijst")
    for index,row in df.iterrows():
        matches = np.array([]).astype('bool')
        url_lijst = np.array([]).astype('<U27')
        for url in search(lang="nl", num=3, pause=2, stop=1, query=row[0]):
            if url not in url_lijst:
                matches = np.append(matches, not any(word in url for word in blacklist))
                url_lijst = np.append(url_lijst, url)
        urls = url_lijst[matches]
        if np.count_nonzero(matches == True) == 1:
            succes_log.write(row[0] + "," + str(urls[0]) + "\n")
            print("SUCCES: " + row[0]
                  + "\nurl: " + str(urls[0])
                  + "\nmatches: " + str(matches))
        else:
            error_log.write(row[0] + "," + str(urls) + "\n")
            print("ERROR: " + row[0]
                  + "\nurl: " + str(urls)
                  + "\nmatches: " + str(matches))


def corrigeer_en_verzamel_urls():
    filepath = Path("final_websites/final_websites.csv")
    blacklist = "ensie|tijd|wvi|onshartkloptvooru|essenscia|vrt|hln|bsearch|vlan|dagelijksekost|buildyourhome|ohgreen|zonnepanelen-expert|volvocars|gids|standaard|stamnummer"
    final_websites = open(filepath, "a")
    final_websites.close()
    for filename in os.listdir("succes_log"):
        print("uitschrijven van " + filename + " naar " + str(filepath))
        urls = pd.read_csv("succes_log/" + filename, encoding="cp1252")
        urls = urls[-urls["matchende_url"].squeeze().str.contains(blacklist)]
        urls.to_csv(filepath, index=False, mode="a")
    for filename in os.listdir("error_log"):
        print("uitschrijven van " + filename + " naar " + str(filepath))
        urls = pd.read_csv("error_log/" + filename, encoding="cp1252")
        urls.to_csv(filepath, index=False, mode="a")


def prepare_uploads():
    urls_extended = pd.DataFrame()
    #urls = pd.read_csv("final_websites/final_websites.csv")
    urls = pd.read_csv("error_log/final_error_file.csv", delimiter=";")
    kmos = pd.read_excel("kmo's_Vlaanderen_2021.xlsx", usecols="B,H", sheet_name="Lijst")
    final_error_file = open("error_log/final_error_file2.csv", mode="a")
    for index, row in urls.iterrows():
        if any(kmos["Naam"].isin([row[0]])):
            odnr = kmos[kmos["Naam"] == row[0]]["Ondernemingsnummer"]
            urls_extended = pd.concat([pd.DataFrame({"ondernemingsnummer": odnr, "bedrijfsnaam": row[0]}), urls_extended])
        else:
            #write to error log
            final_error_file.write(str(row[0]) + ";" + str(row[1]) + "\n")
    urls_resultaat = urls_extended.merge(urls, left_on="bedrijfsnaam", right_on="bedrijfsnaam")
    print(urls_resultaat)
    #urls_resultaat.to_csv(path_or_buf="final_websites/final_websites2.csv", index=False, mode="a")
    urls_resultaat.to_csv(path_or_buf="final_websites/final_websites2_errors.csv", index=False, mode="w", sep=";")


def upload_urls():
    pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
    pg_conn = pg_engine.connect()
    metadata = MetaData(pg_engine)

    pg_Base = declarative_base(pg_engine) # initialize Base class
    pg_Base.metadata.reflect(pg_engine)   # get metadata from database

    Session = sessionmaker(bind=pg_engine)
    pg_session = Session()

    class Website(pg_Base):
        __table__ = pg_Base.metadata.tables['website']

    #urls = pd.read_csv("final_websites/final_websites3.csv", encoding="cp1252")
    urls = pd.read_csv("final_websites/final_websites2_errors.csv", encoding="cp1252", delimiter=";")
    for index, row in urls.iterrows():
        print(row)
        pg_website = Website(ondernemingsnummer=int(row[0].replace(" ", "")), websitetekst="", jaar=2022, url=row[2])
        pg_session.add(pg_website)
    pg_session.commit()
    pg_conn.close()
    pg_session.close()


def remove_doubles():
    #data = pd.read_csv("final_websites/final_websites2.csv")
    data = pd.read_csv("final_websites/final_websites2_errors.csv", delimiter=";")
    data = data.drop_duplicates()
    #data.to_csv(path_or_buf="final_websites/final_websites3.csv", index=False, mode="w")
    data.to_csv(path_or_buf="final_websites/final_websites2_errors.csv", index=False, mode="w", sep=";")


#lees_urls()
#corrigeer_en_verzamel_urls()
prepare_uploads()
remove_doubles()
upload_urls()



