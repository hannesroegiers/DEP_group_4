import pandas as pd

from sqlalchemy import select, update
from sqlalchemy import create_engine, func, Table, MetaData, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
import pdfplumber
import os
import requests
from bs4 import BeautifulSoup
import time

# initialization of PostgreSQL stuff
pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
pg_conn = pg_engine.connect()
metadata = MetaData(pg_engine)  

pg_Base = declarative_base(pg_engine) # initialize Base class
pg_Base.metadata.reflect(pg_engine)   # get metadata from database

Session = sessionmaker(bind=pg_engine)
pg_session = Session()

def machinelearningdata_opvullen():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    xl_file = pd.read_excel("websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

    for rij in xl_file.values:
        
        print(rij[1])
        # table machinelearningData vullen vanuit de excel
        omzet = rij[5] if rij[5] != 'n.b.' else 0
        pg_machinelearningData = PG_SME(ondernemingsnummer=int(rij[7].replace(' ','')), jaar=2021, personeelsledenAantal= rij[4], omzet= omzet)
        pg_session.add(pg_machinelearningData)
    
    pg_session.commit()

def scrape_oprichtingsjaar(ondernemingsnummer):
    URL = "https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?lang=nl&nummer="+str(ondernemingsnummer)+"&actionLu=Recherche"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    result = soup.find("span", {"class":"upd"}).text #output : 'Sinds 9 augustus 1960'
    result = result[::-1] #output : '0691 sutsugua sdniS'
    result = result[:4] #output : '0691'
    result = int(result[::-1]) #output : '1960'
    return(result)

def opvullen_alle_oprichtingsjaren():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    xl_file = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

    for rij in xl_file.values:
        
        ondernemingsnummer = rij[7].replace(' ','')
        ondernemingsnummer = ondernemingsnummer[1:]
        try:
            oprichtingsjaar = scrape_oprichtingsjaar(ondernemingsnummer)
            print("Ondernemingsnummer: " + ondernemingsnummer + "  Oprichtingsjaar: " + str(oprichtingsjaar))
            pg_machinelearningData = PG_SME(oprichtingsjaar=oprichtingsjaar)
            pg_session.add(pg_machinelearningData)
        except:
            pass
        
    pg_session.commit()

try:

    opvullen_alle_oprichtingsjaren()
#machinelearningdata_opvullen()

finally:    
    pg_session.close()
    pg_conn.close()
    print("Connections closed")
