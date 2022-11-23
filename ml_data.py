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
import numpy as np

# initialization of PostgreSQL stuff
pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
pg_conn = pg_engine.connect()
metadata = MetaData(pg_engine)  

pg_Base = declarative_base(pg_engine) # initialize Base class
pg_Base.metadata.reflect(pg_engine)   # get metadata from database

Session = sessionmaker(bind=pg_engine)
pg_session = Session()

""" def machinelearningdata_opvullen():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    xl_file = pd.read_excel("websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

    for rij in xl_file.values:
        
        print(rij[1])
        # table machinelearningData vullen vanuit de excel
        omzet = rij[5] if rij[5] != 'n.b.' else 0
        pg_machinelearningData = PG_SME(ondernemingsnummer=int(rij[7].replace(' ','')), jaar=2021, personeelsledenAantal= rij[4], omzet= omzet)
        pg_session.add(pg_machinelearningData)
    
    pg_session.commit() """


def geef_oprichtingsjaar(result, ondernemingsnummer):


    end = result.loc[result['EnterpriseNumber'] == ondernemingsnummer]['StartDate'].values[0]

    jaar = end[6:]

    return(jaar)




def opvullen_alle_oprichtingsjaren():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    xl_file = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")
    xl_file = xl_file.sort_values('Ondernemingsnummer')
    
    all_enterprises = pd.read_csv("DEP_group_4/websites/enterprise.csv")
    start_dates = all_enterprises.loc[:,["EnterpriseNumber","StartDate"]]
    ondernemingsnummers = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst", usecols=[7])
    
    start_dates['EnterpriseNumber'] = start_dates['EnterpriseNumber'].astype('str').str.replace(r".", r"", regex=False)
    ondernemingsnummers['Ondernemingsnummer'] = ondernemingsnummers['Ondernemingsnummer'].astype('str').str.replace(r" ", r"",regex=False)
    
    result = start_dates[start_dates['EnterpriseNumber'].isin(ondernemingsnummers['Ondernemingsnummer'].to_list())]

    for rij in xl_file.values:
        
        ondernemingsnummer = rij[7].replace(' ','')
        
        try:
            oprichtingsjaar = geef_oprichtingsjaar(result, ondernemingsnummer)
            ondernemingsnummer = ondernemingsnummer[1:]
            print("Ondernemingsnummer: " + ondernemingsnummer + "  Oprichtingsjaar: " + str(oprichtingsjaar))
            print(rij[1])
            omzet = rij[5] if rij[5] != 'n.b.' else 0
            pg_session.execute(
                    update(PG_SME)
                    .where(PG_SME.ondernemingsnummer == ondernemingsnummer)
                    .values(oprichtingsjaar=oprichtingsjaar)
                )           
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


    