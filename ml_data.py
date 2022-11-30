import pandas as pd

from sqlalchemy import select, update
from sqlalchemy import create_engine, func, Table, MetaData, desc, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import sqlalchemy
import psycopg2
import pdfplumber
import os
import requests
from bs4 import BeautifulSoup
import time
import numpy as np
import cloudscraper

scraper = cloudscraper.create_scraper()

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

def geef_oprichtingsjaar(result, ondernemingsnummer):


    end = result.loc[result['EnterpriseNumber'] == ondernemingsnummer]['StartDate'].values[0]

    jaar = end[6:]

    return(jaar)

def geef_oprichtingsjaren():
    all_enterprises = pd.read_csv("DEP_group_4/websites/enterprise.csv")
    start_dates = all_enterprises.loc[:,["EnterpriseNumber","StartDate"]]
    ondernemingsnummers = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst", usecols=[7])
    
    start_dates['EnterpriseNumber'] = start_dates['EnterpriseNumber'].astype('str').str.replace(r".", r"", regex=False)
    ondernemingsnummers['Ondernemingsnummer'] = ondernemingsnummers['Ondernemingsnummer'].astype('str').str.replace(r" ", r"",regex=False)
    
    result = start_dates[start_dates['EnterpriseNumber'].isin(ondernemingsnummers['Ondernemingsnummer'].to_list())]

    return result

def geef_beursgenoteerden():
    # Return dataframe of all beursgenoteerde companies from Belgium, without 'BE'
    all_enterprises = pd.read_excel("DEP_group_4/websites/Euronext_Equities_2022-11-25.xlsx", usecols=[1])

    result = all_enterprises[3:][all_enterprises['ISIN'].str.contains('BE', regex=True, na=True)]
    result = result['ISIN'].astype('str').str.replace(r'BE', r'', regex=False).reset_index(drop=True)
    return result

def geef_beursgenoteerd(status_beursgenoteerd, ondernemingsnummer):
     # Returns boolean True if ondernemingsnummer in status_beursgenoteerd, returns false if not 

    #result = status_beursgenoteerd.loc[status_beursgenoteerd['ISIN'] == ondernemingsnummer]['ISIN'].values[0]

    if ondernemingsnummer in status_beursgenoteerd.unique():
        return True
    else: 
        return False

def geef_provincie_en_hoofdstad(postcode):
    postcode = int(postcode)
    if postcode >= 1000 and postcode <= 1299:
        provincie = 'Brussels Hoofdstedelijk Gewest'
        provinciehoofdstad = 'Brussel'
    if postcode >= 1500 and postcode <=1999:
        provincie = 'Vlaams-Brabant'
        provinciehoofdstad = 'Leuven'
    if postcode >= 3000 and postcode <=3499:
        provincie = 'Vlaams-Brabant'
        provinciehoofdstad = 'Leuven'
    if postcode >= 2000 and postcode <= 2999:
        provincie = 'Antwerpen'
        provinciehoofdstad = 'Antwerpen'
    if postcode >= 3500 and postcode <= 3999:
        provincie = 'Limburg'
        provinciehoofdstad = 'Hasselt' 
    if postcode >= 8000 and postcode <= 8999:
        provincie = 'West-Vlaanderen'
        provinciehoofdstad = 'Brugge'
    if  postcode >= 9000 and postcode < 9999:
        provincie = 'Oost-Vlaanderen'
        provinciehoofdstad = 'Gent'

    return([provincie,provinciehoofdstad])

def opvullen_alle_gemeentedata():
    class PG_SME(pg_Base):
        __table__ = pg_Base.metadata.tables['gemeente']


    xl_file = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")
    xl_file = xl_file.sort_values('Postcode')

    for rij in xl_file.values:
        deelgemeente = rij[2]
        postcode = int(rij[8])
        try:
            [provincie, provinciehoofdstad] = geef_provincie_en_hoofdstad(postcode)
            print("Deelgemeente: "+ deelgemeente + " Provincie: " + provincie + " Provinciehoofdstad: " + provinciehoofdstad + " Postcode: " + str(postcode))

#            pg_session.add(PG_SME(deelgemeente=deelgemeente, provincie=provincie, provinciehoofdstad=provinciehoofdstad, postcode=postcode))
            pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.deelgemeente == deelgemeente)
                        .values(postcode=postcode)
                    )
        except exc.SQLAlchemyError as e:
            pg_session.rollback()
        pg_session.commit()

def opvullen_alle_beursgenoteerd():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    class PG_SME2(pg_Base):
        __table__ = pg_Base.metadata.tables['kmo']


    xl_file = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")
    xl_file = xl_file.sort_values('Ondernemingsnummer')
    

    status_beursgenoteerd = geef_beursgenoteerden()

    for rij in xl_file.values:
        
        ondernemingsnummer = rij[7].replace(' ','')
        
        try:
            beursgenoteerd = geef_beursgenoteerd(status_beursgenoteerd, ondernemingsnummer)
            print(ondernemingsnummer)
            ondernemingsnummer = ondernemingsnummer[1:]
            print("Ondernemingsnummer: " + ondernemingsnummer + "  Status Beursgenoteerd: " + str(beursgenoteerd))
            print(rij[1])

            pg_session.execute(
                    update(PG_SME)
                    .where(PG_SME.ondernemingsnummer == ondernemingsnummer)
                    .values(beursgenoteerd=beursgenoteerd)
                )           
        except:
            pass
        
    pg_session.commit() 

def opvullen_alle_oprichtingsjaren():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    xl_file = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")
    xl_file = xl_file.sort_values('Ondernemingsnummer')
    

    result = geef_oprichtingsjaren()

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

def clean_up_gemeentes():
    class PG_SME(pg_Base):
        __table__ = pg_Base.metadata.tables['gemeente']
    
        pg_session.execute(
                        "DELETE FROM public.gemeente WHERE deelgemeente = 'MACHELEN '"
                    )
        print("done")
        pg_session.commit()

try:
    #opvullen_alle_oprichtingsjaren()
    #opvullen_alle_beursgenoteerd()
    #machinelearningdata_opvullen()
    #opvullen_alle_gemeentedata()
    #clean_up_gemeentes()
    print('Nothing to see here')
 

finally:    
    pg_session.close()
    pg_conn.close()
    print("Connections closed")


    