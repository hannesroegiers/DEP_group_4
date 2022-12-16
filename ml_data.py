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
import geopandas as gpd
import matplotlib.pyplot as plt


scraper = cloudscraper.create_scraper()

# initialization of PostgreSQL stuff
pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
pg_conn = pg_engine.connect()
metadata = MetaData(pg_engine)  

pg_Base = declarative_base(pg_engine) # initialize Base class
pg_Base.metadata.reflect(pg_engine)   # get metadata from database

Session = sessionmaker(bind=pg_engine)
pg_session = Session()

def omzet_winst_scrape(ondernemingsnummer):
    ondernemingsnummer = int(ondernemingsnummer)
    url = "https://www.companyweb.be/nl/"+str(ondernemingsnummer)
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'lxml')
    table = soup.find('table')
    omzet = table.find_all(text= True)
    omzet_new = []
    for item in omzet:
        if "\n" not in item:
            omzet_new.append(item)
    print(omzet_new)
    print(len(omzet_new))
    # Case: winst is niet meegegeven, tweede rij is streepje - -
    if  omzet_new[7] == '-':
        omzet = omzet_new[0]
        omzet = omzet[2:]
        real_omzet = omzet.replace('.', '')
        real_omzet = int(real_omzet)
        real_omzet = round(real_omzet/1000)    
    
        winst = 0
        return real_omzet, winst
    if omzet_new[0] == 'Type':
        return 0, 0

    # Case: eerste rij (omzet) zijn streepjes - - - - - -
    if omzet_new[0] == '-':
        real_omzet = 0
        winst = omzet_new[7]
        winst = winst[2:]
        winst = winst.replace('.', '')
        winst = int(winst)
        winst = round(winst/1000)
        return real_omzet, winst

    # Case: eerste rij is winst: dus lengte van array is 27
    if len(omzet_new) == 27:
        real_omzet = 0
        winst = omzet_new[0]
        winst = winst[2:]
        winst = winst.replace('.', '')
        winst = int(winst)
        winst = round(winst/1000)
        return real_omzet, winst

    # Case: eerste kolom is niet beschikbaar
    if len(omzet_new) == 11 or len(omzet_new) == 14:
        omzet = omzet_new[0]
        omzet = omzet[2:]
        real_omzet = omzet.replace('.', '')
        real_omzet = int(real_omzet)
        real_omzet = round(real_omzet/1000)

        winst = omzet_new[3]
        winst = winst[2:]
        winst = winst.replace('.', '')
        winst = int(winst)
        winst = round(winst/1000)
        return real_omzet, winst
    # Case: alle data is beschikbaar
    omzet = omzet_new[0]
    omzet = omzet[2:]
    real_omzet = omzet.replace('.', '')
    real_omzet = int(real_omzet)
    real_omzet = round(real_omzet/1000)

    winst = omzet_new[7]
    winst = winst[2:]
    winst = winst.replace('.', '')
    winst = int(winst)
    winst = round(winst/1000)

    return real_omzet, winst


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

def geef_provincie_frame():
    url = "https://www.metatopos.eu/belgcombiN.html"
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'lxml')
    table = soup.find_all('table')
    df = pd.read_html(str(table))[0]

    df.drop(
        labels=[0],
        axis=0,
        inplace=True
    )

    df.drop(
        labels=[3,4,5,6],
        axis=1,
        inplace=True
    )

    df.rename(columns = {0 :'Postcode'}, inplace = True)
    df.rename(columns = {1: 'Gemeente'}, inplace = True)
    df.rename(columns = {2: 'Deelgemeente' }, inplace = True)
    
    df['Deelgemeente'] = df['Deelgemeente'].fillna(0)
    deelgemeenten = []
    for i in range(len(df)):
        if df.iloc[i,2] == 0:
            df.iloc[i,2] = df.iloc[i,1]
    return df

def geef_gemeente(df, postcode): 
  return df[df.iloc[:, 0] == postcode].iloc[0, 1]

def opvullen_alle_gemeenten():
    class PG_SME(pg_Base):
        __table__ = pg_Base.metadata.tables['gemeente']
    
    xl_file = pd.read_excel("DEP_group_4/websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")
    xl_file = xl_file.sort_values('Postcode')

    dataframe = geef_provincie_frame()
    

    for rij in xl_file.values:
        deelgemeente = rij[2]
        postcode = int(rij[8])

        print(" ")
        print("Postcode in behandeling: " + str(postcode) + " Deelgemeente: " + deelgemeente)
        
        if postcode == 1931:
            gemeente = 'Machelen'
            pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.deelgemeente == deelgemeente)
                        .values(gemeente=gemeente)
                    )
            continue
        if postcode == 9950 or postcode == 9930:
            gemeente = 'Lievegem'
            pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.deelgemeente == deelgemeente)
                        .values(gemeente=gemeente)
                    )
            continue
        if postcode == 9770 or postcode == 9750:
            gemeente = 'Kruisem'
            pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.deelgemeente == deelgemeente)
                        .values(gemeente=gemeente)
                    )
            continue
        try:

            gemeente = geef_gemeente(dataframe, str(postcode))

            print("Postcode: " + str(postcode) + " Gemeente: " + str(gemeente))
        
#            pg_session.add(PG_SME(deelgemeente=deelgemeente, provincie=provincie, provinciehoofdstad=provinciehoofdstad, postcode=postcode))
            pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.deelgemeente == deelgemeente)
                        .values(gemeente=gemeente)
                    )
            print("Postcode afgehandeld ")
        except exc.SQLAlchemyError as e:
            pg_session.rollback()
        pg_session.commit()

def geef_verstedelijking(filename):
    df = pd.read_csv('GSM_ranking_register_provincie.csv')

    # Alleen data van 2021
    # Alleen data van gemeentes met meer dan 5000 en minder dan 100.000 inwoners
    df = df[df['Jaar'] >= 2020]

    # Verwijderen irrelevante groepen (min, max, gem, groepering)
    df = df.drop('Gemeentegrootte', axis = 1)
    df = df.drop('Waarde Vlaams Gewest', axis = 1)
    df = df.drop('Minimum Vlaams Gewest', axis = 1)
    df = df.drop('Maximum Vlaams Gewest', axis = 1)
    df = df.drop('Minimum provincie', axis = 1)
    df = df.drop('Maximum provincie', axis=1)
    df = df.drop('Thema', axis=1)
    df = df.drop('Provincie', axis = 1)
    df = df.drop('Waarde provincie', axis = 1)
    df = df.drop('Rangschikking', axis = 1)


    bebouwingsgraad = df[df['Indicator'] == 'RU01: Bebouwingsgraad']
    bebouwingsgraad = bebouwingsgraad[bebouwingsgraad['Jaar'] == 2020]
    bebouwingsgraad = bebouwingsgraad.drop('Indicator', axis=1)

    print("Bebouwingsgraad:")
    print(bebouwingsgraad)

    shapefile = gpd.read_file(filename)

    return shapefile
    #lu_vrl_vlaa_2013.shp
def geef_verstedelijkingsgraad():
    df = pd.read_csv('GSM_ranking_register_provincie.csv')

    # Alleen data van 2020
    # Alleen data van gemeentes met meer dan 5000 en minder dan 100.000 inwoners
    df = df[df['Jaar'] >= 2020]

    # Verwijderen irrelevante groepen (min, max, gem, groepering)
    df = df.drop('Gemeentegrootte', axis = 1)
    df = df.drop('Waarde Vlaams Gewest', axis = 1)
    df = df.drop('Minimum Vlaams Gewest', axis = 1)
    df = df.drop('Maximum Vlaams Gewest', axis = 1)
    df = df.drop('Minimum provincie', axis = 1)
    df = df.drop('Maximum provincie', axis=1)
    df = df.drop('Thema', axis=1)
    df = df.drop('Provincie', axis = 1)
    df = df.drop('Waarde provincie', axis = 1)
    df = df.drop('Rangschikking', axis = 1)

    bebouwingsgraad = df[df['Indicator'] == 'RU01: Bebouwingsgraad']
    bebouwingsgraad = bebouwingsgraad[bebouwingsgraad['Jaar'] == 2020]
    bebouwingsgraad = bebouwingsgraad.drop('Indicator', axis=1)
    bebouwingsgraad = bebouwingsgraad.drop('Jaar', axis=1)
    bebouwingsgraad = bebouwingsgraad.drop('Inwoners', axis=1)
    print("Bebouwingsgraad:")
    print(bebouwingsgraad)

    return bebouwingsgraad

def opvullen_alle_verstedelijkingsgraden():
    class PG_SME(pg_Base):
        __table__ = pg_Base.metadata.tables['gemeente']
    
    verstedelijkingsgraden = geef_verstedelijkingsgraad()


    for rij in verstedelijkingsgraden.values:
        print(rij)
        gemeente = rij[0]
        verstedelijkingsgraad = rij[1]
        print(" ")
        print("Gemeente in behandeling: " + str(gemeente) + " Verstedelijkingsgraad: " + str(verstedelijkingsgraad))
        
        try:
            pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.gemeente == gemeente)
                        .values(verstedelijkingsgraad=verstedelijkingsgraad)
                    )

            print("Postcode afgehandeld ")
        except exc.SQLAlchemyError as e:
            pg_session.rollback()
        pg_session.commit()

def opvullen_winst_omzet_machinelearningdata():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['machinelearningData']

    ondernemingsnummers = pg_session.query(PG_SME.ondernemingsnummer).where(PG_SME.winst == None)

    for rij in ondernemingsnummers:
        ondernemingsnummer = rij.ondernemingsnummer
        print(ondernemingsnummer)

        omzet, winst = omzet_winst_scrape(ondernemingsnummer)
        print("omzet: " + str(omzet) + ", winst: " + str(winst))

        print("Ondernemingsnummer: " + str(ondernemingsnummer) + "  Omzet: " + str(omzet) + " Winst: " + str(winst))
        
        pg_session.execute(
                update(PG_SME)
                .where(PG_SME.ondernemingsnummer == ondernemingsnummer)
                .values(omzet=omzet)
            )      
        pg_session.commit()
        break
try:
    #opvullen_alle_oprichtingsjaren()
    #opvullen_alle_beursgenoteerd()

    #machinelearningdata_opvullen()
    #opvullen_alle_gemeentedata()
    #clean_up_gemeentes()
    #opvullen_alle_gemeenten()
    #geef_verstedelijkingsgraad()
    #opvullen_alle_verstedelijkingsgraden()
    #print(omzet_winst_scrape(206460639))
    opvullen_winst_omzet_machinelearningdata()
    #print('Nothing to see here')
 

finally:    
    pg_session.close()
    pg_conn.close()
    print("Connections closed")


    