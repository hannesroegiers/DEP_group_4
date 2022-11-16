import pandas as pd

from sqlalchemy import select, update
from sqlalchemy import create_engine, func, Table, MetaData, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
import pdfplumber
import os

# initialization of PostgreSQL stuff
pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
pg_conn = pg_engine.connect()
metadata = MetaData(pg_engine)  

pg_Base = declarative_base(pg_engine) # initialize Base class
pg_Base.metadata.reflect(pg_engine)   # get metadata from database

Session = sessionmaker(bind=pg_engine)
pg_session = Session()

# class PG_SME(pg_Base):  # each table is a subclass from the Base class
#     __table__ = pg_Base.metadata.tables['jaarverslag']


def bepaalSector(nummer:int) -> str:
    afdeling = nummer // 1000
    if 1 <= afdeling <= 3:
        return "Landbouw, bosbouw en visserij"
    elif 5 <= afdeling <= 9:
        return "Winning van delfstoffen"
    elif 10 <= afdeling <= 33:
        return "Industrie"
    elif 35 == afdeling:
        return "Productie en distributie van elektriciteit, gas, stoom en gekoelde lucht"
    elif 36 <= afdeling <= 39:
        return "Distributie van water; afval- en afvalwaterbeheer en sanering"
    elif 41 <= afdeling <= 43:
        return "Bouwnijverheid"
    elif 45 <= afdeling <= 47:
        return "Groot- en detailhandel; reparatie van auto's en motorfietsen"
    elif 49 <= afdeling <= 53:
        return "Vervoer en opslag"
    elif 55 <= afdeling <= 56:
        return "Verschaffen van accommodatie en maaltijden"
    elif 58 <= afdeling <= 63:
        return "Informatie en communicatie"
    elif 64 <= afdeling <= 66:
        return "Financiële activiteiten en verzekeringen"
    elif 68 == afdeling:
        return "Exploitatie van en handel in onroerend goed"
    elif 69 <= afdeling <= 75:
        return "Vrije beroepen en wetenschappelijke en technische activiteiten"
    elif 77 <= afdeling <= 82:
        return "Administratieve en ondersteunende diensten"
    elif 84 == afdeling:
        return "Openbaar bestuur en defensie; verplichte sociale verzekeringen"
    elif 85 == afdeling:
        return "Onderwijs"
    elif 86 <= afdeling <= 88:
        return "Menselijke gezondheidszorg en maatschappelijke dienstverlening"
    elif 90 <= afdeling <= 93:
        return "Kunst, amusement en recreatie"
    elif 94 <= afdeling <= 96:
        return "Overige diensten"
    elif 97 <= afdeling <= 98:
        return "Huishoudens als werkgever; niet-gedifferentieerde productie van goederen en diensten door huishoudens voor eigen gebruik"
    elif 99 == afdeling:
        return "Extraterritoriale organisaties en lichamen"
    else:
        return f"WRONG CODE: {afdeling}"

def kmo_opvullen():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['kmo']

    xl_file = pd.read_excel("websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

    for rij in xl_file.values:
        
        print(rij[1])
        # table 'kmo' vullen vanuit de excel
        sector = bepaalSector(rij[3])
        pg_kmo = PG_SME(ondernemingsnummer=int(rij[7].replace(' ','')), bedrijfsnaam= rij[1], adres=  rij[9], website=  rij[11], sector= sector, gemeente=  rij[2])
        pg_session.add(pg_kmo)
    
    pg_session.commit()

def jaarverslag_opvullen():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['jaarverslag']

    xl_file = pd.read_excel("websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

    for rij in xl_file.values:
        
        print(rij[1])
        # table jaarverslag vullen vanuit de excel
        omzet = rij[5] if rij[5] != 'n.b.' else 0
        pg_jaarverslag = PG_SME(ondernemingsnummer=int(rij[7].replace(' ','')), jaar=2021, personeelsbestand= rij[4], omzet= omzet, totaal_activa= rij[6], tekst= "")
        pg_session.add(pg_jaarverslag)
    
    pg_session.commit()

def jaarverslag_tekst_toevoegen():
    class PG_SME(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['jaarverslag']

    # table jaarverslag tekst toevoegen
    directory = 'PDF'
    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)
        print(path)
        ondernummer = (path.split('_')[1])
        try:
            with pdfplumber.open(path) as pdf:
                jaarverslag = ""
                for page in pdf.pages:
                    print(page.page_number, end='\t')
                    jaarverslag += page.extract_text()

                kmo = pg_session.execute(
                        update(PG_SME)
                        .where(PG_SME.ondernemingsnummer == ondernummer)
                        .values(tekst=jaarverslag)
                    )
            pg_session.commit()
        except(ValueError):
            None
        finally:
            pdf.close()

def domeinen_toevoegen():
    class PG_sub(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['subdomein']
    class PG_term(pg_Base):  # each table is a subclass from the Base class
        __table__ = pg_Base.metadata.tables['termen']

    xl_file = pd.read_excel("zoektermen_opdracht_sem1_sv.xlsx")
    #Environment
    for row in range(4,9):
        pg_session.add(PG_sub(subdomein= xl_file.iloc[row][0], hoofddomein=xl_file.iloc[1][0]))
        zoektermen = set(xl_file.iloc[row][1].split(','))
        for term in zoektermen:
            pg_session.add(PG_term(zoekwoord= term.strip(), subdomein=xl_file.iloc[row][0]))
    #Social
    for row in range(4,8):
        pg_session.add(PG_sub(subdomein= xl_file.iloc[row][4], hoofddomein=xl_file.iloc[1][4]))
        zoektermen = set(xl_file.iloc[row][5].split(','))
        for term in zoektermen:
            pg_session.add(PG_term(zoekwoord= term.strip(), subdomein=xl_file.iloc[row][4]))
    #Governance
    for row in range(4,6):
        pg_session.add(PG_sub(subdomein= xl_file.iloc[row][8], hoofddomein=xl_file.iloc[1][8]))
        zoektermen = set(xl_file.iloc[row][9].split(','))
        for term in zoektermen:
            pg_session.add(PG_term(zoekwoord= term.strip(), subdomein=xl_file.iloc[row][8]))
    
    pg_session.commit()

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




try:

    # jaarverslag_opvullen()

    # kmo_opvullen()

    jaarverslag_tekst_toevoegen()  

    # domeinen_toevoegen()

    # machinelearningdata_opvullen()


finally:    
    pg_session.close()
    pg_conn.close()
    print("Connections closed")
