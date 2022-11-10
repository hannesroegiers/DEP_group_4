import pandas as pd

import sqlalchemy
from sqlalchemy import create_engine, func, Table, MetaData, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

# initialization of PostgreSQL stuff
pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
pg_conn = pg_engine.connect()
metadata = MetaData(pg_engine)  

pg_Base = declarative_base(pg_engine) # initialize Base class
pg_Base.metadata.reflect(pg_engine)   # get metadata from database

class PG_SME(pg_Base):  # each table is a subclass from the Base class
    __table__ = pg_Base.metadata.tables['jaarverslag']
    
Session = sessionmaker(bind=pg_engine)
pg_session = Session()

xl_file = pd.read_excel("websites/kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

# try:
#     with pdfplumber.open(f"../PDF/jaarraport_{ondernemingsnummer}.*") as pdf:
#         re.

# except:

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


for rij in xl_file.values:
    
    print(rij[1])

    # table jaarverslag vullen vanuit de excel
    omzet = rij[5] if rij[5] != 'n.b.' else 0
    pg_jaarverslag = PG_SME(ondernemingsnummer=int(rij[7].replace(' ','')), jaar=2021, personeelsbestand= rij[4], omzet= omzet, totaal_activa= rij[6], tekst= "")
    pg_session.add(pg_jaarverslag)

    # table 'kmo' vullen vanuit de excel
    # sector = bepaalSector(rij[3])
    # pg_kmo = PG_SME(ondernemingsnummer=int(rij[7].replace(' ','')), bedrijfsnaam= rij[1], adres=  rij[9], website=  rij[11], sector= sector, gemeente=  rij[2])
    # pg_session.add(pg_kmo)
    

pg_session.commit()
pg_session.close()
pg_conn.close()


