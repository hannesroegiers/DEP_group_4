import pandas as pd

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

class PG_Employee(pg_Base):  # each table is a subclass from the Base class
    __table__ = pg_Base.metadata.tables['kmo']
    
Session = sessionmaker(bind=pg_engine)
pg_session = Session()

pd_xl_file = pd.read_excel("kmo's_Vlaanderen_2021.xlsx", sheet_name= "Lijst")

pd_pdf = 

for el in pd_xl_file.values:
    
    print(el[1])
    break


