import numpy as np
import pandas as pd
import tensorflow
from tensorflow import keras
import matplotlib.pyplot as plt

from sqlalchemy import select, update, text, join
from sqlalchemy import create_engine, func, Table, MetaData, desc
from sqlalchemy.sql import column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

def start_postgres():
    # initialization of PostgreSQL stuff
    global pg_engine
    pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
    global pg_conn
    pg_conn = pg_engine.connect()
    metadata = MetaData(pg_engine)  

    global pg_Base
    pg_Base = declarative_base(pg_engine) # initialize Base class
    pg_Base.metadata.reflect(pg_engine)   # get metadata from database

    Session = sessionmaker(bind=pg_engine)
    global pg_session
    pg_session = Session()

    # class PG_SME(pg_Base):  # each table is a subclass from the Base class
    #     __table__ = pg_Base.metadata.tables['jaarverslag']

try:
    start_postgres()

    # STAP 1: data inladen
    class MachinelearningData(pg_Base):
            __table__ = pg_Base.metadata.tables['machinelearningData']
    class Score(pg_Base):
            __table__ = pg_Base.metadata.tables['score']
    
    
    ml_data = pd.read_sql(select(MachinelearningData).where(MachinelearningData.jaar == 2021),pg_conn)
    score_data = pd.read_sql(select(Score),pg_conn)
    
    # STAP 2: data visualiseren, statistieken bekijken, inzichten ophalen, zoeken naar correlaties

    # bereken een algemene score per ondernemingnummer als label
    average = score_data.groupby(['ondernemingsnummer'])['score'].mean()
    
    # Door een inner join te doen verwijder je de kmo's zonder score
    df = ml_data.join(average, how='inner', on="ondernemingsnummer")
    # STAP 3: data voorbereiden pipeline, redundante data verwijderen, test set opstellen
    #   labels en data
    #   pipeline (missing values, scaling, onehotencoders, etc...)
    #verwijder onnodige kolommen en die waar de omzet 0 is
    df = df.dropna()
    df = df.drop(['ondernemingsnummer','jaar'], axis=1)
    df = df[df['omzet'] != 0]
    print(df.describe())

    # maken X en y
    X = df.drop(['score'], axis=1)
    y = df['score']

    #jaartallen groeper per 10 jaar
    # jaar gaat beter functioneren als een classificatie dan als een getal
    X['jaarGroep'] = df['oprichtingsjaar'].apply(lambda x: (x//5)/2)

    print(X.head(5))
    print(y.head(5))
    print(X['jaarGroep'].value_counts())

    plt.plot(X['jaarGroep'].value_counts())

    # STAP 4: selecteer, train en evalueer (grid search cross-validation (CV) of randomnized search CV) het model


finally:
    print("Connections closed")
    pg_session.close()
    pg_conn.close()
