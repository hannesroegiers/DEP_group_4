import numpy as np
import pandas as pd
import tensorflow
from tensorflow import keras
import matplotlib.pyplot as plt

import joblib

from sqlalchemy import select, update, text, join
from sqlalchemy import create_engine, func, Table, MetaData, desc
from sqlalchemy.sql import column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from sklearn import metrics

def start_postgres():
    global pg_engine
    pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
    global pg_conn
    pg_conn = pg_engine.connect()
    metadata = MetaData(pg_engine)  

    global pg_Base
    pg_Base = declarative_base(pg_engine)
    pg_Base.metadata.reflect(pg_engine)


def train_model_Omzet():
    # Dit zijn de tabellen die we gebruiken
    class MachinelearningData(pg_Base):
        __table__ = pg_Base.metadata.tables['machinelearningData']
    class Score(pg_Base):
        __table__ = pg_Base.metadata.tables['score']
    class Kmo(pg_Base):
        __table__ = pg_Base.metadata.tables['kmo']
    class Gemeente(pg_Base):
        __table__ = pg_Base.metadata.tables['gemeente']

    ml_data = pd.read_sql(select(MachinelearningData, Gemeente.verstedelijkingsgraad)
        .join(Kmo, MachinelearningData.ondernemingsnummer == Kmo.ondernemingsnummer)
        .join(Gemeente, Kmo.gemeente == Gemeente.deelgemeente)
        .where(MachinelearningData.jaar == 2021),
        pg_conn)
    score_data = pd.read_sql(select(Score),pg_conn)

    # Bereken een algemene score per ondernemingnummer als label
    average2 = score_data.groupby(['ondernemingsnummer'])['score'].sum().apply(lambda x: x/11)

    # Door een inner join te doen verwijder je de kmo's zonder score
    df = ml_data.join(average2, how='inner', on="ondernemingsnummer")

    # Verwijder onnodige kolommen
    df = df.drop(['ondernemingsnummer','jaar'], axis=1)
    # df = df[df['omzet'] != 0]


    # jaartallen groeperen per 5 jaar
    # jaar gaat beter functioneren als een classificatie dan als een getal
    df['jaarGroep'] = df['oprichtingsjaar'].apply(lambda x: str((x//5)*5))
    df = df.drop('oprichtingsjaar', axis=1)
    

    # X en y definiëren
    X = df.drop(['score'], axis=1)
    y = df['score']

    # vanwege een probleem met de headers moet je alle data uit het datadrame halen
    # en er opnieuw terug insteken
    array = np.asarray(X)
    X = pd.DataFrame(array, 
        columns=['omzet', 'personeelsledenAantal', 'verstedelijkingsgraad', 'totaalActiva','winst', 'jaarGroep'])
    X['omzet'] = X['omzet'].astype(np.int64)
    X['personeelsledenAantal'] = X['personeelsledenAantal'].astype(np.uint64)
    X['verstedelijkingsgraad'] = X['verstedelijkingsgraad'].astype(np.float64)
    X['winst'] = X['winst'].astype(np.uint64)
    X['totaalActiva'] = X['totaalActiva'].astype(np.uint64)
    X['jaarGroep'] = X['jaarGroep'].astype(str)

    # prepareer de categorische datatypes
    X = pd.get_dummies(X, prefix='dummy')

    # splits train en test datasets
    X, X_test, y, y_test = train_test_split(X, y, test_size=0.15, random_state=42)

    model = RandomForestRegressor(max_depth= 8, n_estimators= 200)
    model.fit(X,y)

    # slaag het model op
    joblib.dump(model, './random_forest_z0.joblib')

    # print some statistics
    y_pred = model.predict(X_test) # The predictions from your ML / RF model
    print('Model met omzet')
    print('Root Mean Squared Error (RMSE):', metrics.mean_squared_error(y_test, y_pred, squared=False))
    print('R^2:', metrics.r2_score(y_test, y_pred))
    print(f'''Feature importance:
        {pd.DataFrame(model.feature_importances_[:5].reshape(1,-1), columns=X.columns.values[:5])}''')

def train_model_zonder_Omzet():
    # Dit zijn de tabellen die we gebruiken
    class MachinelearningData(pg_Base):
        __table__ = pg_Base.metadata.tables['machinelearningData']
    class Score(pg_Base):
        __table__ = pg_Base.metadata.tables['score']
    class Kmo(pg_Base):
        __table__ = pg_Base.metadata.tables['kmo']
    class Gemeente(pg_Base):
        __table__ = pg_Base.metadata.tables['gemeente']

    ml_data = pd.read_sql(select(MachinelearningData, Gemeente.verstedelijkingsgraad)
        .join(Kmo, MachinelearningData.ondernemingsnummer == Kmo.ondernemingsnummer)
        .join(Gemeente, Kmo.gemeente == Gemeente.deelgemeente)
        .where(MachinelearningData.jaar == 2021),
        pg_conn)
    score_data = pd.read_sql(select(Score),pg_conn)

    # Bereken een algemene score per ondernemingnummer als label
    average2 = score_data.groupby(['ondernemingsnummer'])['score'].sum().apply(lambda x: x/11)

    # Door een inner join te doen verwijder je de kmo's zonder score
    df = ml_data.join(average2, how='inner', on="ondernemingsnummer")

    # Verwijder onnodige kolommen
    df = df.drop(['ondernemingsnummer','jaar','omzet'], axis=1)


    # jaartallen groeperen per 5 jaar
    # jaar gaat beter functioneren als een classificatie dan als een getal
    df['jaarGroep'] = df['oprichtingsjaar'].apply(lambda x: str((x//5)*5))
    df = df.drop('oprichtingsjaar', axis=1)
    

    # X en y definiëren
    X = df.drop(['score'], axis=1)
    y = df['score']

    # vanwege een probleem met de headers moet je alle data uit het datadrame halen
    # en er opnieuw terug insteken
    array = np.asarray(X)
    X = pd.DataFrame(array, 
        columns=['personeelsledenAantal', 'verstedelijkingsgraad', 'totaalActiva','winst', 'jaarGroep'])
    X['personeelsledenAantal'] = X['personeelsledenAantal'].astype(np.uint64)
    X['verstedelijkingsgraad'] = X['verstedelijkingsgraad'].astype(np.float64)
    X['winst'] = X['winst'].astype(np.uint64)
    X['totaalActiva'] = X['totaalActiva'].astype(np.uint64)
    X['jaarGroep'] = X['jaarGroep'].astype(str)

    # prepareer de categorische datatypes
    X = pd.get_dummies(X, prefix='dummy')

    # splits train en test datasets
    X, X_test, y, y_test = train_test_split(X, y, test_size=0.15, random_state=42)

    model = RandomForestRegressor(max_depth= 8, n_estimators= 200)
    model.fit(X,y)

    # slaag het model op
    joblib.dump(model, './random_forest_m0.joblib')

    # print some statistics
    y_pred = model.predict(X_test) # The predictions from your ML / RF model
    print('Model zonder omzet')
    print('Root Mean Squared Error (RMSE):', metrics.mean_squared_error(y_test, y_pred, squared=False))
    print('R^2:', metrics.r2_score(y_test, y_pred))
    print('Accuracy:', model.score(X_test,y_test))
    print(f'''Feature importance:
        {pd.DataFrame(model.feature_importances_[:5].reshape(1,-1), columns=X.columns.values[:5])}''')


def predict_z0(array):
    print('begin predict')

    rnd_reg = joblib.load('./random_forest_z0.joblib')
    predict = rnd_reg.predict(array)

    print('klaar predict')
    return predict

def predict_m0(array):
    print('begin predict')

    rnd_reg = joblib.load('./random_forest_m0.joblib')
    predict = rnd_reg.predict(array)

    print('klaar predict')
    return predict

try:
    start_postgres()

    train_model_Omzet()
    # train_model_zonder_Omzet()

finally:
    pg_conn.close()
