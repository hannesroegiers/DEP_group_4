import os

import matplotlib.pyplot as plt
import pandas as pd
import pdfplumber
import psycopg2
import sshtunnel
from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import (ForeignKey, MetaData, Table, create_engine, desc, func,
                        select, update)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder

# initialization database connection
# def get_deb_connection():
#     conn = psycopg2.connect(host='vichogent.be',
#                             database='durabilitysme',
#                             user='postgres',
#                             password='loldab123',
#                             port='40031')
#     return conn

# def get_deb_connection():


#     return Session()

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



#setting up flask app
app = Flask(__name__)

db = SQLAlchemy(app)



#flask routing
@app.route('/')
@app.route('/overzicht')
def index():
    start_postgres()
    class Kmo(pg_Base):
        __table__ = pg_Base.metadata.tables['kmo']
    class Website(pg_Base):
        __table__ = pg_Base.metadata.tables['website']

    jaarverslagen_url = "https://www.staatsbladmonitor.be/bedrijfsfiche.html?ondernemingsnummer=0"

    kmos = pg_session.query(Kmo.ondernemingsnummer,Kmo.bedrijfsnaam,Website.url,Kmo.sector,Kmo.gemeente) \
                        .join(Website) \
                        .where(Website.url != 'geen')
    print(kmos[0])
    
    return render_template('home.html',title="SUOR - KMO's",bedrijven=kmos,jaarverslagen_url=jaarverslagen_url)

@app.route('/result/<ondernemingsnummer>',methods=['GET','POST'])
def bedrijf(ondernemingsnummer):
    start_postgres()
    class Kmo(pg_Base):
        __table__ = pg_Base.metadata.tables['kmo']
    class Website(pg_Base):
        __table__ = pg_Base.metadata.tables['website']
    class Subdomein(pg_Base):
        __table__ = pg_Base.metadata.tables['subdomein']
    class Jaarverslag(pg_Base):
        __table__ = pg_Base.metadata.tables['jaarverslag']

    environment_subdomeinen=pg_session.query(Subdomein) \
                                        .where(Subdomein.hoofddomein =='Environment')
    social_subdomeinen=pg_session.query(Subdomein) \
                                        .where(Subdomein.hoofddomein =='Social')
    governance_subdomeinen=pg_session.query(Subdomein) \
                                        .where(Subdomein.hoofddomein =='Governance')
    subdomein_dict = {
        "env": environment_subdomeinen,
        "soc" : social_subdomeinen,
        "gov" : governance_subdomeinen
    }
    
    info = pg_session.query(Kmo.bedrijfsnaam,Kmo.adres,Website.url,Kmo.ondernemingsnummer,Jaarverslag.personeelsbestand,Jaarverslag.omzet,Kmo.sector,Kmo.gemeente) \
                        .join(Website) \
                        .join(Jaarverslag) \
                        .where(Jaarverslag.ondernemingsnummer == ondernemingsnummer)

    for row in info:
        print(row)
        print(row.bedrijfsnaam)
    scores= ""
    return render_template('bedrijf.html',title="SUOR - Domeinen",subdomeinen=subdomein_dict,info=info[0])


@app.route('/sectoren')
def sectoren():
    # importing the required module

    # x axis values
    x = [1,2,3]
    # corresponding y axis values
    y = [2,4,1]

    # plotting the points
    plt.plot(x, y)

    # naming the x axis
    plt.xlabel('x - axis')
    # naming the y axis
    plt.ylabel('y - axis')

    # giving a title to my graph
    plt.title('My first graph!')


    return render_template('sectoren.html',title="SUOR - Sectoren",graph=plt)

if __name__ == '__main__':
    app.run(debug=True)