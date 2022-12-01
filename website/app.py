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
def get_deb_connection():
    conn = psycopg2.connect(host='vichogent.be',
                            database='durabilitysme',
                            user='postgres',
                            password='loldab123',
                            port='40031')
    return conn


#setting up flask app
app = Flask(__name__)

db = SQLAlchemy(app)



#flask routing
@app.route('/')
@app.route('/overzicht')
def index():
    jaarverslagen_url = "https://www.staatsbladmonitor.be/bedrijfsfiche.html?ondernemingsnummer=0"
    conn = get_deb_connection()
    cur = conn.cursor()
    # join_query = 'SELECT kmo.ondernemingsnummer,kmo.bedrijfsnaam,kmo.website,s.score,kmo.sector,kmo.gemeente FROM kmo LEFT JOIN score s ON s.ondernemingsnummer = kmo.ondernemingsnummer'
    # cur.execute('SELECT * FROM kmo')   
    cur.execute('select kmo.ondernemingsnummer, bedrijfsnaam, w.url,sector,gemeente from kmo JOIN website w on w.ondernemingsnummer = kmo.ondernemingsnummer')   
    kmos = cur.fetchall()
    print(kmos[0])

    conn.close()
    cur.close()
    
    return render_template('home.html',title="SUOR - KMO's",bedrijven=kmos,jaarverslagen_url=jaarverslagen_url)

@app.route('/result/<ondernemingsnummer>',methods=['GET','POST'])
def bedrijf(ondernemingsnummer):

    conn=get_deb_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM kmo WHERE ondernemingsnummer = {ondernemingsnummer}")
    bedrijf = cur.fetchone()
    cur.execute(f"SELECT * FROM subdomein where hoofddomein = 'Environment'")
    environment_subdomeinen=cur.fetchall()
    cur.execute(f"SELECT * FROM subdomein where hoofddomein = 'Social'")
    social_subdomeinen=cur.fetchall()
    cur.execute(f"SELECT * FROM subdomein where hoofddomein = 'Governance'")
    governance_subdomeinen=cur.fetchall()
    subdomein_dict = {
        "env": environment_subdomeinen,
        "soc" : social_subdomeinen,
        "gov" : governance_subdomeinen
    }
    cur.execute(f"select bedrijfsnaam,adres, w.url,kmo.ondernemingsnummer,j.personeelsbestand,j.omzet, kmo.sector from kmo JOIN website w on w.ondernemingsnummer = kmo.ondernemingsnummer JOIN jaarverslag j on j.ondernemingsnummer = w.ondernemingsnummer WHERE j.ondernemingsnummer = {ondernemingsnummer}")
    info = cur.fetchall()[0]

    return render_template('bedrijf.html',bedrijf=bedrijf,title="SUOR - Domeinen",subdomeinen=subdomein_dict,info=info)


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