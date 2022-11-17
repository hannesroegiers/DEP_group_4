import os

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
def index():
    jaarverslagen_url = "https://www.staatsbladmonitor.be/bedrijfsfiche.html?ondernemingsnummer=0"
    conn = get_deb_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM kmo')
    kmos = cur.fetchall()

    conn.close()
    cur.close()
    
    return render_template('home.html',title="KMO's",bedrijven=kmos,jaarverslagen_url=jaarverslagen_url)

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
    return render_template('bedrijf.html',bedrijf=bedrijf,title="Domeinen",subdomeinen=subdomein_dict)

if __name__ == '__main__':
    app.run(debug=True)