import matplotlib.pyplot as plt
import numpy as np
from flask import Flask, render_template,json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import (ForeignKey, MetaData, Table, create_engine, desc, func,
                        select, update)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,aliased
from decimal import Decimal



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

#flask routing
@app.route('/')
@app.route('/overzicht')
def index():
    start_postgres()
    class Kmo(pg_Base):
        __table__ = pg_Base.metadata.tables['kmo']
    class Website(pg_Base):
        __table__ = pg_Base.metadata.tables['website']
    class Score(pg_Base):
        __table__ = pg_Base.metadata.tables['score']

    jaarverslagen_url = "https://www.staatsbladmonitor.be/bedrijfsfiche.html?ondernemingsnummer=0"

    kmos = pg_session.query(Kmo.ondernemingsnummer,Kmo.bedrijfsnaam,Website.url,Kmo.sector,Kmo.gemeente,func.avg(Score.score).label("duurzaamheid")) \
                        .join(Website) \
                        .join(Score, Score.ondernemingsnummer == Website.ondernemingsnummer)    \
                        .where(Website.url != 'geen') \
                        .group_by(Kmo.ondernemingsnummer, Website.url)
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
    class Score(pg_Base):
        __table__ = pg_Base.metadata.tables['score']



    environment_subdomeinen=pg_session.query(Subdomein) \
                                        .where(Subdomein.hoofddomein =='Environment')
    social_subdomeinen=pg_session.query(Subdomein) \
                                        .where(Subdomein.hoofddomein =='Social')
    governance_subdomeinen=pg_session.query(Subdomein) \
                                        .where(Subdomein.hoofddomein =='Governance')
    avg_hoofddomein_scores=pg_session.query(func.avg(Score.score).label('score'),Subdomein.hoofddomein) \
                                        .join(Subdomein) \
                                        .where(Score.ondernemingsnummer==ondernemingsnummer) \
                                        .group_by(Subdomein.hoofddomein)
    subdomein_scores=pg_session.query(Score.score, Score.subdomein, Subdomein.hoofddomein) \
                                        .join(Subdomein) \
                                        .where(Score.ondernemingsnummer==ondernemingsnummer,
                                               Score.subdomein == Subdomein.subdomein)\
                                        .order_by(Score.subdomein)
    subdomein_unique=pg_session.query(Subdomein.subdomein, Subdomein.hoofddomein).distinct()
    hoofddomein_unique=pg_session.query(Subdomein.hoofddomein).distinct()
    alle_subdomeinen = pg_session.query(Score.subdomein).where(Score.ondernemingsnummer==ondernemingsnummer)
    subdomein_list = [value for (value,) in alle_subdomeinen]

    sector_avg_subquery = pg_session.query(Kmo.sector).where(Kmo.ondernemingsnummer==ondernemingsnummer)
    sector_avg = pg_session.query(func.avg(Score.score).label("average"),Score.subdomein,Kmo.sector)\
                                        .join(Kmo,Kmo.ondernemingsnummer==Score.ondernemingsnummer) \
                                        .where(Kmo.sector==sector_avg_subquery) \
                                        .group_by(Score.subdomein,Kmo.sector) \
                                        .order_by(Score.subdomein)

    sector_avg_dict = {}
    for entry in sector_avg:
        sector_avg_dict[entry.subdomein] = entry.average
            
    subdomein_score_dict = {}
    for entry in subdomein_scores:
        subdomein_score_dict[entry.subdomein] = entry.score

    nieuw_score_dict = {}
    for avg in sector_avg_dict:
        nieuw_score_dict[avg] = str(float(subdomein_score_dict[avg]) - float(sector_avg_dict[avg]))
    
   
    domeinen_dict = {}
    for hoofddomein in hoofddomein_unique:
        domeinlijst = []
        for subdomein in subdomein_unique:
            if hoofddomein.hoofddomein == subdomein.hoofddomein:
                domeinlijst.append(subdomein.subdomein)
        domeinen_dict[hoofddomein.hoofddomein] = domeinlijst

    #bepalen percentielen
    min_max_subquery = pg_session.query(func.avg(Score.score).label('duurzaamheidsscore')).group_by(Score.ondernemingsnummer).subquery('sub')
    alias = aliased(min_max_subquery,name="sub")

    min_max_query = pg_session.query(func.min(alias.columns.duurzaamheidsscore),func.max(alias.columns.duurzaamheidsscore)).select_from(alias).one()
    min_score,max_score = float(min_max_query[0]),float(min_max_query[1])
    diff_score = max_score-min_score
    aantal_kolommen_ESG_distrib = 10.0
    iters = diff_score/aantal_kolommen_ESG_distrib


    #general info about sme
    info = pg_session.query(Kmo.bedrijfsnaam,Kmo.adres,Website.url,Kmo.ondernemingsnummer,Jaarverslag.personeelsbestand,Jaarverslag.omzet,Kmo.sector,Kmo.gemeente,func.avg(Score.score).label("duurzaamheid")) \
                    .join(Website) \
                    .join(Jaarverslag) \
                    .join(Score,Score.ondernemingsnummer == Website.ondernemingsnummer)\
                    .where(Jaarverslag.ondernemingsnummer == ondernemingsnummer)\
                    .group_by(Kmo.ondernemingsnummer, Website.url,Jaarverslag.personeelsbestand,Jaarverslag.omzet)

    # print(f"min {type(min_score)}, max {type(max_score)}, diff: {type(iters)}\n\n")
    perc_arr2 = []
    for i in np.arange(min_score,max_score,diff_score/aantal_kolommen_ESG_distrib):
        perc_arr2.append(i)



    perc_dict = {}
    perc_dict["this"] = int((info[0].duurzaamheid/Decimal(diff_score/100))%10)
    for count,perc in enumerate(perc_arr2):
        perc_subquery = pg_session.query(func.avg(Score.score)).group_by(Score.ondernemingsnummer).having(func.avg(Score.score)<= perc).having(func.avg(Score.score) > perc-iters).subquery('sub')
        perc_query = pg_session.query(func.count()).select_from(perc_subquery).scalar()
        perc_dict[f"{count*int(aantal_kolommen_ESG_distrib)}% - {int((count+1)*aantal_kolommen_ESG_distrib)}%"] = perc_query
    # print(f"perc_query: {perc_query}\nperc: {perc}, perc-diff: {perc-iters}, iters: {iters}")
    print(perc_dict)
    
    subdomein_dict = {
        "env": environment_subdomeinen,
        "soc" : social_subdomeinen,
        "gov" : governance_subdomeinen
    }

    avg_hoofddomein_scores_dict={
        "env" : float(avg_hoofddomein_scores[0].score),
        "soc" : float(avg_hoofddomein_scores[1].score),
        "gov" : float(avg_hoofddomein_scores[2].score)
    }
    hoofddomein_set = {"env": "Environment", "soc": "Social", "gov": "Governance"}

    return render_template('bedrijf.html',title="SUOR - Domeinen"
                           ,subdomeinen=subdomein_dict,
                           info=info[0],
                           hoofddomeinscores=avg_hoofddomein_scores_dict,
                           subdomeinscores=subdomein_scores,
                           hoofddomeinen=hoofddomein_set,
                           domeinmapper=domeinen_dict,
                           score_subdomeinmapper=subdomein_score_dict,
                           subdomein_list = json.dumps(subdomein_list),
                           nieuwe_score_dict = nieuw_score_dict,
                           perc_dict = perc_dict)


if __name__ == '__main__':
    app.run(debug=True)