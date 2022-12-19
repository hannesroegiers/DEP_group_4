from sqlalchemy import create_engine, MetaData, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
pg_conn = pg_engine.connect()
pg_Base = declarative_base(pg_engine) # initialize Base class
pg_Base.metadata.reflect(pg_engine)   # get metadata from database
Session = sessionmaker(bind=pg_engine)
pg_session = Session()
pg_session_push = Session()


class Score(pg_Base):
    __table__ = pg_Base.metadata.tables['score']


class Subdomein(pg_Base):
    __table__ = pg_Base.metadata.tables['subdomein']


class Kmo(pg_Base):
    __table__ = pg_Base.metadata.tables['kmo']


subdomeinen = pg_session.query(Subdomein.subdomein).distinct()
kmos = pg_session.query(Kmo.ondernemingsnummer).distinct()

for ondernemingsnummer in kmos:
    for subdomein in subdomeinen:
        try:
            print(ondernemingsnummer, 0, subdomein)
            pg_session_push.add(Score(ondernemingsnummer=ondernemingsnummer,
                                 score=0,
                                 subdomein=subdomein,))
            pg_session.commit()
        except:
            pass

pg_session.close()




