import queue
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine, MetaData, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
import time

def scrape_duurzame_websitetekst():
    pg_engine = create_engine('postgresql://postgres:loldab123@vichogent.be:40031/durabilitysme')
    pg_conn = pg_engine.connect()

    pg_Base = declarative_base(pg_engine) # initialize Base class
    pg_Base.metadata.reflect(pg_engine)   # get metadata from database

    Session = sessionmaker(bind=pg_engine)
    pg_session = Session()

    options = webdriver.FirefoxOptions()
    options.headless = True
    ff_driver = webdriver.Firefox(options=options)

    class Website(pg_Base):
        __table__ = pg_Base.metadata.tables['website']

    #TODO: querry aanpassen naar ondernemignsnummers die je moet overlopen!
    db_entries = pg_session.query(Website.url, Website.jaar, Website.ondernemingsnummer)\
        .where(Website.url != "geen", 425000000 < Website.ondernemingsnummer, Website.ondernemingsnummer <= 450000000)
    try:
        for entry in db_entries:
            duurzaamheid_tekst = ""
            basis_url = '/'.join(entry.url.split("/")[0:3])
            duurzaamheid_urls = find_urls_duurzaamheid(basis_url)
            for url in duurzaamheid_urls:
                ff_driver.get(url)
                html = ff_driver.page_source
                soup = BeautifulSoup(html, "lxml")
                b_lijst = soup.find_all('b')
                website_tekst = "\n".join([x.get_text().strip() for x in b_lijst])
                duurzaamheid_tekst += website_tekst
            pg_session.query(Website)\
                .filter(Website.ondernemingsnummer == entry.ondernemingsnummer)\
                .update({Website.websitetekst: duurzaamheid_tekst})
            pg_session.commit()
            print(duurzaamheid_tekst)
    finally:
        pg_conn.close()
        pg_session.close()
        if duurzaamheid_tekst == "":
            print("geen tekst gevonden...")
        print("             Volgende website!               ")

def find_urls_duurzaamheid(start_url):
    doorzochte_url_lijst = set({})
    te_doorzoeken_urls = queue.Queue()
    te_doorzoeken_urls.put(start_url)
    zoekwoorden = ["over", "rapport", "duurzaamheid", "rapportering", "duurzaam", "rapport"]
    verboden_tags = ["/fr", "/en", "/de"]
    options = webdriver.FirefoxOptions()
    options.headless = True
    ff_driver = webdriver.Firefox(options=options)
    timer = datetime.now().minute
    try:
        while not te_doorzoeken_urls.empty() and not (timer - datetime.now().minute) % 60 >= 2:
            url = te_doorzoeken_urls.get(False)
            print("url doorzoeken: " + url)
            while url in doorzochte_url_lijst or any(taal in url for taal in verboden_tags):
                url = te_doorzoeken_urls.get(False)
                print("duplicaat gevonden, volgende url: " + url)
                print("te doorzoeken urls over: " + str(te_doorzoeken_urls.qsize()))
            ff_driver.get(url)
            time.sleep(2)
            web_element_lijst = ff_driver.find_elements(By.XPATH, "//a")
            for web_element in web_element_lijst:
                sub_url = web_element.get_property("href")
                if sub_url is not None and url in sub_url and "#" not in sub_url and sub_url not in doorzochte_url_lijst:
                    te_doorzoeken_urls.put(sub_url)
                    print("  gevonden sub_url: " + sub_url)
            doorzochte_url_lijst.add(url)
    except NoSuchElementException:
        pass
    except queue.Empty:
        print("Queue leeg!")
    finally:
        ff_driver.close()
        out = [url for url in doorzochte_url_lijst if any(woord in url for woord in zoekwoorden)]
        print("gevonden duurzame urls: " + ', '.join(out))
        duurzame_urls_file = open("duurzame_urls", "a")
        duurzame_urls_file.write(f"{start_url};{','.join(out)}\n")
        duurzame_urls_file.close()
        return out


#find_urls_duurzaamheid("https://o-bio.be/")
scrape_duurzame_websitetekst()