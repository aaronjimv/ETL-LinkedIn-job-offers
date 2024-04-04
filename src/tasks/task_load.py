from prefect import task
import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import asyncio

Base = declarative_base()

class Offer(Base):
   __tablename__ = 'offers'
   id = Column(Integer, primary_key=True)
   company = Column(String)
   job = Column(String)
   locate = Column(String)
   url = Column(String)
   date = Column(Date)


@task(name="CREATE DB ENGINE")
def task_engine():
   """
   Creates and return the database engine.
   """
   print("Creating DB engine...")

   engine = create_engine('sqlite:///data/offers.db', echo=True) # test DB
   Base.metadata.create_all(bind=engine)
   return engine


@task(name="LOAD THE DATA TO THE DB")
async def task_load(offers, engine):
   """
   Receives the list of offers along with the DB engine. 
   
   Load the offers in the DB and close the session.
   """
   print("Loading data...")

   # Database connection
   Session = sessionmaker(bind=engine)
   session = Session()

   # browse all offers
   for offer in offers:
      new_offers = Offer(
         company = offer['company'],
         job = offer['job'],
         locate = offer['location'],
         url = offer['url'],
         date = datetime.strptime(offer['date'],'%Y-%m-%d').date()
      )
      session.add(new_offers)

   session.commit()
   session.close()
