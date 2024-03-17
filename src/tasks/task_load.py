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
   job = Column(String)
   locate = Column(String)
   url = Column(String)
   date = Column(Date)


@task(name="LOAD THE DATA TO THE DB")
async def task_load(offers):
   print("Loading...")

   # Database connection
   engine = create_engine('sqlite:///offers.db', echo=True)
   Base.metadata.create_all(bind=engine)
   Session = sessionmaker(bind=engine)
   session = Session()

   # browse all offers
   for offer in offers:
      new_offers = Offer(
         job = offer['job'],
         locate = offer['locate'],
         url = offer['url'],
         date = datetime.strptime(offer['date'],'%Y-%m-%d').date()
      )
      session.add(new_offers)

   session.commit()
   session.close()
