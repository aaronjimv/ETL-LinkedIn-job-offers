from prefect import task
import requests
from bs4 import BeautifulSoup
from config import Config
import asyncio

config = Config()

@task(name="EXTRACT LINKEDIN DATA")
async def task_extract(skill):
   """
   It is responsible for carrying out the web scraping 
   process. 

   Receives a skill to search for and returns a list of 
   offers or an error if it occurs.
   """
   url = requests.get(config.extract_url.replace('#',skill))
   
   if(url.status_code == 200):
      print(f"Running data extraction for - {skill} - jobs...")
      html = BeautifulSoup(url.text,'html.parser')
      offertsData = html.find('ul',{'class':'jobs-search__results-list'})
      offersList = offertsData.find_all('li')
      offersResult = []

      for offer in offersList:
         title = offer.find('h3',{'class':'base-search-card__title'})
         company = offer.find('a',{'class':'hidden-nested-link'})
         location = offer.find('span',{'class':'job-search-card__location'})
         url_offer = offer.find('a',{'class':'base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]'})
         url_offer = url_offer['href'].strip().split('?')[0]
         id_offer = url_offer.split('-')[-1]
         date = offer.find('time')
         dictionary_offer = {
               'id':id_offer,
               'company':company.get_text().strip(),
               'job':title.get_text().strip(),
               'location':location.get_text().strip(),
               'url': url_offer,
               'date':date['datetime'].strip()
         }
         offersResult.append(dictionary_offer)
      return offersResult
   
   else:
      return url.status_code
