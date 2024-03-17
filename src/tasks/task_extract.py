from prefect import task
import requests
from bs4 import BeautifulSoup
from config import Config
import asyncio

config = Config()

@task(name="EXTRACT LINKEDIN DATA")
async def task_extract(skill):
   url = requests.get(config.extract_url.replace('#',skill))
   
   if(url.status_code == 200):
      print(f"Runing extract data for - {skill} - jobs...")
      html = BeautifulSoup(url.text,'html.parser')
      dataOfertas = html.find('ul',{'class':'jobs-search__results-list'})
      listaOfertas = dataOfertas.find_all('li')
      resultadoOfertas = []
      for oferta in listaOfertas:
         titulo = oferta.find('h3',{'class':'base-search-card__title'})
         empresa = oferta.find('span',{'class':'job-search-card__location'})
         urlOferta = oferta.find('a',{'class':'base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]'})
         urlOferta = urlOferta['href'].strip().split('?')[0]
         oferta_id = urlOferta.split('-')[-1]
         fecha = oferta.find('time')
         dicOferta = {
               'id':oferta_id,
               'puesto':titulo.get_text().strip(),
               'ubicacion':empresa.get_text().strip(),
               'url': urlOferta,
               'fecha':fecha['datetime'].strip()
         }
         resultadoOfertas.append(dicOferta)
      return resultadoOfertas
   
   else:
      print("error " + str(url.status_code))