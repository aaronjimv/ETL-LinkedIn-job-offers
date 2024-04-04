from prefect import task
from datetime import datetime
import csv

@task(name="LOG EXECUTION")
async def task_logs(num_offers_extracted, timestamp, error):
   """
   Receive: 
      the number of offers.
      the timestamp.
      the error if any occurred.

   It organizes the information in a dictionary 
   that is then stored in a csv file.
   """
   log_entry = {
      'date': timestamp.strftime("%Y-%m-%d"),
      'hour': timestamp.strftime("%H:%M:%S"),
      'offers_extracted': num_offers_extracted,
      'error': error
   }

   with open('data/execution_logs.csv', mode='a', newline='') as file:
      writer = csv.DictWriter(file, fieldnames=[
         'date',
         'hour',
         'offers_extracted',
         'error'
      ])

      if file.tell() == 0:
         writer.writeheader()
      
      writer.writerow(log_entry)
