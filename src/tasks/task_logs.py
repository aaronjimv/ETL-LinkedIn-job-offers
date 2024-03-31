from prefect import task
from datetime import datetime

@task(name="LOG EXECUTION")
async def task_logs(num_offers_extracted, timestamp, error):
   log_entry = {
      'date': timestamp.strftime("%Y-%m-%d"),
      'hour': timestamp.strftime("%H:%M"),
      'number_of_offers_extracted': num_offers_extracted,
      'error': error
   }

   print(f"Log entry: {log_entry}")
