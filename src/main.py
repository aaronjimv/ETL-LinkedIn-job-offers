from prefect import flow, task
from tasks.task_extract import task_extract
from tasks.task_load import task_load, task_engine
from tasks.task_logs import task_logs
from datetime import datetime
import asyncio

@flow(name='ETL linkedin job scraper')
async def main_flow():
    """
    Project main flow.

    Put the different functionalities together
    in a `while` loop to handle the `429 error` 
    and not interrupt the process.

    The ideal case is that `offers` is not of `type int`. 
    Which means that the web srcaping process was carried 
    out correctly and returned a list of offers.
    """
    search = ['python']
    for s in search:

        while True:
            offers = await task_extract(s)
            
            if (offers == 429):
                print(f"Connection error: {offers}. Trying again...")

                num_offers_extracted = 0
                timestamp = datetime.now()
                error = offers
                await task_logs(num_offers_extracted, timestamp, error)

            elif offers is not type(int):
                engine = task_engine()
                await task_load(offers, engine)
                
                num_offers_extracted = len(offers)
                timestamp = datetime.now()
                error = "ok"
                await task_logs(num_offers_extracted, timestamp, error)

                break
            
            else:
                print("The workflow has stopped.")
                
                num_offers_extracted = 0
                timestamp = datetime.now()
                error = offers
                await task_logs(num_offers_extracted, timestamp, error)
                
                break


if __name__ == '__main__':
    asyncio.run(main_flow())
