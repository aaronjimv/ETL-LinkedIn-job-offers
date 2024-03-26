from prefect import flow, task
from tasks.task_extract import task_extract
from tasks.task_load import task_load, task_engine
import asyncio

@flow(name='ETL linkedin job scraper')
async def main_flow():
    search = ['python']
    for s in search:

        while True:
            offers = await task_extract(s)
            
            if (offers == 429):
                print(f"Connection error: {offers}. Trying again...")

            elif offers is not type(int):
                engine = task_engine()
                await task_load(offers, engine)
                break
            
            elif (offers == 11001):
                print("The workflow has stopped.")
                break


if __name__ == '__main__':
    asyncio.run(main_flow())
