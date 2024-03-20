from prefect import flow, task
from tasks.task_extract import task_extract
from tasks.task_load import task_load, task_engine
import asyncio

@flow(name='ETL linkedin job scraper')
async def main_flow():
    search = ['python']
    for s in search:
        offers = await task_extract(s)
        engine = task_engine()
        await task_load(offers, engine)
    
    
if __name__ == '__main__':
    asyncio.run(main_flow())
