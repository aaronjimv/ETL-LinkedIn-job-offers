from prefect import flow, task
from tasks.task_extract import task_extract
from tasks.task_load import task_load, task_engine
import asyncio

@flow(name='ETL linkedin job scraper')
async def main_flow():
    search = ['python']
    for s in search:
        offers = await task_extract(s)
        
        if offers is not False:
            engine = task_engine()
            await task_load(offers, engine)
        
        else:
            print("The workflow has stopped, please try again.")
    
    
if __name__ == '__main__':
    asyncio.run(main_flow())
