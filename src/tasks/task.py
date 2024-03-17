from prefect import task

@task(name="Name of task")
def task():
   print("This is my task...")
