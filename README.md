# ETL Linkedind job offers
---

This repository aim to improve my previous project [Linkedin Jobs Scraper](https://github.com/aaronjimv/Linkedin-Jobs-Scraper), which was based on a simple web scraping process of job offers and then saved them in a `csv` file.

With this repository, I intend to improve the quality of the project by incorporating a flow based on [ETL processes](https://aws.amazon.com/what-is/etl/) (extract, transform, and load). 

Now, the web scraping process extracts more information about the job offers and prepares them to be stored in a `sqlite` database for later analysis.

Also, the scraping process handles connection errors such as [`error 429`](https://blog.hubspot.com/website/http-error-429#:~:text=A%20429%20response%20is%20not,accept%20it%20at%20this%20time.), in addition to recording each process in an execution log.

### How to run:

1. Create a virtual environment with the `requirements.txt` dependencies.

2. Run:
```bash
python src/main.py
```

3. The `data base` and `execution_logs` will be store in `data/` foldel.

4. The `analytics/` folder contains a notebook with some analytics of the data.
