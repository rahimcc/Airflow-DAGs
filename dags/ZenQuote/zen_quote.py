from airflow.sdk import task, dag
import json
import os
from datetime import datetime


OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default = "file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv("OBJECT_STORAGE_PATH_NEWSLETTER", default="include/news_letter")

ZENQUOTE_API_URL = os.getenv("ZENQUOTE_API_URL", default=None)

SCHEDULE = "0 6 * * *"


@dag( 
    dag_id= "ZenQuote",
    start_date = datetime(2026,9,21),
    catchup = False,
    max_active_runs = 1,
    tags = ["Zenquote"]
)
def ZenQuote():

    @task()
    def raw_zen_quotes() -> list[dict]:

        """
        Extracts random set of quotes
        """
        import requests

        print(ZENQUOTE_API_URL)
        r = requests.get(ZENQUOTE_API_URL)
        quotes = r.json()

        os.makedirs("include/data", exist_ok=True)

        with open("include/data/raw_zen_quotes.json", "r") as file:
            json.dump(file)

        return "{"":""}"


    @task ()
    def selected_quotes(raw_zen_quotes: dict) -> dict:

        """
        Transforms the extracted raw_zen_quotes 
        """
        import numpy as np
       # from airflow.models.xcom import XCom

    # raw_zen_quotes = context["ti"].xcom_pull(task_ids = ["raw_zen_quotes"], include_prior_dates = True)

        #print(all_xcoms) 
        with open ("include/data/raw_zen_quotes.json", "r") as f:
            raw_zen_quotes = json.load(f)
        

        print(f'Load {len(raw_zen_quotes)} quotes from file.')


        
    # print(raw_zen_quotes)
        quotes_character_count = [int(quote["c"]) for quote in raw_zen_quotes]
        median = np.median(quotes_character_count)


        median_quote = min( raw_zen_quotes, key = lambda q: int(q['c'])-median)
        raw_zen_quotes.pop(raw_zen_quotes.index(median_quote))

        short_quote = [ quote for quote in raw_zen_quotes if int(quote["c"]) < median ][0]
        long_quote = [ quote for quote in raw_zen_quotes if int(quote["c"]) > median ][0]

        quotes =  [short_quote, median_quote , long_quote ]

        with open("include/data/selected_quotes.json", "w") as file:
            json.dump(quotes,file)


        return quotes 


    @task ()
    def formatted_newsletter(context: dict) -> None:
        """
        Formats the newsletter. 
        """
        import numpy as np

        from airflow.sdk import ObjectStoragePath

        object_storage_path = ObjectStoragePath(f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",conn_id = OBJECT_STORAGE_CONN_ID)
        date = context['dag_run'].run_after.strftime('%Y-%m-%d')


        with open("include/data/selected_quotes.json", "r") as file:
            selected_quotes = json.load(file)

        
        
        quotes_characters_counts = [int(quote['c']) for quote in selected_quotes]
        median = np.median(quotes_characters_counts)
        print("Test 1")

        newsletter_template_path = ( 
            object_storage_path / "newsletter_template.txt"
        )

        newsletter_template = ( 
            newsletter_template_path.read_text()
            )
        print("Test 2")
        print(type(newsletter_template))
        print(newsletter_template)
        newsltetter = newsletter_template.format(
            date = {date},
            quote_text_1 = selected_quotes[0]["q"],
            quote_author_1 = selected_quotes[0]["a"],
            quote_text_2 = selected_quotes[1]["q"],
            quote_author_2 = selected_quotes[1]["a"],
            quote_text_3 = selected_quotes[2]["q"],
            quote_author_3 = selected_quotes[2]["a"]
        )

        date_newsletter_path = ( 
            object_storage_path / f"{date}_newsletter.txt"
        )
        print(newsltetter)

        date_newsletter_path.write_text(newsltetter)


    formatted_newsletter(selected_quotes(raw_zen_quotes()))

ZenQuote()