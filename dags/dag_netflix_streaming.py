import logging

logger = logging.getLogger(__name__)

from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import pendulum
import pandas as pd
import io
from resources.utils import get_query
import requests
from datetime import datetime

@dag(
    schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=pendulum.datetime(2023, 1, 1),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["netflix"],
)
def dag_netflix_streaming():
    
    @task_group(group_id = "ingest_data")
    def extract_data():
            
        @task
        def ingest_netflix_streaming():
            """"""

            df_netflix = pd.read_csv('inlcude/data/netflix_csv')
 
            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_netflix.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')
            gcs_hook.upload(
                bucket_name= 'netflix-etl',
                object_name= f'netflifx_raw/netflix_{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )


        @task
        def ingest_movie_details():
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')

            list_raw_files = gcs_hook.list(bucket_name='netflix-etl', prefix='netflifx_raw/')

            for raw_file in list_raw_files:

                logger.info(f'read raw_file: {raw_file}')

                # Download the file as a string
                file_content = gcs_hook.download(bucket_name='sales-data-raw', object_name=raw_file)
                
                # Convert the content to a pandas DataFrame
                df_netflix = pd.read_csv(io.StringIO(file_content.decode('utf-8')))


                url = "https://api.themoviedb.org/3/search/movie?query=Jack+Reacher"
                api_key = ''

                headers = {
                    "accept": "application/json",
                    "Authorization": f"Bearer {api_key}"
                }

                response = requests.get(url, headers=headers)

                response.status_code

  
        @task
        def ingest_genre_details():
            pass

        
        ingest_netflix_streaming() >> ingest_movie_details() >> ingest_genre_details()


    @task_group(group_id = "load_data")
    def load_data():

        @task()
        def load_netflix_streaming():
            pass

        @task()
        def load_movie_details():
            pass

        
        @task()
        def load_genre_details():
            pass

        load_netflix_streaming() >> load_movie_details() >> load_genre_details()


    @task_group(group_id = "move_extracted_data_to_hist")
    def move_extracted_data_to_hist():


        @task
        def move_netflix_streaming_hist():
            pass


        @task
        def move_movie_details_hist():
            pass      


        @task
        def move_genre_details_hist():
            pass      

        move_netflix_streaming_hist() >> move_movie_details_hist() >> move_genre_details_hist()


    extract_data() >> load_data() >> move_extracted_data_to_hist()

dag_netflix_streaming()