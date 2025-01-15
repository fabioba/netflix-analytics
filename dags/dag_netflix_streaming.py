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
from resources.utils import get_query, get_data_from_themoviedb
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
    def extract_load_netflix_streaming():
            
        @task()
        def ingest_netflix_streaming():
            """"""

            df_netflix = pd.read_csv('inlcude/data/netflix.csv')
 
            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_netflix.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')
            gcs_hook.upload(
                bucket_name= 'netflix-etl',
                object_name= f'raw/netflix_{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )
        

        @task()
        def load_netflix_streaming():
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')

            list_raw_files = gcs_hook.list(bucket_name='netflix-etl', prefix='raw/')

            for raw_file in list_raw_files:

                logger.info(f'read raw_file: {raw_file}')

                # Initialize the BigQuery Hook
                bq_hook = BigQueryHook(gcp_conn_id= 'gcp-netflix-data')

                # Download the file as a string
                file_content = gcs_hook.download(bucket_name='netflix-etl', object_name=raw_file)
                
                # Convert the content to a pandas DataFrame
                df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))

                rows = df.to_dict(orient="records")

                # Insert rows into the BigQuery table
                bq_hook.insert_all(
                    project_id= 'ace-mile-446412-j2',
                    dataset_id= 'ANALYTICS_NETFLIX',
                    table_id= 'RAW_MOVIE',
                    rows=rows,
                )

        gcs_raw_to_hist = GCSToGCSOperator(
                task_id="gcs_raw_to_hist",
                source_bucket="netflix-etl",
                source_objects= ["raw/*"],
                destination_bucket="netflix-etl",
                destination_object="hist/",
                gcp_conn_id= 'gcp-netflix-data',
                move_object = True
            )    
        
        ingest_netflix_streaming() >> load_netflix_streaming() >> gcs_raw_to_hist



        @task()
        def ingest_movie_details():

            """filter out those movies that are already ingested"""
            bq_hook = BigQueryHook(gcp_conn_id= 'gcp-netflix-data')

            query_new_movies = get_query('include/sql/new_movie.sql')
        
            df_new_movies = bq_hook.get_pandas_df(sql=query_new_movies)

            df_new_movie_details = get_data_from_themoviedb(df_new_movies)

            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_new_movie_details.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')
            gcs_hook.upload(
                bucket_name= 'netflix-etl',
                object_name= f'raw_movie_detail/movie_detail{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )


  
        @task
        def ingest_genre_details():
            pass

        
        ingest_netflix_streaming() >> ingest_movie_details() >> ingest_genre_details()


    @task_group(group_id = "load_data")
    def load_data():

        @task()
        def load_movie_details():
            pass

        
        @task()
        def load_genre_details():
            pass
        
        load_movie_details() >> load_genre_details()


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


    extract_load_netflix_streaming() >> load_data() >> move_extracted_data_to_hist()

dag_netflix_streaming()