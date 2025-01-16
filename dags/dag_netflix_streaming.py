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
from resources.utils import get_query, get_movie_details, get_genre_details
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
    
    @task_group(group_id = "extract_load_netflix_streaming")
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


    @task_group(group_id = "extract_load_movie_detail")
    def extract_load_movie_details():
            

        @task()
        def ingest_movie_details():

            """filter out those movies that are already ingested"""
            bq_hook = BigQueryHook(gcp_conn_id= 'gcp-netflix-data')

            query_new_movies = get_query('include/sql/new_movie.sql')
        
            df_new_movies = bq_hook.get_pandas_df(sql=query_new_movies)

            df_new_movie_details = get_movie_details(df_new_movies)

            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_new_movie_details.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')
            gcs_hook.upload(
                bucket_name= 'netflix-etl',
                object_name= f'raw_movie_detail/movie_details_{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )


  
        @task()
        def load_movie_details():
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')

            list_raw_files = gcs_hook.list(bucket_name='netflix-etl', prefix='raw_movie_detail/')

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
                    table_id= 'RAW_MOVIE_DETAILS',
                    rows=rows,
                )

        gcs_raw_movie_detail_to_hist = GCSToGCSOperator(
                task_id="gcs_raw_movie_detail_to_hist",
                source_bucket="netflix-etl",
                source_objects= ["raw_movie_detail/*"],
                destination_bucket="netflix-etl",
                destination_object="hist_movie_detail/",
                gcp_conn_id= 'gcp-netflix-data',
                move_object = True
            )    
        
        ingest_movie_details() >> load_movie_details() >> gcs_raw_movie_detail_to_hist


    @task_group(group_id = "extract_load_genre_details")
    def extract_load_genre_details():
            
        
        @task()
        def ingest_genre_details():

            df_new_genre_details = get_genre_details()

            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_new_genre_details.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')
            gcs_hook.upload(
                bucket_name= 'netflix-etl',
                object_name= f'raw_genre_details/genre_details_{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )


  
        @task()
        def load_genre_details():
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-netflix-data')

            list_raw_files = gcs_hook.list(bucket_name='netflix-etl', prefix='raw_genre_details/')

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
                    table_id= 'RAW_GENRE_DETAILS',
                    rows=rows,
                )

        gcs_raw_genre_details_to_hist = GCSToGCSOperator(
                task_id="gcs_raw_genre_details_to_hist",
                source_bucket="netflix-etl",
                source_objects= ["raw_genre_details/*"],
                destination_bucket="netflix-etl",
                destination_object="hist_genre_details/",
                gcp_conn_id= 'gcp-netflix-data',
                move_object = True
            )    
        
        ingest_genre_details() >> load_genre_details() >> gcs_raw_genre_details_to_hist


        

    extract_load_netflix_streaming() >> extract_load_movie_details() >> extract_load_genre_details()


dag_netflix_streaming()