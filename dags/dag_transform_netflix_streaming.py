import logging

logger = logging.getLogger(__name__)

from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import pendulum
import pandas as pd
import io
from resources.utils import get_query, get_movie_details, get_genre_details
from datetime import datetime

@dag(
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
def dag_transform_netflix_streaming():
    
    @task_group(group_id = "tg_dim_movie")
    def tg_dim_movie():

        truncate_stg_movie = BigQueryInsertJobOperator(
            task_id="truncate_stg_movie",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/truncate_stg_movie.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        stg_movie = BigQueryInsertJobOperator(
            task_id="stg_movie",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/stg_movie.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        dim_movie = BigQueryInsertJobOperator(
            task_id="dim_movie",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/dim_movie.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )
        
        truncate_stg_movie >> stg_movie >> dim_movie

    @task_group(group_id = "tg_dim_genre")
    def tg_dim_genre():

        truncate_stg_genre = BigQueryInsertJobOperator(
            task_id="truncate_stg_genre",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/truncate_stg_genre.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        stg_genre = BigQueryInsertJobOperator(
            task_id="stg_genre",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/stg_genre.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        dim_genre = BigQueryInsertJobOperator(
            task_id="dim_genre",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/dim_genre.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )
        
        truncate_stg_genre >> stg_genre >> dim_genre

    @task_group(group_id = "tg_bridge_movie_genre")
    def tg_bridge_movie_genre():

        truncate_stg_bridge_movie_genre = BigQueryInsertJobOperator(
            task_id="truncate_stg_bridge_movie_genre",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/truncate_stg_bridge_movie_genre.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        stg_bridge_movie_genre = BigQueryInsertJobOperator(
            task_id="stg_bridge_movie_genre",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/stg_bridge_movie_genre.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        bridge_movie_genre = BigQueryInsertJobOperator(
            task_id="bridge_movie_genre",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/bridge_movie_genre.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )
        
        truncate_stg_bridge_movie_genre >> stg_bridge_movie_genre >> bridge_movie_genre

    @task_group(group_id = "fct_streaming")
    def tg_fct_streaming():

        truncate_stg_streaming = BigQueryInsertJobOperator(
            task_id="truncate_stg_streaming",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/truncate_stg_streaming.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        stg_streaming = BigQueryInsertJobOperator(
            task_id="stg_streaming",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/stg_streaming.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )

        fct_streaming = BigQueryInsertJobOperator(
            task_id="fct_streaming",
            configuration={
                "query": {
                    "query": get_query('include/sql/transform/fct_streaming.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-netflix-data",  # Replace if using a custom connection
        )
        
        truncate_stg_streaming >> stg_streaming >> fct_streaming


    tg_dim_movie() >> tg_dim_genre() >> tg_bridge_movie_genre() >> tg_fct_streaming()

dag_transform_netflix_streaming()