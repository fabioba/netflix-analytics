
import logging
logger = logging.getLogger(__name__)

import requests 
import json
import pandas as pd

# Define the task
def get_query(file_path: str):
    """
    The goal of this method is to read a query

    Args:  
        file_path (str): path of the query
    """
    logger.info(f'file_path: {file_path}')

    with open(file_path, "r") as file:
        return file.read()
    

def __get_movie_title_clean(movie_title : str) -> str:
    """
    The goal of this method is to clean the title

    Args:
        movie_title (str)
    
    Returns:
        movie_title_clean (str)
    """
    logger.info(f'movie_title: {movie_title}')

    movie_title_clean = movie_title.replace(' ', '+')

    return movie_title_clean

def get_data_from_themoviedb(movie_title : str, api_key : str) -> str:
    """
    The goal of this method is to extract the movie details from themoviedb

    Args:
        movie_title (str)
        api_key (str)

    Returns:
        df_new_movies(pd.DataFrame)
    """
    logger.info(f'get_data_from_themoviedb movie_title: {movie_title}')

    # get title cleaned
    movie_title_clean = __get_movie_title_clean(movie_title)

    # extract data from API
    url = f"https://api.themoviedb.org/3/search/movie?query={movie_title_clean}"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:

        raise ValueError(f'movie_title_clean : {movie_title_clean} response.status_code: {response.status_code}')
    
    else:

        df_new_movies = pd.DataFrame(json.loads(response.content))

        return df_new_movies

