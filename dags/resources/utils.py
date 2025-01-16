
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


def get_best_choice_movie(response : str) -> pd.DataFrame:
    """The goal of this method is to extract the best movie from the list returned by the API. 
    Best movie is defined as most voted and with the highest rate

    Args:  
        response (str): response from API
    
    Returns:
        df_movie_detail (pd.DataFrame) : best choice
    """
    response_json = json.loads(response.content)
    list_movie = response_json['results']

    best_movie = None
    max_vote_count = 0
    max_vote_average = 0

    for item in list_movie:
        
        curr_vote_count = item['vote_count']
        curr_vote_average = item['max_vote_average']

        if curr_vote_count > max_vote_count:
            best_movie = item
        
        elif curr_vote_count == max_vote_count and curr_vote_average > max_vote_average:
            best_movie = item
        
    df_movie_detail = pd.DataFrame(best_movie)

    return df_movie_detail


def get_movie_detail_from_themoviedb(movie_title : str, api_key : str) -> str:
    """
    The goal of this method is to extract the movie details from themoviedb

    Args:
        movie_title (str)
        api_key (str)

    Returns:
        df_movie_detail(pd.DataFrame)
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

        df_movie_detail = get_best_choice_movie(response.content)

        return df_movie_detail




def get_api_key():
    """The goal of this method is to return the API key to access to TMDB
    
    Returns:
        api_key (str)
    """
    pass

def get_movie_details(df_new_movie : pd.DataFrame) -> pd.DataFrame:
    """The goal of this methods is to extract the movie detail.

    Args:
        df_new_movie (pd.DataFrame)

    Returns:
        df_new_movie_details (pd.DataFrame)
    """
    
    df_new_movie_details = pd.DataFrame()

    api_key = get_api_key()

    for idx, row in df_new_movie.iterrows():

        df_movie_detail = get_movie_detail_from_themoviedb(row['title'], api_key)

        df_new_movie_details.append(df_movie_detail)

    
    return df_new_movie_details



def get_genre_details() -> pd.DataFrame:
    """The goal of this methods is to extract the genre detail.

    Returns:
        df_genre_details (pd.DataFrame)
    """
    
    # extract data from API
    url = "https://api.themoviedb.org/3/genre/movie/list"


    api_key = get_api_key()

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:

        raise ValueError(f'response.status_code: {response.status_code}')
    
    else:

        df_genre_details = pd.DataFrame(json.loads(response.content)['genres'])

        return df_genre_details
