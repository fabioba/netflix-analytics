insert into netflix-analytics-448017.ANALYTICS_NETFLIX.BRIDGE_MOVIE_GENRE

    SELECT distinct
        ID_MOVIE ,
        ID_GENRE 
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.STG_BRIDGE_MOVIE_GENRE 
    ;