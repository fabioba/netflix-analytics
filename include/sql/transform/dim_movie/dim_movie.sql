insert into netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE

    SELECT distinct
        MOVIE_ID ,
        TITLE ,
        RELEASE_DATE ,
        VOTE_AVERAGE ,
        VOTE_COUNT ,
        MOVIE_FLAG 
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.STG_MOVIE 
    ;