insert into netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE

    SELECT distinct
        GENRE_ID ,
        NAME 
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.STG_GENRE 
    ;