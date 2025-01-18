insert into netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE

    SELECT distinct
        ID_GENRE ,
        NAME ,
        current_timestamp() as INSERT_TIMESTAMP
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.STG_GENRE 
    ;