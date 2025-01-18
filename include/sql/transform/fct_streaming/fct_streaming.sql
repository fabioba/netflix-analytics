insert into netflix-analytics-448017.ANALYTICS_NETFLIX.FCT_STREAMING

    SELECT distinct 
        (select max(coalesce(FCT_STREAMING_ID, 0)) from netflix-analytics-448017.ANALYTICS_NETFLIX.STG_STREAMING ) + row_number() over(partition by STREAMING_DATE order by TITLE) as FCT_STREAMING_ID,
        TITLE ,
        STREAMING_DATE,
        ID_MOVIE
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.STG_STREAMING 
    ;