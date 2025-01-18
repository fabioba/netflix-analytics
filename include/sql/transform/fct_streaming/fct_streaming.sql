insert into netflix-analytics-448017.ANALYTICS_NETFLIX.FCT_STREAMING

    SELECT distinct 
        FARM_FINGERPRINT(concat(TITLE, cast(STREAMING_DATE as string))) as FCT_STREAMING_ID,
        TITLE ,
        STREAMING_DATE,
        ID_MOVIE,
        current_timestamp() as INSERT_TIMESTAMP
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.STG_STREAMING 
    ;