
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_GENRE

    SELECT distinct
        rg.GENRE_ID ,
        rg.NAME 
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_GENRE rg
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE dg
        ON rg.GENRE_ID = dg.GENRE_ID
    WHERE TRUE
        AND dg.GENRE_ID IS NULL
        AND rg.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )