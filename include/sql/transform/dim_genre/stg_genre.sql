
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_GENRE

    SELECT distinct
        rg.ID as ID_GENRE ,
        rg.NAME ,
        current_timestamp() as INSERT_TIMESTAMP
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_GENRE_DETAILS rg
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE dg
        ON rg.ID = dg.ID_GENRE
    WHERE TRUE
        AND dg.ID_GENRE IS NULL
        AND rg.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )