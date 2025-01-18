
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_STREAMING

    SELECT distinct
        rm.TITLE ,
        cast(rm.date as datetime FORMAT 'DD/MM/YYYY') as STREAMING_DATE,
        dm.ID_MOVIE,
        current_timestamp() as INSERT_TIMESTAMP
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE rm
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE dm
        ON rm.TITLE = dm.TITLE
    WHERE TRUE
        -- exclude series
        and upper(rm.TITLE) not like '%LIMITED SERIE%'
        AND rm.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )