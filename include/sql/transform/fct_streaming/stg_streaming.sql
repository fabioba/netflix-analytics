
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_STREAMING

    SELECT distinct
        rm.TITLE ,
        rm.date as STREAMING_DATE,
        dm.ID_MOVIE
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE rm
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE dm
        ON rm.ID = dm.ID_MOVIE
    WHERE TRUE
        -- exclude series
        and upper(rm.title) not like '%LIMITED SERIE%'
        AND rm.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )