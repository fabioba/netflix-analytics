
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_MOVIE

    SELECT distinct
        rm.MOVIE_ID ,
        rm.TITLE ,
        rm.RELEASE_DATE ,
        rm.VOTE_AVERAGE ,
        rm.VOTE_COUNT ,
        rm.MOVIE_FLAG 
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE rm
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE dm
        ON rm.title = dm.title
    WHERE TRUE
        AND dm.title IS NULL
        and upper(rm.title) not like '%LIMITED SERIE%'
        AND rm.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )