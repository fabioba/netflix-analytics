
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_MOVIE

    SELECT distinct
        rm.ID as ID_MOVIE ,
        rm.TITLE ,
        rm.RELEASE_DATE ,
        rm.VOTE_AVERAGE ,
        rm.VOTE_COUNT ,
        rm.MOVIE_FLAG 
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE_DETAILS rm
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE dm
        ON rm.ID = dm.ID_MOVIE
    WHERE TRUE
        AND dm.ID_MOVIE IS NULL
        -- exclude series
        and upper(rm.title) not like '%LIMITED SERIE%'
        AND rm.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )