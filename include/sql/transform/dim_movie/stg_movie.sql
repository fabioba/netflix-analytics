
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_MOVIE

    SELECT distinct
        rmd.ID as ID_MOVIE ,
        rm.TITLE ,
        datetime(rmd.RELEASE_DATE) as  RELEASE_DATE,
        rmd.VOTE_AVERAGE ,
        rmd.VOTE_COUNT,
        current_timestamp() as INSERT_TIMESTAMP

    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE rm
    left join netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE_DETAILS rmd
        on rm.TITLE = rmd.TITLE 

    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE dm
        ON rmd.ID = dm.ID_MOVIE
    WHERE TRUE
        AND dm.ID_MOVIE IS NULL
        -- exclude series
        and upper(rm.title) not like '%LIMITED SERIE%'
        AND rm.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )