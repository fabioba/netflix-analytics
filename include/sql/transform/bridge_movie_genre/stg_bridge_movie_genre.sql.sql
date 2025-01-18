
insert into netflix-analytics-448017.ANALYTICS_NETFLIX.STG_BRIDGE_MOVIE_GENRE

    SELECT distinct
        rm.ID as ID_MOVIE ,
        rm.genre_ids as ID_GENRE
    FROM netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE_DETAILS rm
    LEFT JOIN netflix-analytics-448017.ANALYTICS_NETFLIX.BRIDGE_MOVIE_GENRE bdg
        ON rm.ID = bdg.ID_MOVIE
    WHERE TRUE
        AND bdg.ID_MOVIE IS NULL
        -- exclude series
        and upper(rm.title) not like '%LIMITED SERIE%'
        AND rm.insert_timestamp >= (
            SELECT last_value 
            FROM netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
            WHERE flow_name = 'NETFLIX-ANALYTICS-TRANSFORM'
        )