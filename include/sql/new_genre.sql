select distinct
    raw_movie_detail.genre_ids
from raw_movie_detail
left join dim_genre
    on raw_movie_detail.genre_ids = dim_movie.genre_id
where true
    -- exclude already stored genre_id
    and dim_genre.genre_id is null
    -- only includes those records ingested since the last run
    and raw_movie.insert_timestamp >= (
        select last_value 
        from cfg_flow_manager 
        where flow_name = 'NETFLIX-ANALYTICS'
        )