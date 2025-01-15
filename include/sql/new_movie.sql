select 
    raw_movie.title,
    raw_movie.date
from raw_movie
left join dim_movie
    on raw_movie.title = dim_movie.title
where true
    and dim_movie.title is null
    and raw_movie.insert_timestamp >= (
        select last_value 
        from cfg_flow_manager 
        where flow_name = 'NETFLIX-ANALYTICS'
        );