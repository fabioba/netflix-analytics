drop table netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE(
  TITLE string,
  DATE string,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

drop table netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE_DETAILS;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_MOVIE_DETAILS(
  ADULT bool,
  BACKDROP_PATH string,
  GENRE_IDS string,
  ID int64,
  ORIGINAL_LANGUAGE string,
  ORIGINAL_TITLE string,
  OVERVIEW string,
  POPULARITY float64,
  POSTER_PATH string,
  RELEASE_DATE string,
  TITLE string,
  VIDEO bool,
  VOTE_AVERAGE float64,
  VOTE_COUNT int64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);


create table netflix-analytics-448017.ANALYTICS_NETFLIX.RAW_GENRE_DETAILS(
  ID int64,
  NAME string,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);


create table netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER(
  FLOW_NAME string,
  LAST_VALUE timestamp,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

insert into netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER(flow_name, last_value)
values('NETFLIX-ANALYTICS-TRANSFORM', '2025-01-01');

insert into netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER(flow_name, last_value)
values('NETFLIX-ANALYTICS-INGEST-LOAD', '2025-01-01');



drop table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_MOVIE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_MOVIE(
  ID_MOVIE int64,
  TITLE string,
  RELEASE_DATE datetime,
  VOTE_AVERAGE float64,
  VOTE_COUNT int64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

drop table netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE(
  ID_MOVIE int64,
  TITLE string,
  RELEASE_DATE datetime,
  VOTE_AVERAGE float64,
  VOTE_COUNT int64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

drop table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_GENRE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_GENRE(
  ID_GENRE float64,
  NAME string,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

drop table netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE(
  ID_GENRE float64,
  NAME string,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

drop table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_BRIDGE_MOVIE_GENRE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_BRIDGE_MOVIE_GENRE(
  ID_MOVIE  int64,
  ID_GENRE float64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);
drop table netflix-analytics-448017.ANALYTICS_NETFLIX.BRIDGE_MOVIE_GENRE;
create table netflix-analytics-448017.ANALYTICS_NETFLIX.BRIDGE_MOVIE_GENRE(
  ID_MOVIE  int64,
  ID_GENRE float64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);


create table netflix-analytics-448017.ANALYTICS_NETFLIX.STG_STREAMING(
  TITLE  string,
  STREAMING_DATE datetime,
  ID_MOVIE int64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);


create table netflix-analytics-448017.ANALYTICS_NETFLIX.FCT_STREAMING(
  FCT_STREAMING_ID int64,
  TITLE  string,
  STREAMING_DATE datetime,
  ID_MOVIE int64,
  INSERT_TIMESTAMP timestamp default current_timestamp()
);

drop view ANALYTICS_NETFLIX.EL_STREAMING;
create view netflix-analytics-448017.ANALYTICS_NETFLIX.EL_STREAMING as 
select 
  fs.FCT_STREAMING_ID,
    fs.STREAMING_DATE,
    fs.TITLE,
    dm.RELEASE_DATE,
    dm.VOTE_AVERAGE,
    dm.VOTE_COUNT,
    dg.NAME as GENRE,
    fs.id_movie is null as FLAG_MOVIE_DETAIL
from netflix-analytics-448017.ANALYTICS_NETFLIX.FCT_STREAMING fs
left join netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_MOVIE dm
    on fs.id_movie = dm.id_movie

left join netflix-analytics-448017.ANALYTICS_NETFLIX.BRIDGE_MOVIE_GENRE bmg
    on fs.id_movie = bmg.id_movie

left join netflix-analytics-448017.ANALYTICS_NETFLIX.DIM_GENRE dg
    on bmg.id_genre = dg.id_genre

;