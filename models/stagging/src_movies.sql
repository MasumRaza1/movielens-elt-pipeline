    WITH raw_movies AS (
        select * from MOVIELENS.RAW.raw_movies
    )
    select 
     movieId as movie_id,
  title,
  genres
  from raw_movies