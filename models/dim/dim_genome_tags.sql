with src_tags as (
    select * from {{ref('src_genome_tags')}}
)
select
   tag_id,
   INITCAP(TRIM(tag)) as tag
from src_tags