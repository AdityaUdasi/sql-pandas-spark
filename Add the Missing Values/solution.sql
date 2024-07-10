select
    row_id,
    coalesce(
        job_role,
        (
            select
                job_role
            from
                job_skills as b
            where
                job_role is not null
                and a.row_id > b.row_id
            order by
                row_id desc
            limit
                1
        )
    ), skills
from
    job_skills as a