--ldp:function get_count_user_group_plpgsql

DROP FUNCTION IF EXISTS get_count_user_group_plpgsql;

CREATE FUNCTION get_count_user_group_plpgsql(

    /* Parameters */

    param_user_group TEXT DEFAULT ''

)
/* Set return type */
RETURNS TABLE (

    /* Output table */

    group_id          TEXT,
    group_description TEXT,
    group_name        TEXT,
    count_by_group    INTEGER
    
)
AS 
/* Code block */
$$
/* PROCEDURE section */
BEGIN 

    /* RETURN value is the result of a query */
    RETURN QUERY

        /* Query Start */
        SELECT  
            user_users.patron_group :: TEXT,
            user_groups.desc,
            user_groups.group,
            COUNT(user_users.id) :: INTEGER
        FROM
            public.user_users  
            LEFT JOIN public.user_groups ON user_groups.id = user_users.patron_group
        WHERE 
            ((user_groups.group = param_user_group) OR (param_user_group = ''))
        GROUP BY 
            user_users.patron_group,
            user_groups.desc,
            user_groups.GROUP
        ;
        /* Query End */
END;
$$
LANGUAGE plpgsql
STABLE
PARALLEL SAFE;