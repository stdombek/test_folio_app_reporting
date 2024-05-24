--metadb:function boolean_test

DROP FUNCTION IF EXISTS boolean_test;

CREATE FUNCTION boolean_test(

    /* Parameters */

    param_status TEXT DEFAULT 'all'
    

)
/* Set return type */
RETURNS TABLE(

    /* Output table */
    user_id      TEXT,
    user_barcode TEXT,
    user_status  TEXT
    
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
            users.id AS user_id,
            jsonb_extract_path_text(users.jsonb, 'barcode') AS user_barcode,
            COALESCE(jsonb_extract_path_text(users.jsonb, 'active'), 'false') :: TEXT AS user_status
        FROM 
            folio_users.users
        WHERE 
            (
                CASE
                    WHEN param_status = 'all'      THEN 1 = 1
                    WHEN param_status = 'active'   THEN COALESCE(jsonb_extract_path_text(users.jsonb, 'active'), 'false') :: TEXT = 'true'
                    WHEN param_status = 'inactive' THEN COALESCE(jsonb_extract_path_text(users.jsonb, 'active'), 'false') :: TEXT = 'false'
                END
            )
        ;
        /* Query End */
END;
$$
LANGUAGE plpgsql;