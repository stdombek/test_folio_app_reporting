--metadb:function get_count_user_group

DROP FUNCTION IF EXISTS get_count_user_group;

CREATE FUNCTION get_count_user_group(
    param_user_group TEXT DEFAULT ''
)
RETURNS TABLE (
    group_id          TEXT,
    group_name        TEXT,
    group_description TEXT,
    count_by_group    INTEGER    
)
AS 
$$
SELECT 
    groups.id                                      :: TEXT,
    jsonb_extract_path_text(groups.jsonb, 'group') :: TEXT,
    jsonb_extract_path_text(groups.jsonb, 'desc')  :: TEXT,
    COUNT(users.id)                                :: INTEGER
FROM 
    folio_de15_users.users
    LEFT JOIN folio_de15_users.groups ON groups.id = jsonb_extract_path_text(users.jsonb, 'patronGroup') :: UUID 
WHERE 
    ((jsonb_extract_path_text(groups.jsonb, 'group') = param_user_group) OR (param_user_group = ''))
GROUP BY 
    groups.id,
    jsonb_extract_path_text(groups.jsonb, 'group'),
    jsonb_extract_path_text(groups.jsonb, 'desc')
$$
LANGUAGE SQL;