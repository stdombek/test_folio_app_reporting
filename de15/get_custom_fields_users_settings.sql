--metadb:function get_custom_fields_users_settings

DROP FUNCTION IF EXISTS get_custom_fields_users_settings;

CREATE FUNCTION get_count_user_group()
RETURNS TABLE (
    id             TEXT,
    name           TEXT,
    reference      TEXT,
    option_id      TEXT,   
    option_value   TEXT,
    option_default TEXT
)
AS 
$$
WITH cf_options AS (
SELECT 
    cf.id,
    jsonb_extract_path_text(cf.jsonb, 'name') AS name,
    jsonb_extract_path_text(cf.jsonb, 'refId') AS ref_id,
    jsonb_extract_path_text(custom_fields.jsonb, 'id') AS option_id,
    jsonb_extract_path_text(custom_fields.jsonb, 'value') AS option_value,
    jsonb_extract_path_text(custom_fields.jsonb, 'default') AS option_default
FROM 
    folio_de15_users.custom_fields AS cf 
    CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(cf.jsonb, 'selectField', 'options', 'values')) AS custom_fields (jsonb) 
)
SELECT 
    cf.id                                      :: TEXT,
    jsonb_extract_path_text(cf.jsonb, 'name')  :: TEXT,
    jsonb_extract_path_text(cf.jsonb, 'refId') :: TEXT,
    cf_options.option_id                       :: TEXT,
    cf_options.option_value                    :: TEXT,
    cf_options.option_default                  :: TEXT
FROM 
    folio_de15_users.custom_fields AS cf
    LEFT JOIN cf_options ON cf_options.id = cf.id
$$
LANGUAGE SQL;
