--metadb:function get_custom_fields_users

DROP FUNCTION IF EXISTS get_custom_fields_users;

CREATE FUNCTION get_custom_fields_users()

RETURNS TABLE (
    user_id                    TEXT,
    last_name                  TEXT,
    first_name                 TEXT,
    radiobutton_option_value   TEXT,   
    einzelauswahl_option_value TEXT,
    options_mehrfachauswahl    TEXT,
    textfield                  TEXT,
    textbereich                TEXT,
    datensatz                  TEXT
)
AS 
$$
--
/*
 * CTE START
 */
WITH refid_radiobutton AS (
    SELECT 
        custom_fields.id AS custom_fields_id,
        jsonb_extract_path_text(custom_fields.jsonb, 'refId') AS custom_fields_refid, -- Eindeutig, da vom System generiert.
        jsonb_extract_path_text(custom_field_options.jsonb, 'id') AS option_id,
        jsonb_extract_path_text(custom_field_options.jsonb, 'value') AS option_value,
        jsonb_extract_path_text(custom_field_options.jsonb, 'default') AS option_default
    FROM 
        folio_de15_users.custom_fields 
        CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(custom_fields.jsonb, 'selectField', 'options', 'values')) AS custom_field_options (jsonb) 
    WHERE 
        jsonb_extract_path_text(custom_fields.jsonb, 'refId') = 'radiobutton'
),
refid_einzelauswahl AS (
    SELECT 
        custom_fields.id AS custom_fields_id,
        jsonb_extract_path_text(custom_fields.jsonb, 'refId') AS custom_fields_refid, -- Eindeutig, da vom System generiert.
        jsonb_extract_path_text(custom_field_options.jsonb, 'id') AS option_id,
        jsonb_extract_path_text(custom_field_options.jsonb, 'value') AS option_value,
        jsonb_extract_path_text(custom_field_options.jsonb, 'default') AS option_default
    FROM 
        folio_de15_users.custom_fields 
        CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(custom_fields.jsonb, 'selectField', 'options', 'values')) AS custom_field_options (jsonb) 
    WHERE 
        jsonb_extract_path_text(custom_fields.jsonb, 'refId') = 'einzelauswahl'
),
multiple_selection_users AS (
    SELECT 
        users.id AS user_id,
        multiple_selection
    FROM 
        folio_de15_users.users
        CROSS JOIN LATERAL jsonb_array_elements_text(jsonb_extract_path(users.jsonb, 'customFields', 'mehrfachauswahl')) AS multiple_selection (jsonb)
),
refid_mehrfachauswahl AS (
    SELECT 
        custom_fields.id AS custom_fields_id,
        jsonb_extract_path_text(custom_fields.jsonb, 'refId') AS custom_fields_refid, -- Eindeutig, da vom System generiert.
        jsonb_extract_path_text(custom_field_options.jsonb, 'id') AS option_id,
        jsonb_extract_path_text(custom_field_options.jsonb, 'value') AS option_value,
        jsonb_extract_path_text(custom_field_options.jsonb, 'default') AS option_default
    FROM 
        folio_de15_users.custom_fields 
        CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(custom_fields.jsonb, 'selectField', 'options', 'values')) AS custom_field_options (jsonb) 
    WHERE 
        jsonb_extract_path_text(custom_fields.jsonb, 'refId') = 'mehrfachauswahl'
)
/*
 * CTE ENDE
 */
--
/*
 * MAIN QUERY START
 */
SELECT 
    users.id                                                            :: TEXT,
    jsonb_extract_path_text(users.jsonb, 'personal', 'lastName')        :: TEXT,
    jsonb_extract_path_text(users.jsonb, 'personal', 'firstName')       :: TEXT,
    refid_radiobutton.option_value                                      :: TEXT,
    refid_einzelauswahl.option_value                                    :: TEXT,
    string_agg(refid_mehrfachauswahl.option_value, ', ')                :: TEXT,
    jsonb_extract_path_text(users.jsonb, 'customFields', 'textfeld')    :: TEXT,
    jsonb_extract_path_text(users.jsonb, 'customFields', 'textbereich') :: TEXT,
    jsonb_extract_path_text(users.jsonb)                                :: TEXT
FROM 
    folio_de15_users.users
    LEFT JOIN refid_radiobutton ON refid_radiobutton.option_id = jsonb_extract_path_text(users.jsonb, 'customFields', 'radiobutton')
    LEFT JOIN refid_einzelauswahl ON refid_einzelauswahl.option_id = jsonb_extract_path_text(users.jsonb, 'customFields', 'einzelauswahl')
    LEFT JOIN multiple_selection_users ON multiple_selection_users.user_id = users.id
    LEFT JOIN refid_mehrfachauswahl ON refid_mehrfachauswahl.option_id = multiple_selection_users.multiple_selection
WHERE  
    jsonb_extract_path(users.jsonb, 'customFields') != '{}' ::jsonb 
GROUP BY 
    users.id,
    jsonb_extract_path_text(users.jsonb, 'personal', 'lastName'),
    jsonb_extract_path_text(users.jsonb, 'personal', 'firstName'),
    refid_radiobutton.option_value,
    refid_einzelauswahl.option_value,
    jsonb_extract_path_text(users.jsonb, 'customFields', 'textfeld'),
    jsonb_extract_path_text(users.jsonb, 'customFields', 'textbereich')
/*
 * MAIN QUERY ENDE
 */
$$
LANGUAGE SQL;
