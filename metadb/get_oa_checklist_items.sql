--metadb:function get_oa_checklist_items

DROP FUNCTION IF EXISTS get_oa_checklist_items;

CREATE FUNCTION get_oa_checklist_items(

    /* Parameters */

    param_request_number       TEXT DEFAULT '',
    param_outcome_value        TEXT DEFAULT '',
    param_outcome_label        TEXT DEFAULT '',
    param_description          TEXT DEFAULT ''
    
)
/* Set return type */
RETURNS TABLE(

    /* Output table */
    request_id        UUID,
    request_number    TEXT,
    checklist_item_id UUID,
    description       TEXT,
    outcome_value     TEXT,
    outcome_label     TEXT,
    date_created      TIMESTAMPTZ,
    last_updated      TIMESTAMPTZ
    
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
            publication_request.pr_id                  :: UUID        AS request_id,
            publication_request.pr_request_number      :: TEXT        AS request_number,
            checklist_item.cli_id                      :: UUID        AS checklist_item_id,
            checklist_item_definition.clid_description :: TEXT        AS description,
            cli_outcome.rdv_value                      :: TEXT        AS outcome_value,
            cli_outcome.rdv_label                      :: TEXT        AS outcome_label,
            checklist_item.cli_date_created            :: TIMESTAMPTZ AS date_created,
            checklist_item.cli_last_updated            :: TIMESTAMPTZ AS last_updated
        FROM 
            folio_oa.checklist_item
            LEFT JOIN folio_oa.checklist_item_definition                ON checklist_item_definition.clid_id :: UUID = checklist_item.cli_definition_fk :: UUID
            LEFT JOIN folio_oa.refdata_value             AS cli_outcome ON cli_outcome.rdv_id :: UUID                = checklist_item.cli_outcome_fk :: UUID
            LEFT JOIN folio_oa.publication_request                      ON publication_request.pr_id :: UUID         = checklist_item.cli_parent_fk :: UUID
        WHERE 
            ((publication_request.pr_request_number LIKE '%' || param_request_number || '%') OR (param_request_number = ''))
            AND ((cli_outcome.rdv_value = param_outcome_value) OR (param_outcome_value = ''))
            AND ((cli_outcome.rdv_label = param_outcome_label) OR (param_outcome_label = ''))
            AND ((checklist_item_definition.clid_description LIKE '%' || param_description || '%') OR (param_description = ''))
        ORDER BY 
            publication_request.pr_request_number
        ;
        /* Query End */
END;
$$
LANGUAGE plpgsql;