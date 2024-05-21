--metadb:function get_oa_publication_requests

DROP FUNCTION IF EXISTS get_oa_publication_requests;

CREATE FUNCTION get_oa_publication_requests(

    /* Parameters */

    param_request_status_label TEXT DEFAULT '',
    param_request_number       TEXT DEFAULT ''
    

)
/* Set return type */
RETURNS TABLE(

    /* Output table */
    pr_id                                      TEXT,
    pr_request_date                            TIMESTAMPTZ,
    pr_date_created                            TIMESTAMPTZ,        
    pr_last_updated                            TIMESTAMPTZ,        
    pr_request_number                          TEXT,        
    pr_title                                   TEXT,        
    pr_request_status                          TEXT,        
    pr_request_status_value                    TEXT,
    pr_request_status_label                    TEXT,        
    pr_pub_type_fk                             TEXT,
    pr_pub_type_value                          TEXT,
    pr_pub_type_label                          TEXT,        
    pr_authnames                               TEXT,                
    pr_corresponding_author_fk                 TEXT,
    pr_corresponding_author_role_value         TEXT,
    pr_corresponding_author_role_label         TEXT,
    pr_corresponding_author_name               TEXT,
    pr_corresponding_author_rp_party_fk        TEXT,
    pr_local_ref                               TEXT,        
    pr_pub_url                                 TEXT,        
    pr_subtype                                 TEXT,        
    pr_subtype_value                           TEXT,
    pr_subtype_label                           TEXT,        
    pr_publisher                               TEXT,
    pr_publisher_value                         TEXT,
    pr_publisher_label                         TEXT,        
    pr_license                                 TEXT,
    pr_license_value                           TEXT,
    pr_license_label                           TEXT,        
    pr_doi                                     TEXT,        
    pr_group_fk                                TEXT,
    pr_agreement_reference                     TEXT,        
    pr_without_agreement                       TEXT,        
    pr_work_fk                                 TEXT,        
    pr_book_date_of_publication                TEXT,        
    pr_book_place_of_publication               TEXT,        
    pr_work_indexed_in_doaj_fk                 TEXT,        
    pr_work_indexed_in_doaj_value              TEXT,
    pr_work_indexed_in_doaj_label              TEXT,
    pr_work_oa_status_fk                       TEXT,
    pr_work_oa_status_value                    TEXT,
    pr_work_oa_status_label                    TEXT,        
    pr_corresponding_institution_level_1_fk    TEXT,
    pr_corresponding_institution_level_1_value TEXT,
    pr_corresponding_institution_level_1_label TEXT,        
    pr_corresponding_institution_level_2       TEXT,        
    pr_retrospective_oa                        TEXT,        
    pr_closure_reason_fk                       TEXT,        
    pr_closure_reason_value                    TEXT,
    pr_closure_reason_label                    TEXT,
    pr_request_contact_fk                      TEXT,
    pr_request_contact_role_value              TEXT,
    pr_request_contact_role_label              TEXT,
    pr_request_contact_name                    TEXT,
    pr_request_contact_rp_party_fk             TEXT
    
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
            publication_request.pr_id :: TEXT,
            publication_request.pr_request_date :: TIMESTAMPTZ,
            publication_request.pr_date_created :: TIMESTAMPTZ,        
            publication_request.pr_last_updated :: TIMESTAMPTZ,        
            publication_request.pr_request_number :: TEXT,        
            publication_request.pr_title :: TEXT,        
            publication_request.pr_request_status :: TEXT,        
            pr_status.rdv_value :: TEXT AS pr_request_status_value,
            pr_status.rdv_label :: TEXT AS pr_request_status_label,        
            publication_request.pr_pub_type_fk :: TEXT,
            pr_pub_type.rdv_value :: TEXT AS pr_pub_type_value,
            pr_pub_type.rdv_label :: TEXT AS pr_pub_type_label,        
            publication_request.pr_authnames :: TEXT,                
            publication_request.pr_corresponding_author_fk :: TEXT,
            rp_role_corresponding_author.rdv_value :: TEXT AS pr_corresponding_author_role_value,
            rp_role_corresponding_author.rdv_label :: TEXT AS pr_corresponding_author_role_label,
            party_corresponding_author.p_full_name :: TEXT AS pr_corresponding_author_name,
            pr_corresponding_author.rp_party_fk :: TEXT AS pr_corresponding_author_rp_party_fk,
            publication_request.pr_local_ref :: TEXT,        
            publication_request.pr_pub_url :: TEXT,        
            publication_request.pr_subtype :: TEXT,        
            pr_subtype.rdv_value :: TEXT AS pr_subtype_value,
            pr_subtype.rdv_label :: TEXT AS pr_subtype_label,        
            publication_request.pr_publisher :: TEXT,
            pr_publisher.rdv_value :: TEXT AS pr_publisher_value,
            pr_publisher.rdv_label :: TEXT AS pr_publisher_label,        
            publication_request.pr_license :: TEXT,
            pr_license.rdv_value :: TEXT AS pr_license_value,
            pr_license.rdv_label :: TEXT AS pr_license_label,        
            publication_request.pr_doi :: TEXT,        
            publication_request.pr_group_fk :: TEXT,
            publication_request.pr_agreement_reference :: TEXT, 
            COALESCE(publication_request.pr_without_agreement, 'false') :: TEXT AS pr_without_agreement,        
            publication_request.pr_work_fk :: TEXT,        
            publication_request.pr_book_date_of_publication :: TEXT,        
            publication_request.pr_book_place_of_publication :: TEXT,        
            publication_request.pr_work_indexed_in_doaj_fk :: TEXT,        
            pr_doaj_status.rdv_value :: TEXT AS pr_work_indexed_in_doaj_value,
            pr_doaj_status.rdv_label :: TEXT AS pr_work_indexed_in_doaj_label,
            publication_request.pr_work_oa_status_fk :: TEXT,
            pr_oa_status.rdv_value :: TEXT AS pr_work_oa_status_value,
            pr_oa_status.rdv_label :: TEXT AS pr_work_oa_status_label,        
            publication_request.pr_corresponding_institution_level_1_fk :: TEXT,
            pr_corresponding_institution_level_1.rdv_value :: TEXT AS pr_corresponding_institution_level_1_value,
            pr_corresponding_institution_level_1.rdv_label :: TEXT AS pr_corresponding_institution_level_1_label,        
            publication_request.pr_corresponding_institution_level_2 :: TEXT,        
            COALESCE(publication_request.pr_retrospective_oa, 'false') :: TEXT AS pr_retrospective_oa,        
            publication_request.pr_closure_reason_fk :: TEXT,        
            pr_closure_reason.rdv_value :: TEXT AS pr_closure_reason_value,
            pr_closure_reason.rdv_label :: TEXT AS pr_closure_reason_label,
            publication_request.pr_request_contact_fk :: TEXT,
            rp_role_request_contact.rdv_value :: TEXT AS pr_request_contact_role_value,
            rp_role_request_contact.rdv_label :: TEXT AS  pr_request_contact_role_label,
            party_request_contact.p_full_name :: TEXT AS pr_request_contact_name,
            pr_request_contact.rp_party_fk :: TEXT AS pr_request_contact_rp_party_fk
        FROM 
            folio_oa.publication_request
            LEFT JOIN folio_oa.refdata_value AS pr_status ON pr_status.rdv_id :: UUID                                                       = publication_request.pr_request_status :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_pub_type ON pr_pub_type.rdv_id :: UUID                                                   = publication_request.pr_pub_type_fk :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_subtype ON pr_subtype.rdv_id :: UUID                                                     = publication_request.pr_subtype :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_publisher ON pr_publisher.rdv_id :: UUID                                                 = publication_request.pr_publisher :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_license ON pr_license.rdv_id :: UUID                                                     = publication_request.pr_license :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_doaj_status ON pr_doaj_status.rdv_id :: UUID                                             = publication_request.pr_work_indexed_in_doaj_fk :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_oa_status ON pr_oa_status.rdv_id :: UUID                                                 = publication_request.pr_work_oa_status_fk :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_corresponding_institution_level_1 ON pr_corresponding_institution_level_1.rdv_id :: UUID = publication_request.pr_corresponding_institution_level_1_fk :: UUID
            LEFT JOIN folio_oa.refdata_value AS pr_closure_reason ON pr_closure_reason.rdv_id :: UUID                                       = publication_request.pr_closure_reason_fk :: UUID
            LEFT JOIN folio_oa.request_party AS pr_corresponding_author ON pr_corresponding_author.rp_id :: UUID                            = publication_request.pr_corresponding_author_fk :: UUID
            LEFT JOIN folio_oa.refdata_value AS rp_role_corresponding_author ON rp_role_corresponding_author.rdv_id :: UUID                 = pr_corresponding_author.rp_role :: UUID        
            LEFT JOIN folio_oa.request_party AS pr_request_contact ON pr_request_contact.rp_id :: UUID                                      = publication_request.pr_request_contact_fk :: UUID
            LEFT JOIN folio_oa.refdata_value AS rp_role_request_contact ON rp_role_request_contact.rdv_id :: UUID                           = pr_request_contact.rp_role :: UUID
            LEFT JOIN folio_oa.party AS party_corresponding_author ON party_corresponding_author.p_id :: UUID                               = pr_corresponding_author.rp_party_fk :: UUID
            LEFT JOIN folio_oa.party AS party_request_contact ON party_request_contact.p_id :: UUID                                         = pr_request_contact.rp_party_fk :: UUID
        WHERE 
                ((pr_status.rdv_label = param_request_status_label) OR (param_request_status_label = ''))
            AND ((publication_request.pr_request_number LIKE '%' || param_request_number || '%') OR (param_request_number = ''))
        ORDER BY 
            publication_request.pr_request_number
        ;
        /* Query End */
END;
$$
LANGUAGE plpgsql;