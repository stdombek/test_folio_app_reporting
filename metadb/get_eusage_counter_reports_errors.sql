--metadb:function get_eusage_counter_reports_errors

DROP FUNCTION IF EXISTS get_eusage_counter_reports_errors;

CREATE FUNCTION get_eusage_counter_reports_errors(

/* Parameters */

)
/* Set return type */
RETURNS TABLE(

    /* Output table */
    usage_data_provider TEXT,
    year_month          TEXT,
    release             TEXT,
    report_name         TEXT,
    failed_attempts     INTEGER,
    download_time       TIMESTAMPTZ,
    failed_reason       TEXT

)
AS 
/* Code block */
$$
/* PROCEDURE section */
BEGIN 

    /* RETURN value is the result of a query */
    RETURN QUERY

        /* Query Start */
        WITH error_reports AS (
            SELECT
                jsonb_extract_path_text(usage_data_providers.jsonb, 'label')           AS usage_data_provider,
                jsonb_extract_path_text(usage_data_providers.jsonb, 'hasFailedReport') AS has_failed_report,
                string_agg(DISTINCT report_error_codes.jsonb, ', ') errors,
                counter_reports.jsonb                                                  AS reports
            FROM 
                folio_erm_usage.usage_data_providers
                CROSS JOIN LATERAL jsonb_array_elements_text(jsonb_extract_path(usage_data_providers.jsonb, 'reportErrorCodes')) WITH ORDINALITY AS report_error_codes (jsonb)
                LEFT JOIN folio_erm_usage.counter_reports ON jsonb_extract_path_text(counter_reports.jsonb, 'providerId') :: UUID = usage_data_providers.id
            GROUP BY 
                usage_data_provider,
                has_failed_report,
                reports
        )
        SELECT 
            error_reports.usage_data_provider                                :: TEXT        AS usage_data_provider,
            jsonb_extract_path_text(error_reports.reports, 'yearMonth')      :: TEXT        AS year_month,
            jsonb_extract_path_text(error_reports.reports, 'release')        :: TEXT        AS release,
            jsonb_extract_path_text(error_reports.reports, 'reportName')     :: TEXT        AS report_name,
            jsonb_extract_path_text(error_reports.reports, 'failedAttempts') :: INTEGER     AS failed_attempts,
            jsonb_extract_path_text(error_reports.reports, 'downloadTime')   :: TIMESTAMPTZ AS download_time,
            jsonb_extract_path_text(error_reports.reports, 'failedReason')   :: TEXT        AS failed_reason
        FROM 
            error_reports
        WHERE 
            jsonb_extract_path_text(error_reports.reports, 'failedReason') IS NOT NULL 
        ORDER BY 
            error_reports.usage_data_provider,
            jsonb_extract_path_text(error_reports.reports, 'yearMonth'),
            error_reports.errors
        ;
        /* Query End */
END;
$$
LANGUAGE plpgsql;