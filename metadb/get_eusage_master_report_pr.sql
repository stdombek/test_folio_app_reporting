--metadb:function get_eusage_master_report_pr

DROP FUNCTION IF EXISTS get_eusage_master_report_pr;

CREATE FUNCTION get_eusage_master_report_pr(

    /* Parameters */

    param_reporting_start_period     DATE DEFAULT '2000-01-01', -- Start date of period of the COUNTER report (YYYY-MM-DD)
    param_reporting_end_period       DATE DEFAULT '2050-01-01', -- End date of period of the COUNTER report (YYYY-MM-DD)
    param_platform                   TEXT DEFAULT ''            -- The name of the platform

)
/* Set return type */
RETURNS TABLE(

    /* Output table */
    id                         UUID,
    report_name                TEXT,
    report_id                  TEXT,
    release                    TEXT,
    institution_name           TEXT,
    institution_id             TEXT,
    reporting_period_start     DATE,
    reporting_period_end       DATE,
    created                    DATE,
    created_by                 TEXT,
    report_year_mounth         TEXT,
    platform                   TEXT,
    data_type                  TEXT,
    access_method              TEXT,
    searches_platform          INTEGER,
    total_item_requests        INTEGER,
    total_item_investigations  INTEGER,
    unique_item_investigations INTEGER,
    unique_item_requests       INTEGER
)
AS 
/* Code block */
$$
/* PROCEDURE section */
BEGIN 

    /* RETURN value is the result of a query */
    RETURN QUERY

        /* Query Start */
        WITH counter_reports AS (
            SELECT 
                counter_reports.id,
                jsonb_extract_path_text(counter_reports.jsonb, 'report', 'Report_Header', 'Report_Name') AS report_name,
                jsonb_extract_path_text(counter_reports.jsonb, 'reportName') AS report_id,
                jsonb_extract_path_text(counter_reports.jsonb, 'report', 'Report_Header', 'Release') AS release,
                --jsonb_extract_path_text(counter_reports.jsonb, 'report', 'Report_Header', 'Institution_Name') AS institution_name,
                'anonymized' AS institution_name,
                --jsonb_extract_path_text(counter_reports.jsonb, 'report', 'Report_Header', 'Customer_ID') AS institution_id,
                'anonymized' AS institution_id,
                jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Period', 'Begin_Date')::DATE AS reporting_period_start,
                jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Period', 'End_Date')::DATE AS reporting_period_end,
                jsonb_extract_path_text(counter_reports.jsonb, 'report', 'Report_Header', 'Created')::DATE AS created,
                jsonb_extract_path_text(counter_reports.jsonb, 'report', 'Report_Header', 'Created_By') AS created_by,
                jsonb_extract_path_text(counter_reports.jsonb, 'yearMonth') AS report_year_mounth,
                jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Platform') AS platform,
                jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Data_Type') AS data_type,
                jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Access_Method') AS access_method,
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->0, 'Metric_Type') AS metric_type_1, 
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->0, 'Count')::INTEGER AS reporting_period_total_1,
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->1, 'Metric_Type') AS metric_type_2, 
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->1, 'Count')::INTEGER AS reporting_period_total_2,
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->2, 'Metric_Type') AS metric_type_3, 
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->2, 'Count')::INTEGER AS reporting_period_total_3,
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->3, 'Metric_Type') AS metric_type_4, 
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->3, 'Count')::INTEGER AS reporting_period_total_4,
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->4, 'Metric_Type') AS metric_type_5, 
                jsonb_extract_path_text(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Performance')), 'Instance')->4, 'Count')::INTEGER AS reporting_period_total_5
            FROM 
                folio_erm_usage.counter_reports
            WHERE 
                jsonb_extract_path_text(counter_reports.jsonb, 'reportName') = 'PR'
            ORDER BY 
                jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(counter_reports.jsonb, 'report', 'Report_Items')), 'Platform'),
                jsonb_extract_path_text(counter_reports.jsonb, 'yearMonth') DESC
        )
        SELECT
            counter_reports.id,
            counter_reports.report_name,
            counter_reports.report_id,
            counter_reports.release,
            counter_reports.institution_name,
            counter_reports.institution_id,
            counter_reports.reporting_period_start,
            counter_reports.reporting_period_end,
            counter_reports.created,
            counter_reports.created_by,
            counter_reports.report_year_mounth,
            counter_reports.platform,
            counter_reports.data_type,
            counter_reports.access_method,
            CASE 
                WHEN counter_reports.metric_type_1 = 'Searches_Platform' THEN reporting_period_total_1
                WHEN counter_reports.metric_type_2 = 'Searches_Platform' THEN reporting_period_total_2
                WHEN counter_reports.metric_type_3 = 'Searches_Platform' THEN reporting_period_total_3
                WHEN counter_reports.metric_type_4 = 'Searches_Platform' THEN reporting_period_total_4
                WHEN counter_reports.metric_type_5 = 'Searches_Platform' THEN reporting_period_total_5
            END AS searches_platform,
            CASE 
                WHEN counter_reports.metric_type_1 = 'Total_Item_Requests' THEN reporting_period_total_1
                WHEN counter_reports.metric_type_2 = 'Total_Item_Requests' THEN reporting_period_total_2
                WHEN counter_reports.metric_type_3 = 'Total_Item_Requests' THEN reporting_period_total_3
                WHEN counter_reports.metric_type_4 = 'Total_Item_Requests' THEN reporting_period_total_4
                WHEN counter_reports.metric_type_5 = 'Total_Item_Requests' THEN reporting_period_total_5
            END AS total_item_requests,
            CASE 
                WHEN counter_reports.metric_type_1 = 'Total_Item_Investigations' THEN reporting_period_total_1
                WHEN counter_reports.metric_type_2 = 'Total_Item_Investigations' THEN reporting_period_total_2
                WHEN counter_reports.metric_type_3 = 'Total_Item_Investigations' THEN reporting_period_total_3
                WHEN counter_reports.metric_type_4 = 'Total_Item_Investigations' THEN reporting_period_total_4
                WHEN counter_reports.metric_type_5 = 'Total_Item_Investigations' THEN reporting_period_total_5
            END AS total_item_investigations,
            CASE 
                WHEN counter_reports.metric_type_1 = 'Unique_Item_Investigations' THEN reporting_period_total_1
                WHEN counter_reports.metric_type_2 = 'Unique_Item_Investigations' THEN reporting_period_total_2
                WHEN counter_reports.metric_type_3 = 'Unique_Item_Investigations' THEN reporting_period_total_3
                WHEN counter_reports.metric_type_4 = 'Unique_Item_Investigations' THEN reporting_period_total_4
                WHEN counter_reports.metric_type_5 = 'Unique_Item_Investigations' THEN reporting_period_total_5
            END AS unique_item_investigations,
            CASE 
                WHEN counter_reports.metric_type_1 = 'Unique_Item_Requests' THEN reporting_period_total_1
                WHEN counter_reports.metric_type_2 = 'Unique_Item_Requests' THEN reporting_period_total_2
                WHEN counter_reports.metric_type_3 = 'Unique_Item_Requests' THEN reporting_period_total_3
                WHEN counter_reports.metric_type_4 = 'Unique_Item_Requests' THEN reporting_period_total_4
                WHEN counter_reports.metric_type_5 = 'Unique_Item_Requests' THEN reporting_period_total_5
            END AS unique_item_requests
    FROM 
        counter_reports
    WHERE 
        ((counter_reports.platform = param_platform) OR (param_platform = ''))
        AND 
        counter_reports.reporting_period_start >= param_reporting_start_period
        AND 
        counter_reports.reporting_period_end <= param_reporting_end_period
        ;
        /* Query End */
END;
$$
LANGUAGE plpgsql;