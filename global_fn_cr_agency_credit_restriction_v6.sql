DROP FUNCTION IF EXISTS global.fn_cr_agency_credit_restriction( CHARACTER VARYING, CHARACTER VARYING );
--select * from global.fn_cr_agency_credit_restriction ('acme','9999999999')
CREATE OR REPLACE FUNCTION global.fn_cr_agency_credit_restriction
(
  parm_tenant_schema_name CHARACTER VARYING,
  parm_application_call_id CHARACTER VARYING  
)
RETURNS TABLE
(
  campaign_id BIGINT,
  agency_credit_restriction CHARACTER VARYING
) 
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
  var_dyn TEXT := ''; 
  
  var_function_name VARCHAR := 'global.fn_cr_agency_credit_restriction';
  var_function_call_text TEXT;
  var_execution_history_id INT;

BEGIN

  PERFORM (set_config('search_path', parm_tenant_schema_name, True));

  var_function_call_text = 'SELECT * FROM ' || var_function_name || '( parm_tenant_schema_name := ''' ||  parm_tenant_schema_name || ''', parm_application_call_id := ''' || parm_application_call_id || ''');';

  SELECT global.fn_report_log_execution_history
  (
    parm_object_called := var_function_name,
    parm_function_application_id := parm_application_call_id ::varchar,
    parm_function_call_text := var_function_call_text
  )
  INTO var_execution_history_id;


  DROP TABLE IF EXISTS agency_credit_restriction;

  CREATE TEMPORARY TABLE agency_credit_restriction
  (
    campaign_id BIGINT,
    agency_credit_restriction CHARACTER VARYING
  );

 
  var_dyn = 'INSERT INTO agency_credit_restriction
    ( campaign_id, agency_credit_restriction )
  SELECT
    c.id AS campaign_id,
    COALESCE ( CASE 
                WHEN grc.name = ''Cash'' THEN gclt_ccr.name
                WHEN grc.NAME = ''Trade'' THEN gclt_tcr.NAME 
              END, ''Credit restriction not set'' ) AS agency_credit_restricition
  FROM  
    tbl_anchor anchor 
    INNER JOIN campaigns c ON C.ID = anchor.campaign_id
    INNER JOIN global.global_revenue_classes grc ON c.global_revenue_class_id = grc.id
    INNER JOIN campaign_markets cm  ON cm.campaign_id = c.id AND cm.type = ''TrafficCampaignMarket''
    INNER JOIN accounts a ON a.id = cm.account_id
    LEFT JOIN agencies agy ON a.agency_id = agy.id
    LEFT JOIN buyer_settings bs_agy ON a.buyer_settings_id = bs_agy.id
    LEFT JOIN cash_credit_restrictions ccr_agy ON bs_agy.cash_credit_restriction_id = ccr_agy.id
    LEFT JOIN global.global_credit_limit_types gclt_ccr ON ccr_agy.global_credit_limit_type_id = gclt_ccr.id
    LEFT JOIN trade_credit_restrictions tcr_agy ON bs_agy.trade_credit_restriction_id = tcr_agy.id
    LEFT JOIN global.global_credit_limit_types gclt_tcr ON tcr_agy.global_credit_limit_type_id = gclt_tcr.id
  GROUP BY
    c.id,
    grc.name,
    gclt_ccr.name,
    gclt_tcr.name';
	

  PERFORM global.fn_report_log_execution_history(
    parm_execution_history_id := var_execution_history_id,
    parm_dynamic_query_string := var_dyn
  );

  EXECUTE var_dyn;

  PERFORM global.fn_report_log_execution_history(
    parm_execution_history_id := var_execution_history_id
  );  


  RETURN QUERY EXECUTE 'SELECT
				campaign_id,
  				agency_credit_restriction
    FROM 
      agency_credit_restriction';

END
$BODY$;

ALTER FUNCTION global.fn_cr_agency_credit_restriction( CHARACTER VARYING, CHARACTER VARYING) OWNER TO marketron_db;
