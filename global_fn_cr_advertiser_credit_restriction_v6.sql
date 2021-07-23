DROP FUNCTION IF EXISTS global.fn_cr_advertiser_credit_restriction( CHARACTER VARYING, CHARACTER VARYING);

CREATE OR REPLACE FUNCTION global.fn_cr_advertiser_credit_restriction
(
	parm_tenant_schema_name CHARACTER VARYING,
	parm_application_call_id CHARACTER VARYING
)
RETURNS TABLE
(
  campaign_id BIGINT,
  advertiser_credit_restriction CHARACTER VARYING
) 
LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
  var_dyn TEXT := ''; 
  
  var_function_name VARCHAR := 'global.fn_cr_advertiser_credit_restriction';
  var_function_call_text TEXT;
  var_execution_history_id INT;
  
BEGIN

  PERFORM (set_config('search_path', parm_tenant_schema_name, True));

  var_function_call_text = 'SELECT * FROM ' || var_function_name || '( parm_tenant_schema_name :=''' ||  parm_tenant_schema_name || ''', parm_application_call_id := ''' || parm_application_call_id || ''');';

  SELECT global.fn_report_log_execution_history
  (
    parm_object_called := var_function_name,
    parm_function_application_id := parm_application_call_id::varchar,
    parm_function_call_text := var_function_call_text
  )
  INTO var_execution_history_id;


  DROP TABLE IF EXISTS advertiser_credit_restriction;
	CREATE TEMPORARY TABLE advertiser_credit_restriction
	(
	  campaign_id BIGINT,
	  advertiser_credit_restriction CHARACTER VARYING
	);

  var_dyn = 'INSERT INTO advertiser_credit_restriction
    ( campaign_id, advertiser_credit_restriction )
  SELECT
    c.id AS compaign_id,
    COALESCE (CASE
                WHEN a.direct = TRUE
                  THEN --advertiser settings
                    CASE
                      WHEN grc.name = ''Cash'' THEN gclt_ccr_adv.name
                      WHEN grc.name = ''Trade'' THEN gclt_tcr_adv.NAME
                      ELSE ''Credit restriction not set''
                    END --AS credit_restricition
                WHEN a.direct = FALSE
                  THEN --agency settings
                    CASE
                      WHEN grc.name = ''Cash'' THEN gclt_ccr_agy.name
                      WHEN grc.name = ''Trade'' THEN gclt_tcr_agy.NAME
                      ELSE ''Credit restriction not set''
                    END --AS credit_restricition
                ELSE ''Credit restriction not set''
              END, ''Credit restriction not set'' ) AS advertiser_credit_restricition
  FROM  
    tbl_anchor anchor 
    INNER JOIN campaigns c ON C.ID = anchor.campaign_id
    INNER JOIN global.global_revenue_classes grc ON c.global_revenue_class_id = grc.id
    INNER JOIN campaign_markets cm  ON cm.campaign_id = c.id AND cm.type = ''TrafficCampaignMarket''
    INNER JOIN accounts a  ON a.id = cm.account_id
    LEFT JOIN advertisers adv ON a.advertiser_id = adv.id
    LEFT JOIN agencies agy  ON a.agency_id = agy.id
    LEFT JOIN buyer_settings bs_adv ON a.buyer_settings_id = bs_adv.id
    LEFT JOIN cash_credit_restrictions ccr_adv ON bs_adv.cash_credit_restriction_id = ccr_adv.id
    LEFT JOIN global.global_credit_limit_types gclt_ccr_adv ON ccr_adv.global_credit_limit_type_id = gclt_ccr_adv.id
    LEFT JOIN trade_credit_restrictions tcr_adv ON bs_adv.trade_credit_restriction_id = tcr_adv.id
    LEFT JOIN global.global_credit_limit_types gclt_tcr_adv ON tcr_adv.global_credit_limit_type_id = gclt_tcr_adv.id
    LEFT JOIN buyer_settings bs_agy ON a.buyer_settings_id = bs_agy.id
    LEFT JOIN cash_credit_restrictions ccr_agy ON bs_agy.cash_credit_restriction_id = ccr_agy.id
    LEFT JOIN global.global_credit_limit_types gclt_ccr_agy ON ccr_agy.global_credit_limit_type_id = gclt_ccr_agy.id
    LEFT JOIN trade_credit_restrictions tcr_agy ON bs_agy.trade_credit_restriction_id = tcr_agy.id
    LEFT JOIN global.global_credit_limit_types gclt_tcr_agy ON tcr_agy.global_credit_limit_type_id = gclt_tcr_agy.id
  GROUP BY
    c.id,
    a.direct,
    grc.name,
    gclt_ccr_adv.name,
    gclt_tcr_adv.name,
    gclt_ccr_agy.name,
    gclt_tcr_agy.name';
  

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
    advertiser_credit_restriction
  FROM 
    advertiser_credit_restriction';

END
$BODY$;

ALTER FUNCTION global.fn_cr_advertiser_credit_restriction( CHARACTER VARYING, CHARACTER VARYING) OWNER TO marketron_db;
