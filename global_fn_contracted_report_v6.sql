DROP FUNCTION global.fn_contracted_report(character varying, bigint, json, bigint, integer, character varying);

CREATE OR REPLACE FUNCTION global.fn_contracted_report
(
  parm_tenant_schema_name CHARACTER VARYING,
  parm_user_id BIGINT,
  parm_query_def JSON,
  parm_report_type_id BIGINT DEFAULT 7,
  parm_page INTEGER DEFAULT 1,
  parm_perpage CHARACTER VARYING DEFAULT 'ALL'::CHARACTER VARYING
)
RETURNS json
LANGUAGE 'plpgsql'
AS $BODY$
------------VARSHA TEST
DECLARE
  var_application_call_id VARCHAR(50) := ( SELECT JSON_EXTRACT_PATH_TEXT( parm_query_def, 'application_call_id' ) );
  var_page INT := CASE WHEN parm_perpage = 'ALL' THEN 0::INT ELSE parm_perpage::INT * ( parm_page - 1 ) END;
  var_total_rows INT;
  var_from TEXT := '';
  var_where TEXT := '';
  var_dynSQL TEXT := '';
  var_dynSQL_result JSON;
  var_msp_tenant_schema_name TEXT := parm_tenant_schema_name;
  var_alias_name VARCHAR(10) :=''; 
  var_tenant_id BIGINT;
  var_temp_tbl TEXT := '';
  var_inset_temp_tbl TEXT := '';
  						
  var_select TEXT;
  var_group_by TEXT := '  GROUP BY ' || E'\n' || '    <<select_column_list>> ';
  var_order_by TEXT := '';

  var_function_name VARCHAR := 'global.fn_contracted_report';
  var_function_call_text TEXT;
  var_execution_history_id INT;
  var_parm_calender_types CHARACTER VARYING;   
  var_parm_months CHARACTER VARYING;  
  var_parm_cal_types CHARACTER VARYING; 
  var_prm_months CHARACTER VARYING; 
  var_application_id CHARACTER VARYING;
  var_projection_clp CHARACTER VARYING;
  var_projection_clpalt CHARACTER VARYING;   
  var_projection_date CHARACTER VARYING;

BEGIN

  PERFORM (set_config('search_path', parm_tenant_schema_name, True));

  var_function_call_text = 'SELECT * FROM ' || var_function_name || '(''' ||  parm_tenant_schema_name || ''',''' || parm_user_id ||''', ''' || parm_query_def   || ''' );';
 
  SELECT global.fn_report_log_execution_history
  (
    parm_object_called := var_function_name,
    parm_function_application_id := var_application_call_id,
    parm_function_call_text := var_function_call_text
  )
  INTO var_execution_history_id;

  ------------------------------------------------------------------------------
  --Create Table to hold data from json combined with data from our setup tables
  --Selected Columns
  DROP TABLE IF EXISTS select_column_list;
  -- select * from select_column_list;
  CREATE TEMPORARY TABLE select_column_list
  (
    id SMALLSERIAL PRIMARY KEY,
    select_column_name VARCHAR,
    alias_name TEXT,
    condition_name TEXT
  );
  
  INSERT INTO select_column_list
    ( select_column_name )
  SELECT
    REPLACE( JSONB_ARRAY_ELEMENTS( ( parm_query_def ->>'select_column_list' )::jsonb )::TEXT, '"', '' ) AS select_list;
  
  SELECT 'clp' INTO var_projection_clp FROM select_column_list WHERE select_column_name IN ('agency_commission_dollars','alt_rev_gross_dollars','average_unit_rate_gross','projected_gross_dollars','net_dollars','projected_gross_percent','net_percent');
  ------------------------------------------------------------------------------    
 
  DROP TABLE IF EXISTS tmp_filter_data;

  CREATE TEMPORARY TABLE tmp_filter_data
  (
    id SMALLSERIAL PRIMARY KEY,
    filter_name TEXT NULL,
    filter_operator TEXT NULL,
    condition_name TEXT NULL,
    condition_type TEXT NULL,
    condition_operator TEXT NULL,
    filter_value_json JSON NULL,
    filter_value TEXT NULL,
    filter_to_value TEXT NULL,
    filter_field TEXT NULL,
    filter_join_snippet_id BIGINT NULL,
    where_clause TEXT NULL,
    invalid_filter BOOLEAN DEFAULT FALSE
  );

  --Populate tmp_filter_data table with filter data from parm_query_def
  WITH filter_data AS 
  (
    SELECT
      LOWER( f.filter_name ) AS filter_name,
      LOWER( f.operator ) AS filter_operator,
	  c.condition_name AS condition_name,
 --     CASE WHEN c.condition_name = 'date_reported' THEN 'c.start_date'
--	      ELSE c.condition_name
--	  END AS condition_name,
      LOWER( c.filter_type ) AS condition_type,
      LOWER( c.operator ) AS condition_operator,
      c.filter_value AS filter_value_json,
      c.filter_to_value
    FROM 
      ( SELECT parm_query_def ->'filter_options' AS filter_options ) fo,
      JSON_TO_RECORDSET( fo.filter_options ) AS f( filter_name TEXT, operator TEXT, conditions JSON ),
      JSON_TO_RECORDSET( f.conditions ) AS c( condition_name TEXT, filter_type TEXT, operator TEXT, filter_value JSON, filter_to_value TEXT )
  )
  INSERT INTO tmp_filter_data
      ( filter_name, filter_operator, condition_name, condition_type, condition_operator,
      filter_value_json, filter_to_value, filter_field, filter_join_snippet_id )
  SELECT 
    fd.filter_name,
    fd.filter_operator,
    fd.condition_name,
    fd.condition_type,
    fd.condition_operator, 
    fd.filter_value_json,
    CASE WHEN fd.filter_to_value = 'undefined'
      THEN ''
      ELSE REPLACE( COALESCE( fd.filter_to_value, '' ), '''', '''''' )
    END,
    LOWER(vrm.filter_field),
    vrm.join_snippet_id
  FROM
    filter_data fd
    LEFT JOIN global.vw_report_mappings vrm
      ON fd.condition_name = lower(vrm.report_field_alias)
        AND vrm.report_type_id = parm_report_type_id;
	
	INSERT INTO tmp_filter_data
      ( filter_name, filter_operator, condition_name, condition_type, condition_operator,
      filter_value_json, filter_to_value, filter_field, filter_join_snippet_id )
	SELECT 	'cal.day' AS filter_name, 
			'AND' AS filter_operator, 
			condition_name, 
			condition_type, 
			condition_operator,
			filter_value_json, 
			filter_to_value, 
			'cal.day' AS filter_field, 
			filter_join_snippet_id
		FROM tmp_filter_data
		WHERE filter_name = 'contract_start_date' AND var_projection_clp = 'clp';
		
  --SELECT * FROM global.vw_report_mappings where report_type_id = 7;
  --Extract filter_value from filter_value_json column
  --Handle non-IN condition operators
  UPDATE tmp_filter_data
  	SET filter_value = replace( filter_value_json #>> '{}', '''', '''''' )
  	WHERE
    	filter_name IS NOT NULL
    	AND condition_operator <> 'in' AND filter_name NOT IN ('campaign_date');

  --Handle condition operator = 'IN' for condition type = 'NUMBER'
  WITH FV AS
  (
    SELECT
      tfd.id,
      STRING_AGG( TRIM( d.elem::text, '"' ), ', ' ) AS filter_value
    FROM
      tmp_filter_data AS tfd
      CROSS JOIN LATERAL json_array_elements( tfd.filter_value_json ) AS d( elem )
    WHERE
      tfd.filter_name IS NOT NULL
      AND tfd.condition_operator = 'in'
      AND tfd.condition_type = 'number'
    GROUP BY
      tfd.id
  )
  UPDATE tmp_filter_data
  	SET filter_value = fv.filter_value
  FROM FV
  WHERE
    FV.id = tmp_filter_data.id;

  --Handle condition operator = 'IN' for condition type <> 'NUMBER'
  WITH FV AS
  (
    SELECT
      tfd.id,
      STRING_AGG( TRIM( REPLACE( d.elem::text, '''', '''''' ), '"' ), ''', ''' ) AS filter_value
    FROM
      tmp_filter_data AS tfd
      CROSS JOIN LATERAL json_array_elements( tfd.filter_value_json ) AS d( elem )
    WHERE
      tfd.filter_name IS NOT NULL
      AND tfd.condition_operator = 'in'
      AND tfd.condition_type <> 'number'
    GROUP BY
      tfd.id
  )
  UPDATE tmp_filter_data
  	SET filter_value = fv.filter_value
  FROM FV
  	WHERE
    FV.id = tmp_filter_data.id;

  --Update WHERE CLAUSE for filters
  UPDATE tmp_filter_data
  	SET where_clause = REPLACE( REPLACE( LOWER( wco.clause ), '<<filter_value>>', tmp_filter_data.filter_value ), '<<filter_to_value>>', tmp_filter_data.filter_to_value )
  FROM global.report_where_clause_operators wco
 	 WHERE
    	LOWER( wco.type ) = tmp_filter_data.condition_type
    	AND LOWER( wco.operator ) = tmp_filter_data.condition_operator
    	AND filter_name IS NOT NULL;
		
  UPDATE tmp_filter_data
  	SET where_clause = REPLACE( where_clause,E'\'','' )
 	 WHERE
    	filter_name = 'campaign_date';
		
  --Flag invalid Filters, so they can be ignored
  UPDATE tmp_filter_data
  	SET invalid_filter = TRUE
  	WHERE
    	filter_name IS NOT NULL
    	AND where_clause IS NULL;
 --Create Table to hold data from json combined with data from our setup tables
 --Date Unification	
	DROP TABLE IF EXISTS date_unification;
	DROP INDEX IF EXISTS tmp_tbl_anchor_idx;
	DROP TABLE IF EXISTS tbl_anchor;
	
	var_temp_tbl = 'CREATE TEMPORARY TABLE tbl_anchor(tbl_anchor_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
													campaign_id BIGINT,market_id BIGINT);

												  CREATE INDEX tmp_tbl_anchor_idx ON tbl_anchor USING BTREE (campaign_id);';

   var_inset_temp_tbl = 'INSERT INTO tbl_anchor(campaign_id,market_id)
  						 SELECT tc.campaign_id,cm.market_id FROM global.get_user_campaign_access_list( ' || parm_user_id || ', ''' || parm_tenant_schema_name || ''' )tc
  						                                           LEFT JOIN campaign_markets cm on tc.campaign_id = cm.campaign_id AND cm.type::TEXT = ''TrafficCampaignMarket''::TEXT;';
																  

  --************CREATING GLOBAL TEMPORARY DATE UNIFICATION****************************
  EXECUTE var_temp_tbl;
  EXECUTE var_inset_temp_tbl;	
																															   
  --Sort Order
	DROP TABLE IF EXISTS sort_order_list;

	CREATE TEMPORARY TABLE sort_order_list
	(
		id SMALLSERIAL PRIMARY KEY,
		sort_order int,
		sort_by varchar,
		sort_dir VARCHAR
	);

	INSERT INTO sort_order_list
    ( sort_order, sort_by, sort_dir )
	SELECT
		s.sort_order AS sort_order,
		LOWER( s.sort_by ) AS sort_by,
		LOWER( s.sort_dir ) AS sort_dir    
	FROM
		( SELECT parm_query_def ->'sort_options' AS sort_options ) so,
			JSON_TO_RECORDSET( so.sort_options ) AS s ( sort_order INT, sort_dir VARCHAR, sort_by VARCHAR )
		ORDER BY sort_order;

  --***********************************************************************************
  --**********                Build blocks for Dynamic Query                 **********
  --***********************************************************************************
    
  --***********************************************************************************
  --**********                     BUILD SELECT CLAUSE                       **********
  --***********************************************************************************
 
   SELECT
      '  SELECT DISTINCT ' || E'\n              ' || STRING_AGG( field_source || ' AS ' || report_field_alias || E'\n', '              , ' )
      INTO var_select
    FROM
      global.vw_report_mappings vrm
	  INNER JOIN select_column_list sl ON sl.select_column_name = vrm.report_field_name
    WHERE
      report_type_id = parm_report_type_id;
	  
	  --raise notice 'Line 236 - var_select build: %', var_select;


  --***********************************************************************************
  --**********                       BUILD FROM CLAUSE                       **********
  --***********************************************************************************
	UPDATE select_column_list SET  condition_name = 'already' where select_column_name = 'date';

	IF EXISTS(select * from tmp_filter_data WHERE condition_name = 'months')  
	AND NOT EXISTS (SELECT 1 FROM select_column_list WHERE select_column_name = 'date') THEN
	BEGIN
	  INSERT INTO select_column_list (select_column_name)
	  VALUES ('date');
	END;	   
	END IF;	  

	IF EXISTS(select * from tmp_filter_data WHERE condition_name = 'station_id') THEN
	BEGIN
	  INSERT INTO select_column_list (select_column_name)
	  VALUES ('station_id');
	END;	   
	END IF;	   
	   
	IF EXISTS(select * from tmp_filter_data WHERE condition_name = 'salesperson_id') THEN
	BEGIN
	  INSERT INTO select_column_list (select_column_name)
	  VALUES ('salesperson_id');
	END;	   
	END IF;	  

	DROP TABLE IF EXISTS tmp_report_snippets;
    
    CREATE TABLE tmp_report_snippets AS
    SELECT DISTINCT
      REPLACE( REPLACE( '              ' || rjs.snippet, ' ON ', E'\n' || '                ON '), ' AND ', E'\n' || '                  AND ' ) AS snippet,
      MIN(vrjsh.snippet_hierarchy_id) AS lvl
 
	FROM
      GLOBAL.vw_report_mappings vrm
	  INNER JOIN select_column_list sl ON sl.select_column_name = vrm.report_field_name OR sl.select_column_name = vrm.report_field_alias
	  INNER JOIN GLOBAL.vw_report_join_snippet_hierarchy vrjsh
        ON vrm.join_snippet_id = vrjsh.report_join_snippet_id
      INNER JOIN global.report_join_snippets rjs
        ON vrjsh.snippet_hierarchy_id = rjs.id
   LEFT JOIN tmp_filter_data tfd ON vrm.report_field_name = tfd.condition_name		

	WHERE vrm.report_type_id = parm_report_type_id
    
    GROUP BY
      rjs.snippet
    
      ORDER BY
	 lvl,
      snippet;

	 var_from := '  FROM' || E'\n' ||  ( SELECT STRING_AGG( trs.snippet, E'\n' ORDER BY trs.lvl, trs.snippet) FROM tmp_report_snippets as trs );
	 
	 var_parm_calender_types := (SELECT QUOTE_LITERAL(REPLACE(filter_value,''', ''',',')) AS filter_value  FROM tmp_filter_data
								 								WHERE condition_name = 'calendar_types');
	 var_parm_months := (SELECT QUOTE_LITERAL(REPLACE(filter_value,''', ''',',')) FROM tmp_filter_data WHERE condition_name = 'months');
	 
	 var_parm_cal_types := (SELECT REPLACE(filter_value,''', ''',',') AS filter_value  FROM tmp_filter_data
								 								WHERE condition_name = 'calendar_types');
	 var_prm_months := (SELECT REPLACE(filter_value,''', ''',',') FROM tmp_filter_data WHERE condition_name = 'months');

	 var_application_id := var_application_call_id;
	 var_from := REPLACE( var_from, '<<tenant_name>>', var_msp_tenant_schema_name );
	 var_from := REPLACE( var_from, '<<var_application_id>>', var_application_id::varchar);
	 var_from := REPLACE( var_from, '<<months>>',  COALESCE(var_parm_months,'' )); 	
	 var_from := REPLACE( var_from, '<<calendar_types>>',  COALESCE(var_parm_calender_types,'') );
	 var_from := REPLACE( var_from, '<<projection_date>>',  COALESCE(var_projection_date,'') );
	 var_from := REPLACE( var_from, '<<clp_date>>',  
				COALESCE((SELECT  ' AND clp.projection_date BETWEEN '|| E'\'' || filter_value || E'\'' || ' AND ' || E'\''|| filter_to_value ||E'\'' 
				FROM tmp_filter_data 
					WHERE condition_name = 'contract_start_date' limit 1),'<<clp_date>>') );					
									
--     raise notice 'Line 309 - var_application_id: %', var_application_id; 
-- 	raise notice 'Line 309 - var_parm_calender_types: %', var_parm_calender_types; 
-- 	raise notice 'Line 310 - var_parm_months: %', var_parm_months; 
				  
   DROP TABLE IF EXISTS tmp_billing_data;	 
	 CREATE TEMPORARY TABLE tmp_billing_data
  (
    id SMALLSERIAL PRIMARY KEY,
    year INTEGER,
    month VARCHAR,
    start_date date,
    end_date date	  
  ); 
  --***********************************************************************************
  --**********                       BUILD WHERE CLAUSE                      **********
  --***********************************************************************************
  --select * from tmp_filter_data;
    IF EXISTS( SELECT * FROM tmp_filter_data WHERE filter_name IS NOT NULL and invalid_filter = false )
      THEN
        var_where := '   WHERE ' ||
            (
            SELECT '( ' || string_agg( tfd3.condition_list, ' AND
             ') ||' )'
            FROM (
                  SELECT '( ' || string_agg(tfd2.filter, ' ' || tfd2.filter_operator || ' ') || ' )' as condition_list
                  FROM (SELECT distinct tfd.filter_name, tfd.filter_field || ' ' || tfd.where_clause as filter, tfd.filter_operator 
                        FROM tmp_filter_data tfd 
                        WHERE tfd.filter_name IS NOT NULL AND tfd.filter_name NOT IN ('contract_start_date')) tfd2
                  GROUP BY tfd2.filter_name
               ) as tfd3
             );
    END IF;
	  
	IF EXISTS( SELECT * FROM tmp_filter_data WHERE condition_name = 'months' ) THEN
      BEGIN
	  
		INSERT INTO tmp_billing_data(year,month,start_date,end_date)
		SELECT 	year,
				month,
				start_date,
				end_date 
		FROM global.fn_billing_dates_sb(var_parm_cal_types,var_prm_months,var_application_id);
		
     	var_where := COALESCE(var_where||' AND ',' WHERE ')||
		(SELECT COALESCE(' ( ' || 
				STRING_AGG(' cal.day  between '''|| start_date ||''' AND '''|| end_date ||'''',' OR ')|| ' )'
				,'') 
			FROM tmp_billing_data ) || E'\n';
		
		var_where := COALESCE(var_where||' AND ',' WHERE ')|| 'cl.type IS NOT NULL' ||E'\n';

		var_from := REPLACE( var_from, '<<clp_date>>',  
				(SELECT	COALESCE( ' AND ( ' || 
				STRING_AGG(' clp.projection_date  between '''|| start_date ||''' AND '''|| end_date ||'''',' OR ')|| ' )','') 
				FROM tmp_billing_data) );

		
	  END;
	ELSE
      BEGIN
	  
       var_where := COALESCE(var_where||' AND ',' WHERE ')|| 'cl.type IS NOT NULL' ||E'\n';
	   
	  END;
	END IF;	  
  
	DELETE FROM select_column_list 
		WHERE select_column_name in ('station_id','salesperson_id','date') 
		  and condition_name IS NULL;

  --raise notice 'Line 236 - var_where: %', var_where;
  --***********************************************************************************
  --**********                     BUILD GROUP BY CLAUSE                     **********
  --***********************************************************************************
    var_group_by := ( SELECT DISTINCT 'GROUP BY ' || STRING_AGG( rfs.field_source, ',' ) AS group_by_values
               FROM select_column_list sl
                 INNER JOIN global.vw_report_mappings vrm
                   ON SL.select_column_name = vrm.report_field_name
                     AND vrm.is_aggregation_column = 'FALSE' AND vrm.report_type_id = parm_report_type_id
 				INNER JOIN global.report_field_sources rfs ON rfs.id = vrm.field_source_id
 					 AND rfs.sort_field_is_aggregate = 'FALSE'
               ORDER BY group_by_values ) ;


    
    --To replace the dynamic tenant string in the subqueries in field mapping
    var_select := REPLACE( var_select, '<<tenant_name>>', var_msp_tenant_schema_name );

	--Creating TEMPORARY Table For Date Based On Filters
	DROP TABLE IF EXISTS tbl_calendars;
	CREATE TEMPORARY TABLE tbl_calendars (day DATE, week_in_year INT, quarter_in_year INT, year INT);
	
	INSERT INTO tbl_calendars(day, week_in_year, quarter_in_year, year)
	SELECT 	day, 
			week_in_year, 
			quarter_in_year, 
			year
	FROM global.calendars 
		WHERE day BETWEEN ( SELECT filter_value::DATE FROM tmp_filter_data 
							WHERE filter_name = 'contract_start_date') AND 
						  (	SELECT filter_to_value::DATE FROM tmp_filter_data
							WHERE filter_name = 'contract_start_date') AND type = 'Gregorian'
	UNION
	SELECT 	day, 
			week_in_year, 
			quarter_in_year, 
			cal.year
	FROM global.calendars cal
	JOIN tmp_billing_data tbd ON cal.day BETWEEN tbd.start_date AND tbd.end_date
		WHERE cal.type = 'Gregorian';
  --**************************************************************************************************
  --**********   COMBINE all the building blocks of the dynamic sql statement and execute   **********
  --**************************************************************************************************
/*
  --For debugging purposes - should be commented out when not debugging
 	  raise notice 'Line 309 - var_parm_calender_types: %', var_parm_calender_types; 
	  raise notice 'Line 310 - var_parm_months: %', var_parm_months;
 	  raise notice 'Line 236 - var_select: %', var_select;
	  raise notice 'Line 236 - var_from: %', var_from;
	  raise notice 'Line 236 - var_group_by: %', var_group_by;
	  raise notice 'Line 236 - var_order_by: %', var_order_by;
	  raise notice 'Line 236 - var_where: %', var_where;
 */

  var_dynSQL = ' SELECT JSON_AGG( agg ) '|| E'\n' ||
  'FROM ( ' || E'\n' ||
  var_select || E'\n' ||
  var_from || E'\n' ||
  var_where || E'\n' ||
  var_group_by || E'\n' ||      
  '  LIMIT ' || parm_perPage || E'\n' ||
  '  OFFSET ' || var_page::VARCHAR || E'\n' ||
  ' ) AS agg;';


  --For debugging purposes - should be commented out when not debugging
  --raise notice 'Line 581 - var_dynSQL: %', var_dynSQL;

 PERFORM global.fn_report_log_execution_history(
    parm_execution_history_id := var_execution_history_id,
    parm_dynamic_query_string := var_dynSQL
  );

  EXECUTE var_dynSQL INTO var_dynSQL_result;

  PERFORM global.fn_report_log_execution_history(
    parm_execution_history_id := var_execution_history_id
  );  
  

  RETURN var_dynSQL_result;

END
$BODY$;

ALTER FUNCTION global.fn_contracted_report( character varying, bigint, json, bigint, integer, CHARACTER VARYING ) OWNER TO marketron_db;
