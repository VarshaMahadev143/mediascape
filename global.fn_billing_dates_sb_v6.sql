--DROP FUNCTION IF EXISTS global.fn_billing_dates( CHARACTER VARYING, CHARACTER VARYING, JSON ); --<-- Old function definition
/*
SELECT * FROM global.fn_billing_dates ('standard_calendar', '2021_April,2021_May,2021_June', '{"application_call_id":999999999999,"select_column_list":[],"filter_options":[],"sort_options":[]}' );
SELECT * FROM global.fn_billing_dates ('broadcast_calendar','2021_April,2021_May,2021_June', '{"application_call_id":999999999999,"select_column_list": [],"filter_options": [],"sort_options": []}' );
SELECT * FROM global.fn_billing_dates ('standard_calendar,broadcast_calendar','2021_April,2021_May,2021_June', '{"application_call_id":999999999999,"select_column_list": [],"filter_options": [],"sort_options": []}' );
SELECT * FROM global.fn_billing_dates_sb ('standard_calendar,broadcast_calendar', '2021_April,2021_May,2021_June', '999999999999' );
SELECT * FROM global.fn_billing_dates_sb ('broadcast_calendar','2021_January,2021_February,2021_June', '999999999999' );
SELECT * FROM global.fn_billing_dates_sb ('standard_calendar','2021_January,2021_February,2021_June', '999999999999' );
select * from tmp_calender_types
select * from tmp_months

*/;
DROP FUNCTION IF EXISTS global.fn_billing_dates_sb( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING );

CREATE OR REPLACE FUNCTION global.fn_billing_dates_sb
(
	parm_calender_types CHARACTER VARYING,
	parm_months CHARACTER VARYING,
	parm_application_call_id CHARACTER VARYING
)
RETURNS TABLE
(
  year INTEGER,
  month CHARACTER VARYING,
  start_date DATE,
  end_date DATE
) 
LANGUAGE 'plpgsql'
AS $BODY$

DECLARE 
  r RECORD;
  var_year INT;
  var_month VARCHAR;
  var_type INT;
  var_prm_type VARCHAR;
  var_cal_type VARCHAR;
  var_dynSQL VARCHAR;

  var_function_name VARCHAR := 'fn_billing_dates';
  var_function_call_text TEXT;
  var_execution_history_id INT;

BEGIN       
  --Create new instance of execution history logging
    var_function_call_text = 'SELECT * FROM ' || var_function_name || '( parm_calender_types := ''' ||  parm_calender_types || ''', parm_months := ''' ||  parm_months || ''', parm_application_call_id := ''' || parm_application_call_id || ''' );';
  
    SELECT global.fn_report_log_execution_history
    (
      parm_object_called := var_function_name,
      parm_function_application_id := parm_application_call_id,
      parm_function_call_text := var_function_call_text
    )
    INTO var_execution_history_id;


  --Split calendar types from the passed parameter parm_calender_types
  --select * from tmp_calender_types
    DROP TABLE IF EXISTS tmp_calender_types;
  
    CREATE TEMPORARY TABLE tmp_calender_types
    (
      id SERIAL,
      calender_type VARCHAR,
      type VARCHAR
    );
  
    INSERT INTO tmp_calender_types
      ( calender_type, type )
    SELECT
      TRIM( ct ),
      CASE WHEN TRIM( ct ) = 'standard_calendar'
        THEN 'Gregorian'
        ELSE 'Broadcast'
      END AS type
    FROM
      regexp_split_to_table( parm_calender_types, ',' ) AS ct;


  --Split months from the passed parameter parm_months
    DROP TABLE IF EXISTS tmp_months;
    
    CREATE TEMPORARY TABLE tmp_months
    (
      id SERIAL,
      yr_mth VARCHAR,
      year INT,
      month VARCHAR
    );
    
    INSERT INTO tmp_months
      ( yr_mth, year, month )
    SELECT
      TRIM( ym ) AS yr_mth,
      LEFT( ym, 4):: INT AS year,
      RIGHT( ym, CHAR_LENGTH( ym ) - 5 ) AS month
    FROM
      regexp_split_to_table( parm_months, ',' ) AS ym;


  --Determine the start and end dates for the calendar types and months provided
    DROP TABLE IF EXISTS tmp_results;
    CREATE TEMPORARY TABLE tmp_results
  	(
      id SERIAL,
      year INT,
      month VARCHAR,
      start_date DATE,
      end_date DATE,
      month_in_year INT
  	); 
  
    INSERT INTO tmp_results
      ( year, month, start_date, end_date, month_in_year )
    SELECT
      m.year,
      m.month,
      MIN( c.day ) AS start_date,
      MAX( c.day ) AS end_date,
      c.month_in_year
    FROM
      tmp_calender_types ct
      CROSS JOIN tmp_months m
      JOIN global.calendars c
        ON ct.type = c.type
          AND m.year = c.year
          AND m.month = c.month_in_year_long
    GROUP BY
      m.year,
      m.month,
      c.month_in_year;


  --Update execution history logging
    PERFORM global.fn_report_log_execution_history(
      parm_execution_history_id := var_execution_history_id
    );  


  --Return results 
    RETURN QUERY EXECUTE 'SELECT year, month, start_date, end_date FROM tmp_results ORDER BY year, month_in_year;';   
   
END;
$BODY$;

ALTER FUNCTION global.fn_billing_dates_sb( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING ) OWNER TO marketron_db;
