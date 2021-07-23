/*
Name: global.report_seed_mapping_tables.sql
Author: Jon Melsa
Co-Author: 
Date: 02/13/2021

Description:  The reporting functions dynamically creates a query based on the criteria it recieves in a json parameter.

  The json parameter coorelate to information that is defines in the tables and/or view created below.

  More detailed descriptions of each table and view are listed below.

Architecture:

Revisions:
  Date: 03/18/2021
  Author: Jon Melsa
  Change: ( AC-4498 ) Update place holder fields to acutal database field references for: Date Contracted, Program Buy,
    L/N/R, User Field 1, User Field 2, User Field 3

  Date: 04/08/2021
  Author: Jon Melsa
  Change: ( AC-4884 ) DB - DSR - Introduce Log # column in NXT report
    ( AC-4231 ) DB - DSR - Introduce Scheduled Date, Scheduled Time and Voice Name to the dataset and Remove Buy Description 

  Date:
  Author:
  Change: 
*/

--/*
  --If it is necessary to rebuild the table and view, then execute this statments first:
  DROP VIEW IF EXISTS global.vw_report_mappings;
  DROP VIEW IF EXISTS global.vw_report_join_snippet_hierarchy;


  DROP TABLE IF EXISTS global.report_type_anchor_report_join_snippets;
  DROP TABLE IF EXISTS global.report_field_aliases;
  DROP TABLE IF EXISTS global.report_field_sources;
  DROP TABLE IF EXISTS global.report_join_snippets;
  DROP TABLE IF EXISTS global.report_where_clause_operators;
  --DROP TABLE IF EXISTS global.report_execution_history;
  DROP TABLE IF EXISTS global.report_spot_states;
  DROP TABLE IF EXISTS global.report_ad_type_mapping;
  DROP TABLE IF EXISTS global.report_dynamic_group_by_columns;
  DROP TABLE IF EXISTS global.dim_date;
--*/

------------------------------------
--Create report mapping tables needed for dynamic query generation
CREATE TABLE IF NOT EXISTS global.report_where_clause_operators
( 
  id SMALLSERIAL
    CONSTRAINT pk_report_where_clause_operators PRIMARY KEY,
  type TEXT NOT NULL,
  operator TEXT NOT NULL,
  clause TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  modified_at TIMESTAMPTZ NULL,
  CONSTRAINT uq_report_where_clause_operators UNIQUE ( type, operator )
);


CREATE TABLE IF NOT EXISTS global.report_join_snippets
( 
	id BIGINT
		CONSTRAINT pk_report_join_snippets PRIMARY KEY,
  source_table VARCHAR NOT NULL,
  snippet TEXT NOT NULL,
  snippet_hierarchy BIGINT[] NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  modified_at TIMESTAMPTZ NULL,
  CONSTRAINT uq_rjs UNIQUE ( snippet )
);
 
  
CREATE TABLE IF NOT EXISTS global.report_field_sources
( 
	id BIGINT
		CONSTRAINT pk_report_field_sources PRIMARY KEY,
  field_source TEXT NOT NULL,
  where_clause_type TEXT NOT NULL,
  sort_field TEXT NULL,
  sort_join_required BOOLEAN NOT NULL DEFAULT TRUE,
  sort_field_is_aggregate BOOLEAN NOT NULL DEFAULT FALSE,
  filter_field TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  modified_at TIMESTAMPTZ NULL,
  CONSTRAINT uq_rfs_field_source UNIQUE ( field_source )
);


CREATE TABLE IF NOT EXISTS global.report_field_aliases
( 
	id BIGSERIAL NOT NULL
		CONSTRAINT pk_report_field_aliases PRIMARY KEY,
  report_field_name TEXT NULL,
  report_field_alias TEXT NOT NULL,
  report_field_source_id BIGINT NOT NULL,
  report_join_snippet_id BIGINT NOT NULL,
  is_group_column BOOLEAN NOT NULL DEFAULT FALSE,
  is_aggregation_column BOOLEAN NOT NULL DEFAULT FALSE,
  report_type_id BIGINT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  notes VARCHAR NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  modified_at TIMESTAMPTZ NULL,
  CONSTRAINT uq_rfa UNIQUE ( report_type_id, report_field_name, report_field_alias ),
  CONSTRAINT fk_rfa_to_rfs FOREIGN KEY ( report_field_source_id ) REFERENCES global.report_field_sources ( id ),
  CONSTRAINT fk_rfa_to_rt FOREIGN KEY ( report_type_id ) REFERENCES global.report_types ( id ),
  CONSTRAINT fk_rfa_to_rjs FOREIGN KEY ( report_join_snippet_id ) REFERENCES global.report_join_snippets ( id )
);


CREATE TABLE IF NOT EXISTS global.report_type_anchor_report_join_snippets
( 
	id BIGSERIAL
		CONSTRAINT pk_report_type_anchor_report_join_snippets PRIMARY KEY,
  report_type_id BIGINT NOT NULL,
  report_join_snippet_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  modified_at TIMESTAMPTZ NULL,
  CONSTRAINT uq_rtarjs UNIQUE ( report_type_id ),
  CONSTRAINT fk_rtarjs_to_rt FOREIGN KEY ( report_type_id ) REFERENCES global.report_types ( id ),
  CONSTRAINT fk_rtarjs_to_rjs FOREIGN KEY ( report_join_snippet_id ) REFERENCES global.report_join_snippets ( id )
);


CREATE TABLE IF NOT EXISTS global.report_execution_history ( 
  id BIGINT GENERATED ALWAYS AS IDENTITY
    CONSTRAINT report_execution_history_pkey PRIMARY KEY,
  object_called VARCHAR( 255 ) NOT NULL,
  tenant_schema_name VARCHAR( 255 ) NULL,
  application_call_id VARCHAR( 100 ) NULL,
  user_id BIGINT NULL,
  parmeters_passed VARCHAR NULL,
  function_call_text TEXT NULL,
  utc_execution TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  dynamic_query_string TEXT NULL,
  utc_dynamic_query_built_time TIMESTAMPTZ NULL,
  utc_dynamic_query_executed_time TIMESTAMPTZ NULL
);


CREATE TABLE IF NOT EXISTS global.report_spot_states
(
  id INT
    CONSTRAINT report_spot_states_pkey PRIMARY KEY,
  name VARCHAR( 100 )
);


------------------------------------
--Populate filter tables
--ALTER TABLE GLOBAL.report_types RENAME COLUMN mlc_link TO options;
--ALTER TABLE GLOBAL.report_types ALTER COLUMN options TYPE TEXT;

INSERT INTO global.report_types
  ( id, name, report_type, created_at, updated_at,
  description, options )
VALUES
  ( 5, 'Daily Spot', 'Reports::DailySpotReport', '2021-02-01 22:43:15.659513', '2021-02-01 22:43:15.659513', 'Radio daily spot report', '{"mlc_link":"/","ui_base_report_url":"/portal/daily-spot"}' ),
  ( 6, 'Cross Revenue', 'Reports::CrossRevenueReport', '2021-03-22 11:20:01.887530', '2021-03-31 06:03:31.714264', 'Cross revenue report', '{"mlc_link":"/","ui_base_report_url":"/portal/cross-revenue"}' ),
  ( 7, 'Contracted', 'Reports::ContractedReport', '2021-04-27 04:58:44.662468', '2021-04-27 04:58:44.662468', 'Contracted report', '{"mlc_link":"/","ui_base_report_url":"/portal/contracted"}' )
ON CONFLICT ( id )
  DO NOTHING;


/*
The where_clause_operator table contains the actual operators and single-quotes needed for the filtering.  The actual value of the
filter, say 'Ford Auto Mall', is placed in clause by doing a Replace command with the '<<filter_value>>' string.  Some of the 
operators have a range setup so the json's filter_to_value element would replace the '<<filter_to_value>>' string.
*/
INSERT INTO global.report_where_clause_operators
  ( type, operator, clause )
VALUES
 ( 'date', 'equals', '= ''<<filter_value>>''' ),
 ( 'date', 'notEqual', '<> ''<<filter_value>>''' ),
 ( 'date', 'in', 'IN ( ''<<filter_value>>'' )' ),
 ( 'date', 'lessThan', '< ''<<filter_value>>''' ),
 ( 'date', 'lessThanOrEqual', '<= ''<<filter_value>>''' ),
 ( 'date', 'greaterThan', '> ''<<filter_value>>''' ),
 ( 'date', 'greaterThanOrEqual', '>= ''<<filter_value>>''' ),
 ( 'date', 'range', 'BETWEEN ''<<filter_value>>'' AND ''<<filter_to_value>>''' ),
 ( 'datetime', 'equals', '= ''<<filter_value>>''' ),
 ( 'datetime', 'notEqual', '<> ''<<filter_value>>''' ),
 ( 'datetime', 'in', 'IN ( ''<<filter_value>>'' )' ),
 ( 'datetime', 'lessThan', '< ''<<filter_value>>''' ),
 ( 'datetime', 'lessThanOrEqual', '<= ''<<filter_value>>''' ),
 ( 'datetime', 'greaterThan', '> ''<<filter_value>>''' ),
 ( 'datetime', 'greaterThanOrEqual', '>= ''<<filter_value>>''' ),
 ( 'datetime', 'range', 'BETWEEN ''<<filter_value>>'' AND ''<<filter_to_value>>''' ), 
 ( 'text', 'equals', '= ''<<filter_value>>''' ),
 ( 'text', 'notEqual', '<> ''<<filter_value>>''' ),
 ( 'text', 'in', 'IN ( ''<<filter_value>>'' )' ),
 ( 'text', 'contains', 'ilike ''%<<filter_value>>%''' ),
 ( 'text', 'notContains', 'not ilike ''%<<filter_value>>%''' ),
 ( 'text', 'startsWith', 'ilike ''<<filter_value>>%''' ),
 ( 'text', 'endsWith', 'ilike ''%<<filter_value>>''' ),
 ( 'number', 'equals', '= <<filter_value>>' ),
 ( 'number', 'notEqual', '<> <<filter_value>>' ),
 ( 'number', 'lessThan', '< <<filter_value>>' ),
 ( 'number', 'lessThanOrEqual', '<= <<filter_value>>' ),
 ( 'number', 'greaterThan', '> <<filter_value>>' ),
 ( 'number', 'greaterThanOrEqual', '>= <<filter_value>>' ),
 ( 'number', 'range', 'BETWEEN <<filter_value>> and <<filter_to_value>>' ),
 ( 'number', 'in', 'IN ( <<filter_value>> )' ),
 ( 'boolean', 'equals', '= <<filter_value>>' )
ON CONFLICT ON CONSTRAINT uq_report_where_clause_operators
DO UPDATE
  SET
    clause = EXCLUDED.clause,
    modified_at = NOW()
  WHERE
    report_where_clause_operators.clause != EXCLUDED.clause;


/*
The report_join_snippets table contains a single join clause for each table that might need to be accessed.  
The functions gets a list of joins, from global.vw_report_join_snippet_hierarchy, of all the joins necessary to 
access all the fields referenced in the json.  Then duplicate joins are removed before dynamically build the FROM clause.
For example, the campaign_markets table may be referenced multiple times for different fields but it 
only needs to appear once in the dynamic FROM statement being created.

Note:  The dynamic statement needs the joins to be in the right order.  Each Join can only
reference a table that is already listed in the From clause.  For that reason, the joins below have
an snippet_hierachy array that provides the necessary joins in the appropiate order.
*/ 
INSERT INTO global.report_join_snippets
  ( id, source_table, snippet, snippet_hierarchy )
VALUES
  ( -4, 'anchor', 'tmp_dsr_anchor_data AS anchor', ARRAY[-4] ),
  ( -3, 'anc', 'tbl_anchor as anc', ARRAY[-3] ),
  ( -2, 'anchor', 'date_unification as anchor', ARRAY[-2] ),
  ( -1, 'unknown', 'Unknown AS tbd', ARRAY[-1] ),
  ( 1, 'campaigns', 'JOIN campaigns AS c ON c.id = anchor.campaign_id', ARRAY[-4, 1] ),
  ( 2, 'campaign_lines', 'JOIN campaign_lines AS cl ON cl.campaign_id = c.id', ARRAY[-4,1,2] ),
  ( 3, 'campaign_markets', 'JOIN campaign_markets AS cm ON cm.campaign_id = c.id AND cm.type::TEXT = ''TrafficCampaignMarket''::TEXT', ARRAY[-4,1,3] ),
  ( 4, 'advertisers', 'JOIN advertisers AS adv ON adv.id = c.advertiser_id', ARRAY[-4,1,4] ),
  ( 5, 'agencies', 'LEFT JOIN agencies AS agy ON agy.id = c.agency_id', ARRAY[-4,1,5] ),
  ( 6, 'accounts', 'JOIN accounts AS ac ON ac.id = cm.account_id AND ac.marked_for_delete IS NOT TRUE', ARRAY[-4,1,3,6] ),
  --( 7, 'spots', 'JOIN spots AS s ON s.campaign_line_id = cl.id', ARRAY[-4,1,2,7] ),
  ( 8, 'traffic_line_details', 'JOIN traffic_line_details AS tld ON tld.traffic_line_id = cl.id', ARRAY[-4,1,2,8] ),
  ( 9, 'campaign_market_sales_reps', 'LEFT JOIN campaign_market_sales_reps AS cmsr ON cmsr.campaign_market_id = cm.id AND cmsr.is_primary = TRUE', ARRAY[-4,1,3,9] ),
  ( 10, 'salespersons', 'LEFT JOIN salespersons AS sp ON sp.id = cmsr.salesperson_id', ARRAY[-4,1,3,9,10] ),
  ( 11, 'traffic_users', 'LEFT JOIN traffic_users AS tu ON tu.salesperson_id = sp.id', ARRAY[-4,1,3,9,10,11] ),
  ( 12, 'global.sales_regions', 'LEFT JOIN global.sales_regions AS sr ON sr.id = tu.market_salesrep_location_id', ARRAY[-4,1,3,9,10,11,12] ),
  ( 13, 'tenant_competitive_types', 'LEFT JOIN tenant_competitive_types AS ptct ON ptct.id = anchor.primary_competitive_type_id', ARRAY[-4,1,2,13] ),
  ( 14, 'competitive_type_markets', 'LEFT JOIN competitive_type_markets AS pctm ON pctm.tenant_competitive_type_id = ptct.id AND pctm.market_id = cm.market_id AND pctm.marked_for_delete IS NOT TRUE', ARRAY[-4,1,2,3,13,14] ),
  ( 15, 'tenant_competitive_types', 'LEFT JOIN tenant_competitive_types AS stct ON stct.id = anchor.secondary_competitive_type_id', ARRAY[-4,1,2,15] ),
  ( 16, 'competitive_type_markets', 'LEFT JOIN competitive_type_markets AS sctm ON sctm.tenant_competitive_type_id = stct.id AND sctm.market_id = cm.market_id AND sctm.marked_for_delete IS NOT TRUE', ARRAY[-4,1,2,3,15,16] ),
  ( 17, 'outlets', 'LEFT JOIN outlets AS o ON o.id = anchor.outlet_id', ARRAY[-4,1,2,17] ),
  ( 18, 'outlet_groups', 'LEFT JOIN outlet_groups AS og ON og.outlet_id = o.id', ARRAY[-4,1,2,17,18] ),
  ( 19, 'groups', 'LEFT JOIN groups AS g ON g.id = og.group_id AND g.type::TEXT = ''Market''::TEXT', ARRAY[-4,1,2,17,18,19] ),
  ( 20, 'tenant_revenue_types', 'LEFT JOIN tenant_revenue_types AS trt ON trt.id = c.revenue_type_id', ARRAY[-4,1,20] ),
  ( 21, 'tenant_sponsorships', 'LEFT JOIN tenant_sponsorships AS ts ON ts.id = tld.sponsorship_id', ARRAY[-4,1,2,8,21] ),
  ( 22, 'traffic_campaign_market_details', 'JOIN traffic_campaign_market_details tcmd ON cm.id = tcmd.traffic_campaign_market_id', ARRAY[-4,1,3,22] ),
  ( 23, 'program_markets', 'LEFT JOIN program_markets AS pm ON anchor.tenant_program_id = pm.tenant_program_id AND pm.market_id = cm.market_id', ARRAY[-4,1,2,3,23] ),
  ( 24, 'market_salesrep_locations', 'LEFT JOIN market_salesrep_locations msl ON c.market_salesrep_location_id = msl.id', ARRAY[-4,1,24] ),
  ( 25, 'groups', 'JOIN groups g ON cm.market_id = g.id', ARRAY[-4,1,3,25] ),
  ( 26, 'report_spot_states', 'JOIN global.report_spot_states rss ON anchor.state = rss.id', ARRAY[-4,1,2,26] ),
  ( 27, 'market advertisers', 'JOIN advertisers AS mkt_adv ON mkt_adv.id = ac.advertiser_id', ARRAY[-4,1,3,6,27] ),

--Contracted
  ( 7000, 'campaigns', 'JOIN campaigns AS c on c.id = anc.campaign_id', ARRAY[-3,7000,7003,7007] ),
  ( 7001, 'credit_approved_by','LEFT JOIN LATERAL ( 
              SELECT id,apr_users FROM (			   
				SELECT u.id,ROW_NUMBER()OVER (PARTITION BY ca.campaign_id ORDER BY ca.created_at DESC) as rnm,''Approved ( '' || string_agg( u.last_name || '' '' || u.first_name, ''; '' ORDER BY u.last_name ) || '' )'' as apr_users
					 FROM campaign_approvals ca                                
						JOIN users u ON u.id = ca.user_id
							WHERE ca.campaign_id = c.id 
								AND ca.archived = false
								AND ca.status = ''approved''
								AND ca.approval_type =''credit_approval''                               
              GROUP BY ca.campaign_id,u.id,ca.created_at) crd WHERE crd.rnm = 1	
            ) as ca_apr ON TRUE', ARRAY[-3,7000,7001,7007,7003] ),
  ( 7002, 'campaign_markets', 'LEFT JOIN campaign_markets AS cm ON cm.campaign_id = c.id AND cm.type::TEXT = ''TrafficCampaignMarket''::TEXT', ARRAY[-3,7000,7002,7007,7003] ),
  ( 7003, 'calendars','JOIN tbl_calendars cal ON cal.day BETWEEN c.start_date AND c.end_date', ARRAY[-3,7000,7007,7003] ),
  ( 7007, 'campaign_lines', 'LEFT JOIN campaign_lines AS cl ON cl.campaign_id = c.id 
						AND cl.type in (''TrafficLine'' , ''AlternativeRevenueLine'')', ARRAY[-3,7000,7007] ),
  ( 7008, 'outlets', 'LEFT JOIN outlets AS olet ON olet.id = cl.outlet_id', ARRAY[-3,7000,7007,7008,7003] ),
  ( 7009, 'calendars','LEFT JOIN global.global_billing_cycles AS bcal
							ON c.billing_cycle_id = bcal.id', ARRAY[-3,7000,7009,7003] ),
  ( 7016, 'market_salesrep_locations', 'LEFT JOIN market_salesrep_locations msl
  				ON c.market_salesrep_location_id = msl.id', ARRAY[-3,7000,7007,7016,7003] ),
  ( 7020, 'tenant_competitive_types', 'LEFT JOIN tenant_competitive_types AS ptct
  			ON ptct.id = c.competitive_type_id', ARRAY[-3,7000,7007,7020,7003] ),
  ( 7025, 'competitive_type_markets','	LEFT JOIN competitive_type_markets AS pct
  			ON pct.tenant_competitive_type_id = ptct.id
  			AND pct.market_id = cm.market_id
  			AND pct.marked_for_delete IS NOT TRUE', ARRAY[-3,7000,7007,7002,7020,7025,7003] ),
  ( 7023, 'tenant_competitive_types', 'LEFT JOIN tenant_competitive_types AS stct
  			ON stct.id = c.secondary_competitive_type_id', ARRAY[-3,7000,7007,7023,7003] ),
  ( 7026,	'competitive_type_markets', 'LEFT JOIN competitive_type_markets AS sct
  			ON sct.tenant_competitive_type_id = stct.id
  			AND sct.market_id = cm.market_id
  			AND sct.marked_for_delete IS NOT TRUE', ARRAY[-3,7000,7007,7002,7023,7026,7003] ),
  ( 7029, 'campaign_line_projections', 'LEFT JOIN campaign_line_projections clp
  							ON cl.id = clp.campaign_line_id
  							AND clp.type IN (''TrafficProjection'',''AlternativeRevenueProjection'') 
   							AND clp.projection_date = cal.day <<clp_date>>', ARRAY[-3,7000,7007,7029,7003] ),
  ( 7039, 'spots', 'LEFT JOIN LATERAL(SELECT SUM( CASE WHEN spt.price = 0.0 THEN 1 ELSE 0 END) AS no_charge_spot,
						 		spt.campaign_line_id
						 FROM spots AS spt 
						 WHERE spt.campaign_line_id = cl.id 
							AND cal.day = spt.start_date_time::DATE
						 GROUP BY spt.campaign_line_id)spt ON spt.campaign_line_id = cl.id', ARRAY[-3,7000,7007,7039,7003] ),
  ( 7042, 'campaign_market_sales_reps', 'LEFT JOIN campaign_market_sales_reps AS cms ON cms.campaign_market_id = cm.id AND cms.is_primary = TRUE', ARRAY[-3,7000,7007,7045,7042,7003] ),										
  ( 7044, 'agencies', ' LEFT JOIN agencies AS agy ON agy.id = c.agency_id', ARRAY[-3,7000,7007,7044,7003] ),							
  ( 7045, 'advertisers', 'LEFT JOIN advertisers AS adv ON adv.id = c.advertiser_id', ARRAY[-3,7000,7045,7003] ),
  ( 7046, 'traffic_campaign_market_details', 'LEFT JOIN traffic_campaign_market_details tcmd ON cm.id = tcmd.traffic_campaign_market_id', ARRAY[-3,7000,7007,7002,7029,7046,7003] ),
  ( 7047, 'salespersons', 'LEFT JOIN salespersons AS sps ON sps.id = cms.salesperson_id', ARRAY[-3,7000,7002,7042,7047,7003] ),
  ( 7048, 'traffic_users', 'LEFT JOIN traffic_users AS tus ON tus.salesperson_id = sps.id AND cm.id = tus.market_id', ARRAY[-3,7000,7007,7002,7042,7047,7048,7003] ),							
  ( 7050, 'advertiser_credit_restriction', 'LEFT JOIN global.fn_cr_advertiser_credit_restriction (''<<tenant_name>>'',''<<var_application_id>>'' ) advcr
  							ON advcr.campaign_id = c.id', ARRAY[-3,7000,7007,7050,7003] ),
  ( 7051, 'agency_credit_restriction', 'LEFT JOIN global.fn_cr_agency_credit_restriction(''<<tenant_name>>'',''<<var_application_id>>'' ) agycr
  							ON agycr.campaign_id = c.id', ARRAY[-3,7000,7007,7051,7003] ),
  ( 7055, 'tenant_revenue_types', 'LEFT JOIN tenant_revenue_types AS trtyp ON trtyp.id = c.revenue_type_id', ARRAY[-3,7000,7007,7055,7003] ),
  ( 7057, 'revenue_categories', 'LEFT JOIN revenue_categories rc
                             ON trtyp.revenue_category_id = rc.id', ARRAY[-3,7000,7055,7002,7059,7057,7003] ),
  ( 7058, 'traffic_line_details', 'LEFT JOIN traffic_line_details AS tld ON tld.traffic_line_id = spt.campaign_line_id', ARRAY[-3,7000,7039,7058,7003] ),
  ( 7059, 'revenue_type_markets', 'LEFT JOIN revenue_type_markets AS rtm 
  					ON rtm.tenant_revenue_type_id = trtyp.id AND rtm.market_id = cm.market_id AND rtm.marked_for_delete IS NOT TRUE', ARRAY[-3,7000,7002,7007,7058,7059,7003] ),
  ( 7066, 'global.fn_billing_dates', 'LEFT JOIN global.fn_billing_dates_sb (<<calendar_types>>,<<months>>,''<<var_application_id>>'') bld
									ON bld.start_date = cal.day
									bld.month = cal.month_in_year_long
									AND bld.year = cal.year', ARRAY[-3,7000,7007,7003,7066] ),
  ( 7068, 'groups', 'LEFT JOIN groups AS gcr ON anc.market_id = gcr.id', ARRAY[-3,7000,7007,7003,7068] )
ON CONFLICT ( id )
DO UPDATE
  SET
    source_table = EXCLUDED.source_table,
    snippet = EXCLUDED.snippet,
    snippet_hierarchy = EXCLUDED.snippet_hierarchy,
    modified_at = NOW()
  WHERE report_join_snippets.snippet <> EXCLUDED.snippet
    OR report_join_snippets.source_table <> EXCLUDED.source_table
    OR report_join_snippets.snippet_hierarchy <> EXCLUDED.snippet_hierarchy;


/*
The report_field_sources table maps the actual fields in the database.
It also defines how sorts and filters for these fields are to be handled.
*/
INSERT INTO global.report_field_sources
  ( id, field_source, where_clause_type, sort_field, sort_join_required,
    sort_field_is_aggregate, filter_field )
VALUES
  ( -8, '1::INTEGER', 'number', '', TRUE, FALSE, '' ),
  ( -7, 'NULL::VARCHAR', 'text', '', TRUE, FALSE, '' ),
  ( -6, 'NULL::TIMESTAMPTZ', 'datetime', '', TRUE, FALSE, '' ),
  ( -5, 'NULL::DATE', 'date', '', TRUE, FALSE, '' ),
  ( -4, 'NULL::TIME', 'datetime', '', TRUE, FALSE, '' ),
  ( -3, 'NULL::INTEGER', 'number', '', TRUE, FALSE, '' ),
  ( -2, 'NULL::BIGINT', 'number', '', TRUE, FALSE, '' ),
  ( -1, 'NULL::BOOLEAN', 'boolean', '', TRUE, FALSE, '' ),
  ( 1, 'adv.id', 'number', 'adv.id <<sort direct>>', TRUE, FALSE, 'adv.id' ),
  ( 2, 'adv.name', 'text', 'adv.name <<sort direct>>', TRUE, FALSE, 'adv.name' ),
  ( 3, 'agy.id', 'number', 'agy.id <<sort direct>>', TRUE, FALSE, 'agy.id' ),
  ( 4, 'agy.name', 'text', 'agy.name <<sort direct>>', TRUE, FALSE, 'agy.name' ),
  ( 5, 'anchor.start_date', 'date', 'anchor.start_date_time::DATE <<sort direct>>', TRUE, FALSE, 'anchor.start_date_time::DATE' ),
  ( 6, 'anchor.start_time', 'datetime', 'anchor.start_date_time::TIME <<sort direct>>', TRUE, FALSE, 'anchor.start_date_time::TIME' ),
  ( 7, 'cm.contract_number', 'text', 'cm.contract_number <<sort direct>>', TRUE, FALSE, 'cm.contract_number' ),
  ( 8, 'c.end_date', 'date', 'c.end_date <<sort direct>>', TRUE, FALSE, 'c.end_date' ),
  ( 9, 'tld.pattern_type', 'text', 'tld.pattern_type <<sort direct>>', TRUE, FALSE, 'tld.pattern_type' ),
  ( 10, 'tld.monday', 'number', 'tld.monday <<sort direct>>', TRUE, FALSE, 'tld.monday' ),
  ( 11, 'tld.tuesday', 'number', 'tld.tuesday <<sort direct>>', TRUE, FALSE, 'tld.tuesday' ),
  ( 12, 'tld.wednesday', 'number', 'tld.wednesday <<sort direct>>', TRUE, FALSE, 'tld.wednesday' ),
  ( 13, 'tld.thursday', 'number', 'tld.thursday <<sort direct>>', TRUE, FALSE, 'tld.thursday' ),
  ( 14, 'tld.friday', 'number', 'tld.friday <<sort direct>>', TRUE, FALSE, 'tld.friday' ),
  ( 15, 'tld.saturday', 'number', 'tld.saturday <<sort direct>>', TRUE, FALSE, 'tld.saturday' ),
  ( 16, 'tld.sunday', 'number', 'tld.sunday <<sort direct>>', TRUE, FALSE, 'tld.sunday' ),
  ( 17, 'tld.spots_per_week', 'number', 'tld.spots_per_week <<sort direct>>', TRUE, FALSE, 'tld.spots_per_week' ),
  ( 18, 'tld.spot_rate', 'number', 'tld.spot_rate <<sort direct>>', TRUE, FALSE, 'tld.spot_rate' ),
  ( 19, 'anchor.price', 'number', 'anchor.price <<sort direct>>', TRUE, FALSE, 'anchor.price' ),
  ( 20, 'anchor.state', 'number', 'anchor.state <<sort direct>>', TRUE, FALSE, 'anchor.state' ),
  ( 21, 'anchor.isci', 'text', 'anchor.isci <<sort direct>>', TRUE, FALSE, 'anchor.isci' ),
  ( 22, 'LEFT( msl.location_type, 1 )', 'text', 'LEFT( msl.location_type, 1 ) <<sort direct>>', TRUE, FALSE, 'LEFT( msl.location_type, 1 )' ),
  ( 23, 'tld.spot_length', 'number', 'tld.spot_length <<sort direct>>', TRUE, FALSE, 'tld.spot_length' ),
  ( 24, 'cl.line_number', 'number', 'cl.line_number <<sort direct>>', TRUE, FALSE, 'cl.line_number' ),
  ( 25, 'cl.line_remark', 'text', 'cl.line_remark <<sort direct>>', TRUE, FALSE, 'cl.line_remark' ),
  ( 26, 'cl.end_date', 'date', 'cl.end_date <<sort direct>>', TRUE, FALSE, 'cl.end_date' ),
  ( 27, 'tld.line_end_time', 'number', 'tld.line_end_time <<sort direct>>', TRUE, FALSE, 'tld.line_end_time' ),
  ( 28, 'cl.start_date', 'date', 'cl.start_date <<sort direct>>', TRUE, FALSE, 'cl.start_date' ),
  ( 29, 'tld.line_start_time', 'number', 'tld.line_start_time <<sort direct>>', TRUE, FALSE, 'tld.line_start_time' ),
  ( 30, 'agy.media_buying_service', 'boolean', 'agy.media_buying_service <<sort direct>>', TRUE, FALSE, 'agy.media_buying_service' ),
  ( 31, 'anchor.cart', 'text', 'anchor.cart <<sort direct>>', TRUE, FALSE, 'anchor.cart' ),
  ( 32, 'anchor.primary_competitive_type_id', 'number', 'anchor.primary_competitive_type_id <<sort direct>>', TRUE, FALSE, 'anchor.primary_competitive_type_id' ),
  ( 33, 'pctm.name', 'text', 'pctm.name <<sort direct>>', TRUE, FALSE, 'pctm.name' ),
  ( 34, 'tld.priority', 'number', 'tld.priority <<sort direct>>', TRUE, FALSE, 'tld.priority' ),
  ( 35, 'anchor.product', 'text', 'anchor.product <<sort direct>>', TRUE, FALSE, 'anchor.product' ),
  ( 36, 'trt.id', 'number', 'trt.id <<sort direct>>', TRUE, FALSE, 'trt.id' ),
  ( 37, 'trt.name', 'text', 'trt.name <<sort direct>>', TRUE, FALSE, 'trt.name' ),
  ( 38, 'sp.id', 'number', 'sp.id <<sort direct>>', TRUE, FALSE, 'sp.id' ),
  ( 39, 'sp.first_name', 'text', 'sp.first_name <<sort direct>>', TRUE, FALSE, 'sp.first_name' ),
  ( 40, 'sp.last_name', 'text', 'sp.last_name <<sort direct>>', TRUE, FALSE, 'sp.last_name' ),
  ( 41, 'anchor.secondary_competitive_type_id', 'number', 'anchor.secondary_competitive_type_id <<sort direct>>', TRUE, FALSE, 'anchor.secondary_competitive_type_id' ),
  ( 42, 'sctm.name', 'text', 'sctm.name <<sort direct>>', TRUE, FALSE, 'sctm.name' ),
  ( 43, 'ts.id', 'number', 'ts.id <<sort direct>>', TRUE, FALSE, 'ts.id' ),
  ( 44, 'ts.name', 'text', 'ts.name <<sort direct>>', TRUE, FALSE, 'ts.name' ),
  ( 45, 'o.name', 'text', 'o.name <<sort direct>>', TRUE, FALSE, 'o.name' ),
  ( 46, 'o.id', 'number', 'o.id <<sort direct>>', TRUE, FALSE, 'o.id' ),
  ( 47, 'sp.first_name || '' '' || sp.last_name', 'text', 'sp.first_name || '' '' || sp.last_name <<sort direct>>', TRUE, FALSE, 'sp.first_name || '' '' || sp.last_name' ),
  ( 48, 'g.name', 'text', 'g.name <<sort direct>>', TRUE, FALSE, 'g.name' ),
  ( 49, 'g.id', 'number', 'g.id <<sort direct>>', TRUE, FALSE, 'g.id' ),
  ( 50, 'c.custom_column_1', 'text', 'c.custom_column_1 <<sort direct>>', TRUE, FALSE, 'c.custom_column_1' ),
  ( 51, 'c.custom_column_2', 'text', 'c.custom_column_2 <<sort direct>>', TRUE, FALSE, 'c.custom_column_2' ),
  ( 52, 'c.custom_column_3', 'text', 'c.custom_column_3 <<sort direct>>', TRUE, FALSE, 'c.custom_column_3' ),
  ( 53, 'tcmd.created_at', 'datetime', 'tcmd.created_at <<sort direct>>', TRUE, FALSE, 'tcmd.created_at' ),
  ( 54, 'COALESCE( pm.name, ''Time Buy'' )', 'text', 'COALESCE( pm.name, ''Time Buy'' ) <<sort direct>>', TRUE, FALSE, 'COALESCE( pm.name, ''Time Buy'' )' ),
  ( 55, 'tld.alternate_log_number', 'number', 'tld.alternate_log_number <<sort direct>>', TRUE, FALSE, 'tld.alternate_log_number' ),
  ( 56, 'anchor.voice_name', 'text', 'anchor.voice_name <<sort direct>>', TRUE, FALSE, 'anchor.voice_name' ),
  ( 57, 'anchor.scheduled_date', 'date', 'anchor.scheduled_date_time::DATE <<sort direct>>', TRUE, FALSE, 'anchor.scheduled_date_time::DATE' ),
  ( 58, 'anchor.scheduled_time', 'datetime', 'anchor.scheduled_date_time::TIME <<sort direct>>', TRUE, FALSE, 'anchor.scheduled_date_time::TIME' ),
  ( 59, 'rss.name', 'text', 'rss.name <<sort direct>>', TRUE, FALSE, 'rss.name' ),
  ( 60, 'mkt_adv.id', 'number', 'mkt_adv.id <<sort direct>>', TRUE, FALSE, 'mkt_adv.id' ),
  ( 61, 'mkt_adv.name', 'text', 'mkt_adv.name <<sort direct>>', TRUE, FALSE, 'mkt_adv.name' ),

--Contracted
  ( 7038, 'c.start_date', 'date', 'c.start_date <<sort direct>>', TRUE, FALSE, 'c.start_date' ),
  ( 7003, 'ca_apr.id', 'number','ca_apr.id <<sort direct>>' , TRUE, FALSE, 'ca_apr.id' ),
  ( 7004, 'COALESCE( ca_apr.apr_users,''Unapproved'')', 'text','ca_apr.apr_users <<sort direct>>' , TRUE, FALSE, 'ca_apr.apr_users' ),
  ( 7005, 'tcmd.product_name', 'text', 'tcmd.product_name <<sort direct>>', TRUE, FALSE, 'tcmd.product_name' ),
  ( 7006, 'msl.id', 'number', 'msl.id <<sort direct>>', TRUE, FALSE, 'msl.id' ),
  ( 7008, '( sps.first_name || '' '' || sps.last_name )', 'text', 'Salesperson <<sort direct>>', TRUE, FALSE, '( sps.first_name || '' '' || sps.last_name )' ),
  ( 7010, 'cal.week_in_year', 'number', 'cal.week_in_year <<sort direct>>', TRUE, FALSE, 'cal.week_in_year' ),
  ( 7011, '''Q''||cal.quarter_in_year', 'number', 'cal.quarter_in_year <<sort direct>>', TRUE, FALSE, 'cal.quarter_in_year' ),
  ( 7012, 'c.billing_cycle_id', 'number', 'c.billing_cycle_id <<sort direct>>', TRUE, FALSE, 'c.billing_cycle_id' ),
  ( 7013, 'bcal.name', 'text', 'bcal.name <<sort direct>>', TRUE, FALSE, 'bcal.name' ),
  ( 7015, 'trtyp.name', 'text', 'trtyp.name <<sort direct>>', TRUE, FALSE, 'trtyp.name' ),
  ( 7017, 'msl.location_type', 'text', 'msl.location_type <<sort direct>>', TRUE, FALSE, 'msl.location_type' ),
  ( 7018, 'CASE WHEN cl.type = ''AlternativeRevenueLine'' THEN 2 WHEN cl.type = ''TrafficLine'' THEN 1
				 ELSE 99  END ', 'text', 'cl.type <<sort direct>>', TRUE, FALSE, 'cl.type' ),
  ( 7019, 'CASE WHEN cl.type = ''AlternativeRevenueLine'' THEN ''Alternative Revenue''  WHEN cl.type = ''TrafficLine'' THEN ''Airtime''
					ELSE cl.type
				END', 'text', 'cl.type <<sort direct>>', TRUE, FALSE, 'cl.type' ),
  ( 7021, 'c.competitive_type_id', 'number', 'c.competitive_type_id <<sort direct>>', TRUE, FALSE, 'c.competitive_type_id' ),
  ( 7027, 'pct.name', 'text', 'pct.name <<sort direct>>', TRUE, FALSE, 'pct.name' ),
  ( 7024, 'c.secondary_competitive_type_id', 'number', 'c.secondary_competitive_type_id <<sort direct>>', TRUE, FALSE, 'c.secondary_competitive_type_id' ),
  ( 7028, 'sct.name', 'text', 'sct.name <<sort direct>>', TRUE, FALSE, 'sct.name' ),
  ( 7030, 'SUM( COALESCE( clp.amount,0 ) * COALESCE( tcmd.agency_commission,0 ) )', 'number', '', TRUE, TRUE, '' ),
  ( 7031, 'SUM( COALESCE( clp.amount,0 ) ) FILTER(WHERE clp.type IN (''TrafficProjection''))', 'number', '', TRUE, TRUE, '' ),
  ( 7032, 'SUM( COALESCE( clp.count,0 ) )', 'number', '', TRUE, TRUE, '' ),
  ( 7043, 'anc.market_id', 'number', 'anc.market_id <<sort direct>>', TRUE, FALSE, 'anc.market_id' ),  
  ( 7033, 'MAX( tld.spot_rate )', 'number', '', TRUE, TRUE, '' ),
  ( 7034, 'MIN( tld.spot_rate )', 'number', '', TRUE, TRUE, '' ),
  ( 7035, 'SUM( COALESCE( clp.amount,0 ) - COALESCE( clp.amount * tcmd.agency_commission,0 ) ) FILTER(WHERE clp.type IN (''TrafficProjection''))', 'number', '', TRUE, TRUE, '' ),
  ( 7036, 'tcmd.airtime_commission', 'number', 'tcmd.airtime_commission <<sort direct>>', TRUE, FALSE, 'tcmd.airtime_commission' ),
  ( 7037, 'SUM( spt.no_charge_spot )', 'number', 'SUM( spt.no_charge_spot ) <<sort direct>>', TRUE, TRUE, '' ),
  ( 7040, 'advcr.advertiser_credit_restriction', 'text', 'advertiser_credit_restriction <<sort direct>>', TRUE, FALSE, 'advcr.advertiser_credit_restriction' ),
  ( 7041, 'agycr.agency_credit_restriction', 'text', 'agycr.agency_credit_restriction <<sort direct>>', TRUE, FALSE, 'agycr.agency_credit_restriction' ),
  ( 7049, 'cal.day', 'date', 'cal.day <<sort direct>>', TRUE, FALSE, 'cal.day' ),
  ( 7053, 'olet.name', 'text', 'olet.name <<sort direct>>', TRUE, FALSE, 'olet.name' ),
  ( 7054, 'olet.id', 'number', 'olet.id <<sort direct>>', TRUE, FALSE, 'olet.id' ),
  ( 7056, 'trtyp.id', 'number', 'trtyp.id <<sort direct>>', TRUE, FALSE, 'trtyp.id' ),
  ( 7060, 'rc.id', 'rc.id', 'rc.id <<sort direct>>', TRUE, FALSE, 'rc.id' ),
  ( 7061, 'rc.name', 'text', 'rc.name <<sort direct>>', TRUE, FALSE, 'rc.name' ),
  ( 7063, 'SUM( COALESCE( clp.amount,0 ) ) FILTER(WHERE clp.type IN (''AlternativeRevenueProjection''))', 'number', '', TRUE, TRUE, '' ),
  ( 7065, 'cal.year', 'text', 'cal.year <<sort direct>>', TRUE, FALSE, 'cal.year' ),
  ( 7067, 'sps.id', 'text', 'sps.id <<sort direct>>', TRUE, FALSE, 'sps.id' ),
  ( 7069, 'gcr.name', 'text', 'gcr.name <<sort direct>>', TRUE, FALSE, 'gcr.name' ),
  --( 7070, 'SUM( COALESCE(clp.amount,0) )', 'number', '', TRUE, TRUE, '' ),
  --( 7071, 'SUM( COALESCE(clp.amount,0) - COALESCE(tcmd.airtime_commission,0) )', 'number', '', TRUE, TRUE, '' ),
  ( 7072, 'msl.name', 'text', 'msl.name <<sort direct>>', TRUE, FALSE, 'msl.name' ),
  ( 7073, 'TO_CHAR(cal.day, ''Mon'')', 'text', 'TO_CHAR(cal.day, ''Mon'') <<sort direct>>', TRUE, FALSE, 'TO_CHAR(cal.day, ''Mon'')' )

ON CONFLICT ( id )
DO UPDATE
  SET
    field_source = EXCLUDED.field_source,
    where_clause_type = EXCLUDED.where_clause_type,
    sort_field = EXCLUDED.sort_field,
    sort_join_required = EXCLUDED.sort_join_required,
    sort_field_is_aggregate = EXCLUDED.sort_field_is_aggregate,
    filter_field = EXCLUDED.filter_field,
    modified_at = NOW()
  WHERE
    report_field_sources.field_source <> EXCLUDED.field_source
    OR report_field_sources.where_clause_type <> EXCLUDED.where_clause_type
    OR report_field_sources.sort_field <> EXCLUDED.sort_field
    OR report_field_sources.sort_join_required <> EXCLUDED.sort_join_required
    OR report_field_sources.sort_field_is_aggregate <> EXCLUDED.sort_field_is_aggregate
    OR report_field_sources.filter_field <> EXCLUDED.filter_field;


/*
The report_field_aliases table maps the way that the reports utilize the actual database data points.
It also house addtional metadata that is utilized to provide back the correct data.  These addtional
metadata columns are report level settings.

The value in the report_field_alias column is the value we expect to receive from the 
json parameter.
*/
INSERT INTO global.report_field_aliases
	( report_field_name, report_field_alias, report_field_source_id, report_join_snippet_id, is_group_column,
    is_aggregation_column, report_type_id, is_active, notes )
VALUES
  ( '', 'advertiser_id', 60, 27, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'advertiser_name', 61, 27, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'agency_id', 3, 5, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'agency_name', 4, 5, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_air_date', 5, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_air_time', 6, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'buy_description', -7, -1, TRUE, FALSE, 5, FALSE, 'Removed as part of AC-4231, Sprint 195' ),
  ( '', 'contract_number', 7, 3, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'contract_end_date', 8, 1, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'date_contracted', 53, 22, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'pattern_type', 9, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'monday', 10, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'tuesday', 11, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'wednesday', 12, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'thursday', 13, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'friday', 14, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'saturday', 15, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'sunday', 16, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spots_per_week', 17, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'isci', 21, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'lrn', 22, 24, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'len', 23, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'line_number', 24, 2, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'line_remark', 25, 2, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'line_end_date', 26, 2, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'line_end_time', 27, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'line_start_date', 28, 2, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'line_start_time', 29, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'log_number', 55, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'market_id', 49, 25, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'market', 48, 25, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'mbs', 30, 5, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'media_number', 31, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'primary_competitive_type_id', 32, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'primary_competitive_type_name', 33, 14, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'priority', 34, 8, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'product', 35, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'program_buy', 54, 23, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'revenue_type_id', 36, 20, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'revenue_type_name', 37, 20, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'salesperson_id', 38, 10, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'salesperson', 47, 10, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_scheduled_date', 57, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_scheduled_time', 58, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'secondary_competitive_type_id', 41, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'secondary_competitive_type_name', 42, 16, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'sponsorship_id', 43, 21, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'sponsorship_name', 44, 21, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_count', -8, -1, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_price', 19, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'station_id', 46, 17, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'station', 45, 17, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'user_field_1', 50, 1, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'user_field_2', 51, 1, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'user_field_3', 52, 1, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'voice_talent', 56, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_state_id', 20, -4, TRUE, FALSE, 5, TRUE, NULL ),
  ( '', 'spot_state_name', 59, 26, TRUE, FALSE, 5, TRUE, NULL ),

  ( 'advertiser_name', 'advertiser_id', 1, 7045, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'advertiser_name', 'advertiser_name', 2, 7045, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'agency_name', 'agency_id', 3, 7044, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'agency_name', 'agency_name', 4, 7044, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'contract_start_date', 'contract_start_date', 7049, 7000, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'date_contracted', 'date_contracted', 7049, 7046, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'credit_approved_by_name', 'credit_approved_by_id', 7003, 7001, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'credit_approved_by_name', 'credit_approved_by_name', 7004, 7001, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'priority', 'priority', 34, 7058, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'product', 'product', 7005, 7046, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'sales_rep_office_location_name', 'sales_rep_office_location_id', 7006, 7016, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'sales_rep_office_location_name', 'sales_rep_office_location_name', 7072, 7016, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'salesperson', 'salesperson_id', 7067, 7047, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'salesperson', 'salesperson', 7008, 7047, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'station_name', 'station_id', 7054, 7008, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'station_name', 'station_name', 7053, 7008, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'user_field_1', 'user_field_1', 50, 7000, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'user_field_2', 'user_field_2', 51, 7000, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'user_field_3', 'user_field_3', 52, 7000, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'contract_start_week', 'contract_start_week', 7010, 7003, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'contract_start_quarter', 'contract_start_quarter', 7011, 7003, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'billing_cycle_name', 'billing_cycle_id', 7012, 7000, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'billing_cycle_name', 'billing_cycle_name', 7013, 7009, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'revenue_type_name', 'revenue_type_id', 7056, 7055, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'revenue_type_name', ' revenue_type_name', 7015, 7055, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'contract_number', ' contract_number', 7, 7002, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'lnr', ' lnr', 7017, 7016, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'line_type_name', 'line_type_id',7018, 7007, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'line_type_name', 'line_type_name', 7019, 7007, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'line_start_time', 'line_start_time',29, 7058, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'line_end_time', 'line_end_time', 27, 7058, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'month_in_year_short', 'month_in_year_short', 7073, 7003, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'primary_competitive_type_name', 'primary_competitive_type_id', 7021, 7020, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'primary_competitive_type_name', 'primary_competitive_type_name', 7027, 7025, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'secondary_competitive_type_name', 'secondary_competitive_type_id', 7024, 7023, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'secondary_competitive_type_name', 'secondary_competitive_type_name', 7028, 7026, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'agency_commission_dollars', 'agency_commission_dollars', 7030, 7046, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'alt_rev_gross_dollars', 'alt_rev_gross_dollars', 7063, 7029, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'average_unit_rate_gross', 'projected_airtime_gross_dollars', 7031, 7029, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'average_unit_rate_gross', 'spot_count', 7032, 7029, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'projected_gross_dollars', 'projected_gross_dollars', 7031, 7029, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'projected_high_rate', 'projected_high_rate', 7033, 7058, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'low_rate', 'low_rate', 7034, 7058, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'net_dollars', 'net_dollars', 7035, 7046, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'salesperson_commission_percent', 'salesperson_commission_percent', 7036, 7046, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'no_charge_spot', 'no_charge_spot', 7037, 7039, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'advertiser_credit_restriction', 'advertiser_credit_restriction', 7040, 7050, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'agency_credit_restriction', 'agency_credit_restriction', 7041, 7051, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'revenue_category_name', 'revenue_category_id', 7060, 7057, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'revenue_category_name', 'revenue_category_name', 7061, 7057, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'mbs', 'mbs', 30, 7044, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'market_name', 'market_id', 7043, -3, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'market_name', 'market_name', 7069, 7068, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'date', 'date', 7049, 7003, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'year', 'number', 7065, 7003, TRUE, FALSE, 7, TRUE, NULL ),
  ( 'projected_gross_percent', 'projected_gross_dollars', 7031, 7029, TRUE, TRUE, 7, TRUE, NULL ),
  ( 'net_percent', 'net_dollars', 7035, 7046, TRUE, TRUE, 7, TRUE, NULL )
  
ON CONFLICT ON CONSTRAINT uq_rfa
DO UPDATE
  SET 
  	report_field_source_id = EXCLUDED.report_field_source_id,
    report_join_snippet_id = EXCLUDED.report_join_snippet_id,
    is_group_column = EXCLUDED.is_group_column,
    is_aggregation_column = EXCLUDED.is_aggregation_column,
    is_active = EXCLUDED.is_active,
    notes = EXCLUDED.notes,
    modified_at = NOW()
  WHERE
    report_field_aliases.report_field_name <> EXCLUDED.report_field_name
    OR report_field_aliases.report_field_source_id <> EXCLUDED.report_field_source_id
    OR report_field_aliases.report_join_snippet_id <> EXCLUDED.report_join_snippet_id
    OR report_field_aliases.is_group_column <> EXCLUDED.is_group_column
    OR report_field_aliases.is_aggregation_column <> EXCLUDED.is_aggregation_column
    OR report_field_aliases.is_active <> EXCLUDED.is_active
    OR report_field_aliases.notes <> EXCLUDED.notes;


/*
The report_type_anchor_report_join_snippets table provides the anchor table needed for a particular report.
*/
INSERT INTO global.report_type_anchor_report_join_snippets
	( report_type_id, report_join_snippet_id )
VALUES
  ( 5, -4 ),
  ( 6, -2 ),
  ( 7, -3 )
ON CONFLICT ON CONSTRAINT uq_rtarjs
DO UPDATE
  SET
    report_join_snippet_id = EXCLUDED.report_join_snippet_id,
    modified_at = NOW()
  WHERE
    report_type_anchor_report_join_snippets.report_join_snippet_id <> EXCLUDED.report_join_snippet_id;


/*
The report_spot_states table provides spot state descriptions.
*/
INSERT INTO global.report_spot_states
  ( id, name )
VALUES
  ( 0, 'New' ),
  ( 1, 'Bumped' ),
  ( 2, 'Scheduled' ),
  ( 3, 'Missed' ),
  ( 4, 'Canceled' ),
  ( 5, 'Deleted' ),
  ( 6, 'Aired' ),
  ( 7, 'Posted' ),
  ( 8, 'Invoiced' ),
  ( 9, 'Hold' ),
  ( 10, 'Preempted' )
ON CONFLICT ( id )
DO UPDATE
  SET
    name = EXCLUDED.name
  WHERE
    report_spot_states.name <> EXCLUDED.name;


--######################################################################################################################
--#####   Table: global.report_ad_type_mapping                                                                     #####
--#####   Note: This is a shared table, so don't drop and recreate                                                 #####
--######################################################################################################################
CREATE TABLE IF NOT EXISTS global.report_ad_type_mapping
( 
	id SMALLINT NOT NULL
    CONSTRAINT pk_report_ad_type_mapping PRIMARY KEY,
	ad_type VARCHAR( 25 ) NOT NULL,
	mapped_parent_ad_type VARCHAR( 25 ) NOT NULL,
  campaign_line_type VARCHAR( 255 ) NOT NULL,
	media_type_id smallint NOT NULL,
  CONSTRAINT uq_report_ad_type_mapping UNIQUE ( ad_type )
 );

--Populate the table global.report_ad_type_mapping
INSERT INTO global.report_ad_type_mapping
  ( id, ad_type, mapped_parent_ad_type, campaign_line_type, media_type_id )
VALUES
  ( 1, 'Airtime', 'Airtime', 'TrafficLine', 5 ),
  ( 2, 'Connected TV', 'Airtime', 'TrafficLine', 106 ),
  ( 3, 'Display', 'Digital', 'DigitalLine', 1 ), --?
  ( 4, 'Video', 'Digital', 'DigitalLine', 2 ),
  ( 5, 'Geofencing', 'Digital', 'DigitalLine', 3 ),
  ( 6, 'Video Geofencing', 'Digital', 'DigitalLine', 4 ),
  ( 7, 'OTT', 'Digital', 'DigitalLine', 6 ),
  ( 8, 'AltRev', 'Other', 'AlternativeRevenueLine', 101 ),
  ( 9, 'Stream Adswizz', 'Other', 'StreamingLine', 103 ),
  ( 10, 'Stream Triton', 'Other', 'TritonLine', 102 ),
  ( 11, 'Third Party', 'Other', 'Other', 104 ),
  ( 12, 'O&O', 'Other', 'DisplayLine', 105 )
ON CONFLICT ON CONSTRAINT uq_report_ad_type_mapping
DO UPDATE
  SET mapped_parent_ad_type = EXCLUDED.mapped_parent_ad_type,
    campaign_line_type = EXCLUDED.campaign_line_type,
    media_type_id = EXCLUDED.media_type_id
  WHERE
    report_ad_type_mapping.mapped_parent_ad_type <> EXCLUDED.mapped_parent_ad_type
    OR report_ad_type_mapping.campaign_line_type <> EXCLUDED.campaign_line_type
    OR report_ad_type_mapping.media_type_id <> EXCLUDED.media_type_id;


--############################################################################
--#####   Table: global.report_dynamic_group_by_columns                  #####
--#####   Note: This is a shared table, so don't drop and recreate       #####
--############################################################################
CREATE TABLE IF NOT EXISTS global.report_dynamic_group_by_columns
	( 
		pk_id SMALLSERIAL NOT NULL
		  CONSTRAINT pk_report_dynamic_group_by_columns PRIMARY KEY,
		id INT,
		field_mapping_name VARCHAR,
		select_names VARCHAR,
		app_passed_names VARCHAR,
		fn_values VARCHAR,
		aliases varchar,
		join_condition VARCHAR,
		fn_id smallint,
		report_type_id BIGINT NOT NULL,
    CONSTRAINT uq_rdgbc UNIQUE ( id, field_mapping_name, fn_id, report_type_id  ),
		CONSTRAINT fk_rdgbc_to_rt FOREIGN KEY ( report_type_id ) REFERENCES global.report_types ( id )
  );

--Populate the table global.report_dynamic_group_by_columns
INSERT INTO global.report_dynamic_group_by_columns
  ( id, field_mapping_name, join_condition, fn_id, report_type_id )
VALUES
  ( 3, 'market_name', '( ( drv_o.market_id = anchor.market_id or ( drv_o.market_id is null and anchor.market_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6 ), 
  ( 3, 'advertiser_name', '( ( drv_o.advertiser_id = anchor.advertiser_id or ( drv_o.advertiser_id is null and anchor.advertiser_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'station_name', '( ( drv_o.outlet_id = anchor.outlet_id or ( drv_o.outlet_id is null and anchor.outlet_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_line_ad_type_name', '( drv_o.media_type_id = atmp.media_type_id or ( drv_o.media_type_id is null and atmp.media_type_id is null ) )', 1, 6  ), 
  ( 3, 'order_line', '( ( drv_o.id = cl.id or ( drv_o.id is null and cl.id is null ) ) ) ', 1, 6  ), 
  ( 3, 'order_line_channel_type', '( drv_o.type = atmp.campaign_line_type or ( drv_o.type is null and atmp.campaign_line_type is null ) )', 1, 6  ), 
  ( 3, 'order_inventory_type', '( drv_o.tenant_inventory_type_id = teninvtyp.id or ( drv_o.tenant_inventory_type_id is null and teninvtyp.id is null ) )', 1, 6  ), 
  ( 3, 'order_inventory_category', '( drv_o.tenant_inventory_category_id = teninvcat.id or ( drv_o.tenant_inventory_category_id is null and teninvcat.id is null ) )', 1, 6  ), 
  ( 3, 'order_creation_date', '( ( drv_o.order_date = anchor.order_date or ( drv_o.order_date is null and anchor.order_date is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_creator_person_name', '( drv_o.owner_id = a.owner_id or ( drv_o.owner_id is null and a.owner_id is null ) )', 1, 6  ), 
  ( 3, 'order_state', '( ( drv_o.order_state = anchor.order_state or ( drv_o.order_state is null and anchor.order_state is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_name', '( ( drv_o.order_id = anchor.order_id or ( drv_o.order_id is null and anchor.order_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_opportunity_owner', '( ( drv_o.opportunity_owner_id = anchor.opportunity_owner_id or ( drv_o.opportunity_owner_id is null and anchor.opportunity_owner_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'proposal_status', '( ( drv_o.proposal_status = anchor.proposal_status or ( drv_o.proposal_status is null and anchor.proposal_status is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'has_parent_proposal', '( ( drv_o.has_parent_proposal = anchor.has_parent_proposal or ( drv_o.has_parent_proposal is null and anchor.has_parent_proposal is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_month_in_year_short', '( ( drv_o.order_month_in_year_short = anchor.order_month_in_year_short or( drv_o.order_month_in_year_short is null and anchor.order_month_in_year_short is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_year', '( ( drv_o.order_year = anchor.order_year or ( drv_o.order_year is null and anchor.order_year is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_quarter', '( ( drv_o.order_quarter = anchor.order_quarter or ( drv_o.order_quarter is null and anchor.order_quarter is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'order_week_in_year', '( ( drv_o.order_week_in_year = anchor.order_week_in_year or ( drv_o.order_week_in_year is null and anchor.order_week_in_year is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ), 
  ( 3, 'week_in_year', '( ( drv_o.week_in_year = anchor.week_in_year or ( drv_o.week_in_year is null and anchor.week_in_year is null ) ) )', 1, 6  ), 
  ( 3, 'month_in_year_short', '( ( drv_o.month_in_year_short = anchor.month_in_year_short or ( drv_o.month_in_year_short is null and anchor.month_in_year_short is null ) ) )', 1, 6  ), 
  ( 3, 'year', '( ( drv_o.year = anchor.year or ( drv_o.year is null and anchor.year is null ) ) )', 1, 6  ), 
  ( 3, 'quarter', '( ( drv_o.quarter = anchor.quarter or ( drv_o.quarter is null and anchor.quarter is null ) ) )', 1, 6  ),
  ( 3, 'date', '( ( drv_o.date = anchor.order_date or ( drv_o.date is null and anchor.order_date is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ),
  ( 3, 'order_source_name', '( ( drv_o.order_source_id = anchor.order_source_id or ( drv_o.order_source_id is null and anchor.order_source_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ),
  ( 3, 'competitive_category_name', '( ( drv_o.competitive_category_id = anchor.competitive_category_id or ( drv_o.competitive_category_id is null and anchor.competitive_category_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  ),
  ( 3, 'revenue_category_name', '( ( drv_o.revenue_category_id = anchor.revenue_category_id or ( drv_o.revenue_category_id is null and anchor.revenue_category_id is null ) ) AND anchor.order_proposal_fltr <> 1 )', 1, 6  )
ON CONFLICT ON CONSTRAINT uq_rdgbc
DO UPDATE
  SET join_condition = EXCLUDED.join_condition
  WHERE
    report_dynamic_group_by_columns.join_condition <> EXCLUDED.join_condition;


--###################################################################################################################
--#####   Table: global.dim_date                                                                                #####
--#####   Note: This table would hold the date dimension data                                                   #####
--#####   Note: This is a shared table, so don't drop and recreate                                              #####
--###################################################################################################################
CREATE TABLE IF NOT EXISTS global.dim_date
( 
  date_dim_id INT NOT NULL
    CONSTRAINT pk_dim_date PRIMARY KEY,
  date_actual DATE NOT NULL,
  epoch BIGINT NOT NULL,
  day_suffix VARCHAR(4) NOT NULL,
  day_name VARCHAR(9) NOT NULL,
  day_of_week INT NOT NULL,
  day_of_month INT NOT NULL,
  day_of_quarter INT NOT NULL,
  day_of_year INT NOT NULL,
  week_of_month INT NOT NULL,
  week_in_year SMALLINT NOT NULL,
  week_of_year_iso CHAR(10) NOT NULL,
  month_actual INT NOT NULL,
  month_name VARCHAR(9) NOT NULL,
  month_in_year_short VARCHAR NOT NULL,
  quarter SMALLINT NOT NULL,
  quarter_name VARCHAR(9) NOT NULL,
  year SMALLINT NOT NULL,
  first_day_of_week DATE NOT NULL,
  last_day_of_week DATE NOT NULL,
  first_day_of_month DATE NOT NULL,
  last_day_of_month DATE NOT NULL,
  first_day_of_quarter DATE NOT NULL,
  last_day_of_quarter DATE NOT NULL,
  first_day_of_year DATE NOT NULL,
  last_day_of_year DATE NOT NULL,
  mmyyyy CHAR(6) NOT NULL,
  mmddyyyy CHAR(10) NOT NULL,
  weekend_indr BOOLEAN NOT NULL,
  CONSTRAINT uq_dd UNIQUE ( date_actual )
);

CREATE INDEX IF NOT EXISTS ix_dim_date_date_actual
  ON global.dim_date( date_actual );

--Populate the table global.dim_date
INSERT INTO global.dim_date
  ( date_dim_id, date_actual, epoch, day_suffix, day_name,
    day_of_week, day_of_month, day_of_quarter, day_of_year, week_of_month,
    week_in_year, week_of_year_iso, month_actual, month_name, month_in_year_short,
    quarter, quarter_name, year, first_day_of_week, last_day_of_week,
    first_day_of_month, last_day_of_month, first_day_of_quarter, last_day_of_quarter, first_day_of_year,
    last_day_of_year, mmyyyy, mmddyyyy, weekend_indr )
SELECT
  TO_CHAR( datum, 'yyyymmdd' )::INT AS date_dim_id,
  datum AS date_actual,
  EXTRACT( EPOCH FROM datum ) AS epoch,
  TO_CHAR( datum, 'fmDDth' ) AS day_suffix,
  TO_CHAR( datum, 'Day' ) AS day_name,
  EXTRACT( ISODOW FROM datum ) AS day_of_week,
  EXTRACT( DAY FROM datum ) AS day_of_month,
  datum - DATE_TRUNC( 'quarter', datum )::DATE + 1 AS day_of_quarter,
  EXTRACT( DOY FROM datum ) AS day_of_year,
  TO_CHAR( datum, 'W' )::INT AS week_of_month,
  EXTRACT( WEEK FROM datum ) AS week_in_year,
  EXTRACT( ISOYEAR FROM datum ) || TO_CHAR( datum, '"-W"IW-' ) || EXTRACT( ISODOW FROM datum ) AS week_of_year_iso,
  EXTRACT( MONTH FROM datum ) AS month_actual,
  TO_CHAR( datum, 'Month' ) AS month_name,
  TO_CHAR( datum, 'Mon' ) AS month_in_year_short,
  EXTRACT( QUARTER FROM datum ) AS quarter,
  CASE
      WHEN EXTRACT( QUARTER FROM datum ) = 1 THEN 'First'
      WHEN EXTRACT( QUARTER FROM datum ) = 2 THEN 'Second'
      WHEN EXTRACT( QUARTER FROM datum ) = 3 THEN 'Third'
      WHEN EXTRACT( QUARTER FROM datum ) = 4 THEN 'Fourth'
      END AS quarter_name,
  EXTRACT( YEAR FROM datum ) AS year,
  datum + ( 1 - EXTRACT( ISODOW FROM datum ) )::INT AS first_day_of_week,
  datum + ( 7 - EXTRACT( ISODOW FROM datum ) )::INT AS last_day_of_week,
  datum + ( 1 - EXTRACT( DAY FROM datum ) )::INT AS first_day_of_month,
  ( DATE_TRUNC( 'MONTH', datum ) + INTERVAL '1 MONTH - 1 day' )::DATE AS last_day_of_month,
  DATE_TRUNC( 'quarter', datum )::DATE AS first_day_of_quarter,
  ( DATE_TRUNC( 'quarter', datum ) + INTERVAL '3 MONTH - 1 day' )::DATE AS last_day_of_quarter,
  TO_DATE( EXTRACT( YEAR FROM datum ) || '-01-01', 'YYYY-MM-DD' ) AS first_day_of_year,
  TO_DATE( EXTRACT( YEAR FROM datum ) || '-12-31', 'YYYY-MM-DD' ) AS last_day_of_year,
  TO_CHAR( datum, 'mmyyyy' ) AS mmyyyy,
  TO_CHAR( datum, 'mmddyyyy' ) AS mmddyyyy,
  CASE
      WHEN EXTRACT( ISODOW FROM datum ) IN ( 6, 7 ) THEN TRUE
      ELSE FALSE
      END AS weekend_indr
FROM ( 
      SELECT '2000-01-01'::DATE + SEQUENCE.DAY AS datum
        FROM GENERATE_SERIES( 0, 29219 ) AS SEQUENCE ( DAY )	--change to 36500, from 29219 -- Check for max date postgresql range
      GROUP BY SEQUENCE.DAY
      ) DQ
ON CONFLICT ON CONSTRAINT uq_dd
DO UPDATE
  SET epoch = EXCLUDED.epoch,
    day_suffix = EXCLUDED.day_suffix,
    day_name = EXCLUDED.day_name,
    day_of_week = EXCLUDED.day_of_week,
    day_of_month = EXCLUDED.day_of_month,
    day_of_quarter = EXCLUDED.day_of_quarter,
    day_of_year = EXCLUDED.day_of_year,
    week_of_month = EXCLUDED.week_of_month,
    week_in_year = EXCLUDED.week_in_year,
    week_of_year_iso = EXCLUDED.week_of_year_iso,
    month_actual = EXCLUDED.month_actual,
    month_name = EXCLUDED.month_name,
    month_in_year_short = EXCLUDED.month_in_year_short,
    quarter = EXCLUDED.quarter,
    quarter_name = EXCLUDED.quarter_name,
    year = EXCLUDED.year,
    first_day_of_week = EXCLUDED.first_day_of_week,
    last_day_of_week = EXCLUDED.last_day_of_week,
    first_day_of_month = EXCLUDED.first_day_of_month, 
    last_day_of_month = EXCLUDED.last_day_of_month,
    first_day_of_quarter = EXCLUDED.first_day_of_quarter,
    last_day_of_quarter = EXCLUDED.last_day_of_quarter,
    first_day_of_year = EXCLUDED.first_day_of_year,
    last_day_of_year = EXCLUDED.last_day_of_year,
    mmyyyy = EXCLUDED.mmyyyy,
    mmddyyyy = EXCLUDED.mmddyyyy,
    weekend_indr = EXCLUDED.weekend_indr
  WHERE
    dim_date.epoch <> EXCLUDED.epoch
    OR dim_date.day_suffix <> EXCLUDED.day_suffix
    OR dim_date.day_name <> EXCLUDED.day_name
    OR dim_date.day_of_week <> EXCLUDED.day_of_week
    OR dim_date.day_of_month <> EXCLUDED.day_of_month
    OR dim_date.day_of_quarter <> EXCLUDED.day_of_quarter
    OR dim_date.day_of_year <> EXCLUDED.day_of_year
    OR dim_date.week_of_month <> EXCLUDED.week_of_month
    OR dim_date.week_in_year <> EXCLUDED.week_in_year
    OR dim_date.week_of_year_iso <> EXCLUDED.week_of_year_iso
    OR dim_date.month_actual <> EXCLUDED.month_actual
    OR dim_date.month_name <> EXCLUDED.month_name
    OR dim_date.month_in_year_short <> EXCLUDED.month_in_year_short
    OR dim_date.quarter <> EXCLUDED.quarter
    OR dim_date.quarter_name <> EXCLUDED.quarter_name
    OR dim_date.year <> EXCLUDED.year
    OR dim_date.first_day_of_week <> EXCLUDED.first_day_of_week
    OR dim_date.last_day_of_week <> EXCLUDED.last_day_of_week
    OR dim_date.first_day_of_month <> EXCLUDED.first_day_of_month
    OR dim_date.last_day_of_month <> EXCLUDED.last_day_of_month
    OR dim_date.first_day_of_quarter <> EXCLUDED.first_day_of_quarter
    OR dim_date.last_day_of_quarter <> EXCLUDED.last_day_of_quarter
    OR dim_date.first_day_of_year <> EXCLUDED.first_day_of_year
    OR dim_date.last_day_of_year <> EXCLUDED.last_day_of_year
    OR dim_date.mmyyyy <> EXCLUDED.mmyyyy
    OR dim_date.mmddyyyy <> EXCLUDED.mmddyyyy
    OR dim_date.weekend_indr <> EXCLUDED.weekend_indr;



DROP VIEW IF EXISTS global.vw_report_mappings;

CREATE OR REPLACE VIEW global.vw_report_mappings
AS

/*
Name: global.vw_report_mappings
Author: Jon Melsa
Co-Author: Jon Melsa
Date: 01/26/2021

Description: This view is created to combine the contents of the above tables into a list.
  It simplifies the code in the Filter function and is an easy way to see how the data in
  the tables above is joined together.

  It will will allow for dynamic queries to be built for a particular report type.
*/

SELECT
  rt.id AS report_type_id,
  rt.name AS report_type,
  COALESCE( NULLIF( report_field_name, '' ), report_field_alias ) AS report_field_name,
  rfa.id AS report_field_alias_id,
  rfa.report_field_alias,
  rfa.report_join_snippet_id AS join_snippet_id,
  CASE WHEN rtarjs.id IS NOT NULL
    THEN TRUE::BOOLEAN
    ELSE FALSE::BOOLEAN
  END AS anchor_snippet,
  rfs.id AS field_source_id,
  rfs.field_source,
  rfs.where_clause_type,
  rfa.is_group_column,
  rfa.is_aggregation_column,
  rfs.sort_field,
  rfs.sort_join_required,
  rfs,sort_field_is_aggregate,
  rfs.filter_field
FROM
  global.report_field_sources rfs
  INNER JOIN global.report_field_aliases rfa
    ON rfa.report_field_source_id = rfs.id
  INNER JOIN global.report_types rt
    ON rt.id = rfa.report_type_id
  INNER JOIN global.report_join_snippets rjs
    ON rjs.id = rfa.report_join_snippet_id
  LEFT JOIN global.report_type_anchor_report_join_snippets rtarjs
    ON rtarjs.report_type_id = rt.id
      AND rtarjs.report_join_snippet_id = rjs.id
WHERE
  rfa.is_active = TRUE;


DROP VIEW IF EXISTS global.vw_report_join_snippet_hierarchy;

CREATE OR REPLACE VIEW global.vw_report_join_snippet_hierarchy
AS

/*
Name: global.vw_report_join_snippet_hierarchy
Author: Jon Melsa
Co-Author: Jon Melsa
Date: 01/26/2021

Description: This view will return the necessary join snippet ids in the correct order that
  are needed to support the requested field source.

  This will be utilized to dynamically build the FROM statement.
*/

SELECT
  id AS report_join_snippet_id,
  snippet_hierarchy_id,
  level
FROM
  global.report_join_snippets rjs,
  UNNEST( snippet_hierarchy )
WITH ORDINALITY AS sh( snippet_hierarchy_id, level );


--###################################################################################################################
/*
  --If it is necessary update ownership of objects
  ALTER VIEW global.vw_report_mappings OWNER TO marketron_db;
  ALTER VIEW global.vw_report_join_snippet_hierarchy OWNER TO marketron_db;


  ALTER TABLE global.report_type_anchor_report_join_snippets OWNER TO marketron_db;
  ALTER TABLE global.report_field_aliases OWNER TO marketron_db;
  ALTER TABLE global.report_field_sources OWNER TO marketron_db;
  ALTER TABLE global.report_join_snippets OWNER TO marketron_db;
  ALTER TABLE global.report_where_clause_operators OWNER TO marketron_db;
  ALTER TABLE global.report_execution_history OWNER TO marketron_db;
  ALTER TABLE global.report_dynamic_group_by_columns OWNER TO marketron_db;
  ALTER TABLE global.report_ad_type_mapping OWNER to marketron_db;
  ALTER TABLE global.report_spot_states OWNER to marketron_db;
  ALTER TABLE global.dim_date OWNER TO marketron_db;
*/

