create or replace database PRDO_TEST_INTNS;

create or replace schema AUDIT;

create or replace TABLE ETL_FILE_RUN_LOG (
	FILE_RUN_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 order,
	FILENAME VARCHAR(16777216) NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
create or replace TABLE ETL_RUN_LOG (
	RUN_ID NUMBER(38,0),
	RUN_ACTIVITY VARCHAR(100),
	RUN_STATUS VARCHAR(100),
	RUN_MESSAGE VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9),
	LOG__MY__ID VARCHAR(255)
);
create or replace TABLE MYTABLE (
	FILE_RUN_ID NUMBER(38,0),
	FILENAME VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9)
);
create or replace schema AUDITER;

create or replace schema PUBLIC;
