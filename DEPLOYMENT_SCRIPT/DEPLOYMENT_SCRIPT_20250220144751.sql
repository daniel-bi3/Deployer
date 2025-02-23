-- ADD_SCHEMA CONTROL
CREATE SCHEMA PRDO_TEST_INTNS.CONTROL;
-- ADD CONTROL.ETL_META
create or replace TABLE PRDO_TEST_INTNS.CONTROL.ETL_META (
	META_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	SOURCE_NAME VARCHAR(100) NOT NULL,
	SOURCE_TYPE VARCHAR(100),
	OBJECT_NAME VARCHAR(256) NOT NULL,
	LOAD_TYPE VARCHAR(256),
	PRIMARY_KEYS VARCHAR(256),
	SQL_SNIPPET VARCHAR(16777216),
	WATERMARK ARRAY,
	TRAN_WATERMARK ARRAY,
	LVL2_FOLDER_NAME VARCHAR(256),
	TARGET_SI_NAME VARCHAR(256),
	ACTIVE_FLAG BOOLEAN NOT NULL DEFAULT FALSE,
	CREATED_DATE TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	UPDATED_DATE TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	TIMESTAMP_PARSE VARCHAR(16777216),
	EXCLUDED_COLUMNS VARCHAR(16777216),
	SQL_STATEMENT VARCHAR(16777216),
	SEQUENCE_PARSE VARCHAR(16777216),
	SECURE_DATASET_FLAG BOOLEAN NOT NULL DEFAULT FALSE,
	REMOVE_SOURCE_FILE_FLAG BOOLEAN NOT NULL DEFAULT FALSE,
	SOURCE_PATH VARCHAR(256),
	EMAIL_TO_META VARIANT,
	STAGE_FILE_TYPE VARCHAR(256),
	STAGE_FILE_COMPRESSION BOOLEAN NOT NULL DEFAULT FALSE,
	SQL_WATERMARK VARCHAR(16777216),
	PARQUET_HEADER BOOLEAN NOT NULL DEFAULT FALSE,
	PARQUET_COLUMN_ORDER VARCHAR(16777216),
	constraint ETL_META_PK primary key (SOURCE_NAME, OBJECT_NAME)
);



-- ADD CONTROL.ETL_PARAMETER
create or replace TABLE PRDO_TEST_INTNS.CONTROL.ETL_PARAMETER (
	ETL_PARAMETER_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 order,
	SOURCE_NAME VARCHAR(16777216) NOT NULL,
	OBJECT_NAME VARCHAR(16777216) NOT NULL,
	KEY VARCHAR(16777216) NOT NULL,
	VALUE VARCHAR(16777216) NOT NULL,
	ACTIVE_FLAG BOOLEAN NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL,
	UPDATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL,
	constraint ETL_PARAMETER_PK primary key (ETL_PARAMETER_ID)
);



-- ADD CONTROL.ETL_RUN_PLAN
create or replace TABLE PRDO_TEST_INTNS.CONTROL.ETL_RUN_PLAN (
	RUN_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	META_ID NUMBER(38,0) NOT NULL,
	PLAN_NUMBER NUMBER(38,0),
	SOURCE_NAME VARCHAR(100),
	SOURCE_TYPE VARCHAR(100),
	OBJECT_NAME VARCHAR(256),
	SOURCE_SNIPPET VARCHAR(16777216),
	LVL1_FOLDER_NAME VARCHAR(100),
	FILE_NAME VARCHAR(256),
	FILE_DATE TIMESTAMP_LTZ(9),
	TARGET_SI_NAME VARCHAR(256),
	CREATED_DATE TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	LOAD_TYPE VARCHAR(256),
	SOURCE_PATH VARCHAR(256),
	REMOVE_SOURCE_FILE_FLAG BOOLEAN,
	IN_SEQUENCE BOOLEAN DEFAULT FALSE
);



-- ADD CONTROL.JOB_META
create or replace TABLE PRDO_TEST_INTNS.CONTROL.JOB_META (
	JOB_ID NUMBER(38,0) NOT NULL autoincrement start 775 increment 1 order,
	SOURCE_NAME VARCHAR(200) NOT NULL,
	JOB_NAME VARCHAR(200) NOT NULL,
	JOB_GROUP VARCHAR(200) NOT NULL,
	JOB_GROUP_SEQ NUMBER(38,0) NOT NULL,
	JOB_RUN_SEQ NUMBER(38,0) NOT NULL,
	JOB_ACTION VARCHAR(50) NOT NULL,
	SOURCE_SCHEMA_NAME VARCHAR(200) NOT NULL,
	SOURCE_TABLE_NAME VARCHAR(200) NOT NULL,
	TARGET_SCHEMA_NAME VARCHAR(200) NOT NULL,
	TARGET_TABLE_NAME VARCHAR(200) NOT NULL,
	TARGET_PRIMARY_KEY VARCHAR(16777216),
	EXCLUDED_COLUMNS VARCHAR(16777216),
	SQL_STATEMENT VARCHAR(16777216),
	COMPARE_BY_JOB_NAME BOOLEAN NOT NULL,
	ACTIVE_FLAG BOOLEAN NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	WAREHOUSE VARCHAR(200),
	TRIM_PRIMARY_KEY BOOLEAN NOT NULL DEFAULT FALSE,
	constraint JOB_PK primary key (JOB_ID)
);



-- ADD CONTROL.JOB_PARAMETER
create or replace TABLE PRDO_TEST_INTNS.CONTROL.JOB_PARAMETER (
	JOB_PARAMETER_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 order,
	SOURCE_NAME VARCHAR(16777216) NOT NULL,
	KEY VARCHAR(16777216) NOT NULL,
	VALUE VARCHAR(16777216) NOT NULL,
	ACTIVE_FLAG BOOLEAN NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL,
	UPDATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL,
	JOB_NAME VARCHAR(16777216),
	constraint JOB_PARAMETER_PK primary key (JOB_PARAMETER_ID)
);



-- ADD CONTROL.JOB_RUN_PLAN
create or replace TABLE PRDO_TEST_INTNS.CONTROL.JOB_RUN_PLAN (
	JOB_RUN_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	JOB_ID NUMBER(38,0) NOT NULL,
	RUN_ID NUMBER(38,0) NOT NULL,
	SOURCE_NAME VARCHAR(200) NOT NULL,
	JOB_GROUP VARCHAR(200) NOT NULL,
	JOB_GROUP_SEQ NUMBER(38,0) NOT NULL,
	JOB_RUN_SEQ NUMBER(38,0) NOT NULL,
	JOB_NAME VARCHAR(200) NOT NULL,
	JOB_ACTION VARCHAR(50) NOT NULL,
	SOURCE_SCHEMA_NAME VARCHAR(200) NOT NULL,
	SOURCE_TABLE_NAME VARCHAR(200) NOT NULL,
	TARGET_SCHEMA_NAME VARCHAR(200) NOT NULL,
	TARGET_TABLE_NAME VARCHAR(200) NOT NULL,
	TARGET_PRIMARY_KEY VARCHAR(16777216),
	EXCLUDED_COLUMNS VARCHAR(16777216),
	SQL_STATEMENT VARCHAR(16777216),
	COMPARE_BY_JOB_NAME BOOLEAN NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	WAREHOUSE VARCHAR(200),
	TRIM_PRIMARY_KEY BOOLEAN NOT NULL DEFAULT FALSE,
	PIPELINE_RUN_ID VARCHAR(16777216),
	UPDATED_TIMESTAMP TIMESTAMP_LTZ(9)
);



-- ADD CONTROL.SAS_TOKEN
create or replace TABLE PRDO_TEST_INTNS.CONTROL.SAS_TOKEN (
	ENVIRONMENT VARCHAR(100) NOT NULL,
	TOKEN VARCHAR(256) NOT NULL,
	EXPIRY_DATE TIMESTAMP_NTZ(9),
	SIGNING_KEY VARCHAR(256) NOT NULL,
	BLOB_URL VARCHAR(16777216) NOT NULL,
	ACTIVE_FLAG BOOLEAN DEFAULT TRUE
);






-- REMOVE_SCHEMA AUDITER
DROP SCHEMA PRDO_TEST_INTNS.AUDITER;



-- ADD AUDIT
create or replace TABLE PRDO_TEST_INTNS.AUDIT.JOB_RUN_LOG (
	LOG_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	JOB_RUN_ID NUMBER(38,0),
	JOB_ACTIVITY VARCHAR(100),
	ROW_COUNT NUMBER(38,0),
	JOB_STATUS VARCHAR(100),
	JOB_MESSAGE VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP()
);



-- REMOVE AUDIT.MYTABLE
DROP TABLE PRDO_TEST_INTNS.AUDIT.MYTABLE;



-- MODIFY AUDIT.ETL_RUN_LOG
ALTER TABLE PRDO_TEST_INTNS.AUDIT.ETL_RUN_LOG DROP COLUMN LOG__MY__ID; ALTER TABLE PRDO_TEST_INTNS.AUDIT.ETL_RUN_LOG ADD COLUMN LOG_ID NUMBER; 


