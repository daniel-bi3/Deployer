create or replace database PRDO_TEST_INTNS;

create or replace schema PRDO_TEST_INTNS.AUDIT;

create or replace TABLE PRDO_TEST_INTNS.AUDIT.ETL_FILE_RUN_LOG (
	FILE_RUN_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 order,
	FILENAME VARCHAR(16777216) NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
create or replace TABLE PRDO_TEST_INTNS.AUDIT.ETL_RUN_LOG (
	RUN_ID NUMBER(38,0),
	RUN_ACTIVITY VARCHAR(100),
	RUN_STATUS VARCHAR(100),
	RUN_MESSAGE VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9),
	LOG_ID NUMBER(38,0)
);
create or replace TABLE PRDO_TEST_INTNS.AUDIT.JOB_RUN_LOG (
	LOG_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	JOB_RUN_ID NUMBER(38,0),
	JOB_ACTIVITY VARCHAR(100),
	ROW_COUNT NUMBER(38,0),
	JOB_STATUS VARCHAR(100),
	JOB_MESSAGE VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
create or replace schema PRDO_TEST_INTNS.CONTROL;

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
create or replace TABLE PRDO_TEST_INTNS.CONTROL.SAS_TOKEN (
	ENVIRONMENT VARCHAR(100) NOT NULL,
	TOKEN VARCHAR(256) NOT NULL,
	EXPIRY_DATE TIMESTAMP_NTZ(9),
	SIGNING_KEY VARCHAR(256) NOT NULL,
	BLOB_URL VARCHAR(16777216) NOT NULL,
	ACTIVE_FLAG BOOLEAN DEFAULT TRUE
);
create or replace schema PRDO_TEST_INTNS.PUBLIC;

create or replace TABLE PRDO_TEST_INTNS.PUBLIC.JOHN_123 (
	REG_NO VARCHAR(50),
	NAME VARCHAR(16777216)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.JOHN_321 (
	REG_NO VARCHAR(16777216)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.LOG_A (
	START_TIME TIMESTAMP_TZ(9),
	END_TIME TIMESTAMP_TZ(9),
	NEW_RECORDS_ADDED NUMBER(38,0),
	RECORDS_UPDATED NUMBER(38,0),
	OPERATION_STATUS VARCHAR(255)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.MOBILE (
	NAME VARCHAR(255)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.MOBILE_CSV (
	PROD_NAME VARCHAR(255),
	PRICE FLOAT,
	SALE NUMBER(38,0),
	LEFT_QNTY NUMBER(38,0)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.MOBILE_JSON (
	PROD_NAME VARCHAR(255),
	PRICE FLOAT,
	SALE NUMBER(38,0),
	LEFT_QNTY NUMBER(38,0)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.MOBILE_TXT (
	PROD_NAME VARCHAR(255),
	PRICE FLOAT,
	SALE NUMBER(38,0),
	LEFT_QNTY NUMBER(38,0)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.MY_VALUES (
	VALUE NUMBER(38,0)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.NEW_SALES (
	SALEID NUMBER(38,0) NOT NULL,
	CUSTOMERID NUMBER(38,0),
	PRODUCTID NUMBER(38,0),
	SALEDATE DATE,
	AMOUNT FLOAT,
	primary key (SALEID)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.ORDER_TRANSACTIONS (
	USER_ID NUMBER(38,0),
	NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	CREDIT_CARD_NUMBER VARCHAR(16777216)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.PROD_1 (
	PRODUCTID NUMBER(38,0),
	PRODUCTNAME VARCHAR(16777216),
	CATEGORY VARCHAR(16777216),
	PRICE NUMBER(38,2),
	STOCKQUANTITY NUMBER(38,0),
	NEW_DATE TIMESTAMP_NTZ(9)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.PURCHASE_1 (
	COL_1 VARIANT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.RAW_SALE (
	C1 VARCHAR(16777216),
	C2 VARCHAR(16777216),
	C3 VARCHAR(16777216),
	C4 VARCHAR(16777216),
	C5 VARCHAR(16777216),
	C6 VARCHAR(16777216)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.RAW_SALSE (
	TRANSACTIONID VARCHAR(255),
	DATE VARCHAR(255),
	CUSTOMERID VARCHAR(255),
	AMOUNT VARCHAR(255),
	PRODUCTID VARCHAR(255),
	EMPTY VARCHAR(255)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SALES (
	SALEID NUMBER(38,0) NOT NULL,
	CUSTOMERID NUMBER(38,0),
	PRODUCTID NUMBER(38,0),
	SALEDATE DATE,
	AMOUNT FLOAT,
	primary key (SALEID)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SALES_DATA (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	ORDER_AMOUNT FLOAT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SALSE (
	SALEID NUMBER(38,0),
	PRODUCTID NUMBER(38,0),
	CUSTOMERID NUMBER(38,0),
	QUANTITY NUMBER(38,0),
	SALEDATE DATE,
	TOTALAMOUNT FLOAT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SAMPLE_1 (
	NAME VARCHAR(16777216),
	ROLL_NO VARCHAR(16777216)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SOURCE_1 (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	ORDER_AMOUNT FLOAT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SOURCE_A (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	ORDER_AMOUNT FLOAT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.SUPPORT_TICKETS (
	TICKET_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	TICKET_STATUS VARCHAR(255),
	LAST_UPDATED TIMESTAMP_TZ(9)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.TARGET_1 (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	ORDER_AMOUNT FLOAT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.TARGET_A (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	ORDER_AMOUNT FLOAT
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.TARGET_SALES_DATA (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	ORDER_AMOUNT FLOAT,
	LAST_MODIFIED TIMESTAMP_LTZ(9)
);
create or replace TABLE PRDO_TEST_INTNS.PUBLIC.TARGET_TICKETS (
	TICKET_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	TICKET_STATUS VARCHAR(255),
	LAST_UPDATED TIMESTAMP_TZ(9)
);