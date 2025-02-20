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
create or replace TABLE PRDO_TEST_INTNS.AUDIT.MYTABLE (
	FILE_RUN_ID NUMBER(38,0),
	FILENAME VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_LTZ(9)
);
create or replace schema PRDO_TEST_INTNS.AUDITER;

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
create or replace view PRDO_TEST_INTNS.CONTROL.ETL_RUN_PLAN_STATUS_VW(
	RUN_ID,
	META_ID,
	PLAN_NUMBER,
	SOURCE_NAME,
	SOURCE_TYPE,
	OBJECT_NAME,
	ETL_RUN_STATUS,
	ETL_RUN_STATUS_DT,
	ETLPLAN_STATUS_MSG,
	LOAD_TYPE,
	PRIMARY_KEYS,
	SOURCE_SNIPPET,
	LVL1_FOLDER_NAME,
	ARCHIVE_FILE_NAME,
	FILE_NAME,
	TARGET_SI_NAME,
	SOURCE_ROWS_READ,
	BLOBFILE_ROW_COUNT,
	SI_ROWS_INSERTED,
	SI_ROWS_UPDATED,
	SI_ROWS_DELETED,
	BLOB_COPY_STATUS,
	BLOB_COPY_STATUS_DT,
	BLOB_COPY_STATUS_MSG,
	SOURCE_ARCHIVE_STATUS,
	SOURCE_ARCHIVE_STATUS_DT,
	SOURCE_ARCHIVE_STATUS_MSG,
	PROVISION_STATUS,
	PROVISION_STATUS_DT,
	PROVISION_STATUS_MSG,
	CREATE_JOB_STATUS,
	CREATE_JOB_STATUS_DT,
	CREATE_JOB_STATUS_MSG,
	RUN_JOB_STATUS,
	RUN_JOB_STATUS_DT,
	RUN_JOB_STATUS_MSG,
	STAGE_ARCHIVE_STATUS,
	STAGE_ARCHIVE_STATUS_DT,
	STAGE_ARCHIVE_STATUS_MSG,
	WATERMARK_SET_STATUS,
	WATERMARK_SET_STATUS_DT,
	WATERMARK_SET_STATUS_MSG,
	JOB_SQL_GEN_CDC,
	JOB_SQL_GEN_CDC_DT,
	JOB_SQL_GEN_EXPIRE,
	JOB_SQL_GEN_EXPIRE_DT,
	JOB_SQL_GEN_INSERT,
	JOB_SQL_GEN_INSERT_DT,
	JOB_SQL_CREATE_TABLE,
	JOB_SQL_CREATE_TABLE_DT,
	JOB_SQL_READ,
	JOB_SQL_READ_DT,
	JOB_SQL_EXPIRE,
	JOB_SQL_EXPIRE_DT,
	JOB_SQL_INSERT,
	JOB_SQL_INSERT_DT
) as 
/*
--==============================================================================================================================================
-- View CONTROL.ETL_RUN_PLAN_STATUS_VW: Control.ETL_RUN_PLAN_STATUS_VW
--
-- Description: 
--
--==============================================================================================================================================
-- Revision History
-- Version	  Modified By				Modified Date		Description
-- ---------- -----------------------	-------------------	------------------------------------------------------------------------------------
-- 0.1		  Anup Ramadas				08/01/2020			Initial version
-- 0.2		  Anup Ramadas				29/01/2020			Added all job status details   
-- 0.3		  Anup Ramadas				31/01/2020			Added  LOAD_TYPE , PRIMARY_KEYS to the view 
-- 0.4		  Anup Ramadas				11/02/2020			Changed archive_file_name  
--==============================================================================================================================================
*/
  
SELECT a.RUN_ID,
       a.META_ID,
       a.PLAN_NUMBER,
       a.SOURCE_NAME,
       a.SOURCE_TYPE,
       a.OBJECT_NAME,
       -- 
       b.etlplan_status ETL_RUN_STATUS,
       c.etlplan_status_dt ETL_RUN_STATUS_DT,
       d.etlplan_status_msg,
       -- 
       f.LOAD_TYPE,
       f.PRIMARY_KEYS,
       -- 
       a.SOURCE_SNIPPET,
       a."LVL1_FOLDER_NAME",
       concat(a.SOURCE_NAME,'/',substr( a.FILE_NAME,1,position('/',a.FILE_NAME,1)-1),a.OBJECT_NAME) archive_file_name,
       a.FILE_NAME,
       a.TARGET_SI_NAME,
       -- row counts 
       b.source_rows_read,
       b.blobfile_row_count,
       e.rows_inserted si_rows_inserted,
       e.rows_updated  si_rows_updated,
       e.rows_deleted  si_rows_deleted,
       -- 
       b.blob_copy_status, 
       c.blob_copy_status_dt,
       d.blob_copy_status_msg,
       -- 
       b.source_archive_status,
       c.source_archive_status_dt,
       d.source_archive_status_msg,
       --
       b.provision_status,
       c.provision_status_dt,
       d.provision_status_msg,
       --
       b.create_job_status,
       c.create_job_status_dt,
       d.create_job_status_msg,
       --
       b.run_job_status,
       c.run_job_status_dt,
       d.run_job_status_msg,
       --
       b.stage_archive_status,
       c.stage_archive_status_dt,   
       d.stage_archive_status_msg,   
       -- 
       b.watermark_set_status,
       c.watermark_set_status_dt,
       d.watermark_set_status_msg,
       -- 
      
       -- ** JOB details  
       e.SQL_GEN_CDC JOB_SQL_GEN_CDC ,
       e.SQL_GEN_CDC_DT JOB_SQL_GEN_CDC_DT,
       e.SQL_GEN_EXPIRE JOB_SQL_GEN_EXPIRE,
       e.SQL_GEN_EXPIRE_DT JOB_SQL_GEN_EXPIRE_DT,
       e.SQL_GEN_INSERT JOB_SQL_GEN_INSERT,
       e.SQL_GEN_INSERT_DT JOB_SQL_GEN_INSERT_DT,
       e.SQL_CREATE_TABLE JOB_SQL_CREATE_TABLE,
       e.SQL_CREATE_TABLE_DT JOB_SQL_CREATE_TABLE_DT,
       e.SQL_READ JOB_SQL_READ,
       e.SQL_READ_DT JOB_SQL_READ_DT,
       e.SQL_EXPIRE JOB_SQL_EXPIRE,
       e.SQL_EXPIRE_DT JOB_SQL_EXPIRE_DT,
       e.SQL_INSERT JOB_SQL_INSERT,
       e.SQL_INSERT_DT JOB_SQL_INSERT_DT
       -- **
FROM  CONTROL.ETL_RUN_PLAN a
-- for status   
INNER JOIN 
(
  SELECT RUN_ID,blob_copy_status,source_archive_status,create_job_status,etlplan_status,provision_status,run_job_status,stage_archive_status,watermark_set_status,source_rows_read,blobfile_row_count
FROM 
( 
  SELECT a.RUN_ACTIVITY, a.RUN_ID , a.RUN_STATUS 
  FROM AUDIT.ETL_RUN_LOG a
  INNER JOIN 
  (SELECT RUN_ID,RUN_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP 
FROM AUDIT.ETL_RUN_LOG 
GROUP BY RUN_ID,RUN_ACTIVITY) b
  on a.RUN_ID = b.RUN_ID
  and a.RUN_ACTIVITY = b.RUN_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(RUN_STATUS) for RUN_ACTIVITY in ('blob_copy_status','source_archive_status','create_job_status','etlplan_status','provision_status','run_job_status','stage_archive_status','watermark_set_status','source_rows_read','blobfile_row_count')
) as P (RUN_ID,blob_copy_status,source_archive_status,create_job_status,etlplan_status,provision_status,run_job_status,stage_archive_status,watermark_set_status,source_rows_read,blobfile_row_count)
) b on a.RUN_ID = b.RUN_ID
-- for created_timestamp 
INNER JOIN
(
  SELECT RUN_ID,blob_copy_status_dt,source_archive_status_dt,create_job_status_dt,etlplan_status_dt,provision_status_dt,run_job_status_dt,stage_archive_status_dt,watermark_set_status_dt
FROM 
( 
  SELECT a.RUN_ACTIVITY, a.RUN_ID , a.CREATED_TIMESTAMP 
  FROM AUDIT.ETL_RUN_LOG a
  INNER JOIN 
  (SELECT RUN_ID,RUN_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.ETL_RUN_LOG 
GROUP BY RUN_ID,RUN_ACTIVITY) b
  on a.RUN_ID = b.RUN_ID
  and a.RUN_ACTIVITY = b.RUN_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(CREATED_TIMESTAMP) for RUN_ACTIVITY in ('blob_copy_status','source_archive_status','create_job_status','etlplan_status','provision_status','run_job_status','stage_archive_status','watermark_set_status')
) as P (RUN_ID,blob_copy_status_dt,source_archive_status_dt,create_job_status_dt,etlplan_status_dt,provision_status_dt,run_job_status_dt,stage_archive_status_dt,watermark_set_status_dt)
) c on a.RUN_ID = c.RUN_ID
  -- for messages 
INNER JOIN
(
  SELECT RUN_ID,blob_copy_status_msg,source_archive_status_msg,create_job_status_msg,etlplan_status_msg,provision_status_msg,run_job_status_msg,stage_archive_status_msg,watermark_set_status_msg
FROM 
( 
  SELECT a.RUN_ACTIVITY, a.RUN_ID , a.RUN_MESSAGE 
  FROM AUDIT.ETL_RUN_LOG a
  INNER JOIN 
  (SELECT RUN_ID,RUN_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.ETL_RUN_LOG 
GROUP BY RUN_ID,RUN_ACTIVITY) b
  on a.RUN_ID = b.RUN_ID
  and a.RUN_ACTIVITY = b.RUN_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(RUN_MESSAGE) for RUN_ACTIVITY in ('blob_copy_status','source_archive_status','create_job_status','etlplan_status','provision_status','run_job_status','stage_archive_status','watermark_set_status')
) as P (RUN_ID,blob_copy_status_msg,source_archive_status_msg,create_job_status_msg,etlplan_status_msg,provision_status_msg,run_job_status_msg,stage_archive_status_msg,watermark_set_status_msg)
) d on a.RUN_ID = d.RUN_ID
-- Get JOB DETAILS 
LEFT JOIN CONTROL.JOB_RUN_PLAN_STATUS_VW e
ON a.RUN_ID=E.RUN_ID 
-- Get META Details 
INNER JOIN CONTROL.ETL_META f
ON a.META_ID = f.META_ID;
create or replace view PRDO_TEST_INTNS.CONTROL.ETL_RUN_PLAN_VW(
	RUN_ID,
	META_ID,
	PLAN_NUMBER,
	SOURCE_NAME,
	SOURCE_TYPE,
	OBJECT_NAME,
	SOURCE_SNIPPET,
	SOURCE_PATH,
	REMOVE_SOURCE_FILE_FLAG,
	LVL1_FOLDER_NAME,
	IN_SEQUENCE,
	LVL2_FOLDER_NAME,
	FILE_NAME,
	TARGET_SI_NAME,
	BLOB_COPY_STATUS,
	SOURCE_ARCHIVE_STATUS,
	PROVISION_STATUS,
	CREATE_JOB_STATUS,
	RUN_JOB_STATUS,
	STAGE_ARCHIVE_STATUS,
	WATERMARK_SET_STATUS,
	RUN_STATUS,
	EMAIL_TO_META,
	SQL_WATERMARK
) as 
/*
--==============================================================================================================================================
-- View CONTROL.ETL_RUN_PLAN_VW: ETL_RUN_PLAN_VW
--
-- Description: 
--
--==============================================================================================================================================
-- Revision History
-- Version	  Modified By				Modified Date		Description
-- ---------- -----------------------	-------------------	------------------------------------------------------------------------------------
-- 0.1		  Anup Ramadas				08/01/2020			Initial version
-- 0.2		  Anup Ramadas				16/01/2020			Added LVL2_FOLDER_NAME
-- 0.3		  Anup Ramadas				17/01/2020			Added source_archive_status
-- 0.4		  Anup Ramadas				11/02/2020			Changed archive_file_name
-- 0.5		  Anup Ramadas				14/02/2020			Added SOURCE PATH              
-- 0.6      Gordon Sinclair     02/03/2020      Added EMAIL_TO_META
-- 0.7        Anup Ramadas              04/03/2020           Added IN_SQUENCE
-- 0.8       Anup Ramadas               11/03/2020           Added SQL_WATERMARK     
--==============================================================================================================================================
*/
  
SELECT a.RUN_ID,
       a.META_ID,
       a.PLAN_NUMBER,
       a.SOURCE_NAME,
       a.SOURCE_TYPE,
       a.OBJECT_NAME,
       a.SOURCE_SNIPPET,
       a.SOURCE_PATH,
       a.REMOVE_SOURCE_FILE_FLAG,       
       a."LVL1_FOLDER_NAME",
       a.IN_SEQUENCE ,
       substr( a.FILE_NAME,1,position('/',a.FILE_NAME,1)-1) LVL2_FOLDER_NAME, 
       -- concat(a.SOURCE_NAME,'/',substr( a.FILE_NAME,1,position('/',a.FILE_NAME,1)-1),a.OBJECT_NAME) archive_file_name,
       a.FILE_NAME,
       a.TARGET_SI_NAME,
       b.blob_copy_status, 
       b.source_archive_status,
       b.provision_status,
       b.create_job_status,
       b.run_job_status,
       b.stage_archive_status,
       b.watermark_set_status,
       b.etlplan_status RUN_STATUS,
       c.EMAIL_TO_META,
       CASE a.SOURCE_TYPE
       WHEN 'TABLE'
       THEN 
            CASE COALESCE(c.SQL_WATERMARK, '') 
            WHEN '' THEN concat('SELECT max(-1) VALUE FROM ',a.OBJECT_NAME)
            ELSE concat(c.SQL_WATERMARK,'FROM ',a.OBJECT_NAME) 
        END
        END SQL_WATERMARK
FROM  CONTROL.ETL_RUN_PLAN a
INNER JOIN 
(
  SELECT RUN_ID,blob_copy_status,source_archive_status,create_job_status,etlplan_status,provision_status,run_job_status,stage_archive_status,watermark_set_status
FROM 
( 
  SELECT a.RUN_ACTIVITY, a.RUN_ID , a.RUN_STATUS 
  FROM AUDIT.ETL_RUN_LOG a
  INNER JOIN 
  (SELECT RUN_ID,RUN_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.ETL_RUN_LOG 
GROUP BY RUN_ID,RUN_ACTIVITY) b
  on a.RUN_ID = b.RUN_ID
  and a.RUN_ACTIVITY = b.RUN_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(RUN_STATUS) for RUN_ACTIVITY in ('blob_copy_status','source_archive_status','create_job_status','etlplan_status','provision_status','run_job_status','stage_archive_status','watermark_set_status')
) as P (RUN_ID,blob_copy_status,source_archive_status,create_job_status,etlplan_status,provision_status,run_job_status,stage_archive_status,watermark_set_status)
) b on a.RUN_ID = b.RUN_ID
LEFT JOIN CONTROL.ETL_META c 
	ON a.META_ID = c.META_ID 
WHERE b.etlplan_status = 'PLANNED'
ORDER BY RUN_ID;
create or replace view PRDO_TEST_INTNS.CONTROL.JOB_META_VW(
	JOB_ID,
	SOURCE_NAME,
	JOB_GROUP,
	JOB_NAME,
	JOB_ACTION,
	SOURCE_SCHEMA_NAME,
	SOURCE_TABLE_NAME,
	TARGET_SCHEMA_NAME,
	TARGET_TABLE_NAME,
	TARGET_PRIMARY_KEY,
	SQL_STATEMENT
) as
/*
--==============================================================================================================================================
-- View CONTROL.JOB_META_VW: Control.JOB_META_VW
--
-- Description: 
--
--==============================================================================================================================================
-- Revision History
-- Version	  Modified By				Modified Date		Description
-- ---------- -----------------------	-------------------	------------------------------------------------------------------------------------
-- 0.1		  Anup Ramadas				08/01/2020			Initial version
-- 0.2        Anup Ramadas				25/02/2020			Rename DELETE_STATEMENT to SQL_STATEMENT 
--==============================================================================================================================================
*/

SELECT JOB_ID,SOURCE_NAME,JOB_GROUP,SOURCE_TABLE_NAME JOB_NAME  ,JOB_ACTION ,SOURCE_SCHEMA_NAME ,SOURCE_TABLE_NAME
      ,TARGET_SCHEMA_NAME      ,TARGET_TABLE_NAME      ,TARGET_PRIMARY_KEY      ,SQL_STATEMENT

FROM CONTROL.JOB_META
WHERE ACTIVE_FLAG = TRUE;
create or replace view PRDO_TEST_INTNS.CONTROL.JOB_RUN_PLAN_STATUS_VW(
	JOB_RUN_ID,
	JOB_ID,
	RUN_ID,
	SOURCE_NAME,
	JOB_GROUP,
	JOB_GROUP_SEQ,
	JOB_RUN_SEQ,
	JOB_NAME,
	JOB_ACTION,
	SOURCE_SCHEMA_NAME,
	SOURCE_TABLE_NAME,
	TARGET_SCHEMA_NAME,
	TARGET_TABLE_NAME,
	TARGET_PRIMARY_KEY,
	EXCLUDED_COLUMNS,
	SQL_STATEMENT,
	COMPARE_BY_JOB_NAME,
	RUN_STATUS,
	RUN_TIME,
	RUN_MESSAGE,
	SQL_GEN_CDC,
	SQL_GEN_CDC_DT,
	SQL_GEN_CDC_MSG,
	SQL_GEN_EXPIRE,
	SQL_GEN_EXPIRE_DT,
	SQL_GEN_EXPIRE_MSG,
	SQL_GEN_INSERT,
	SQL_GEN_INSERT_DT,
	SQL_GEN_INSERT_MSG,
	SQL_CREATE_TABLE,
	SQL_CREATE_TABLE_DT,
	SQL_CREATE_TABLE_MSG,
	SQL_READ,
	SQL_READ_DT,
	SQL_READ_MSG,
	SQL_EXPIRE,
	SQL_EXPIRE_DT,
	SQL_EXPIRE_MSG,
	SQL_INSERT,
	SQL_INSERT_DT,
	SQL_INSERT_MSG,
	ROWS_READ,
	ROWS_INSERTED,
	ROWS_UPDATED,
	ROWS_DELETED,
	PIPELINE_RUN_ID,
	UPDATED_TIMESTAMP
) as 
/*
--====================================================================================================================================================================
-- View CONTROL.JOB_RUN_PLAN_STATUS_VW: Control.JOB_RUN_PLAN_STATUS_VW
--
-- Description: 
--
--====================================================================================================================================================================
-- Revision History
-- Version	  Modified By				Modified Date		Description
-- ---------- -----------------------	-------------------	-----------------------------------------------------------------------------------------------------------
-- 0.1		  Anup Ramadas				08/01/2020			Initial version
-- 0.2        Anup Ramadas				23/01/2020			Added interim status
-- 0.3        Anup Ramadas				25/02/2020			Rename DELETE_STATEMENT to SQL_STATEMENT 
-- 0.4        Thiru                     05/05/2020          Added PIPELINE_RUN_ID
-- 0.5        Dhandapani Sudhakar       07/02/2021          Updated the select query - When the jobplan_status is "Forced Completion" that will be cast to "COMPLETED"
--====================================================================================================================================================================
*/
  
SELECT a.JOB_RUN_ID,
       a.JOB_ID,
       a.RUN_ID,
       a.SOURCE_NAME,
       a.JOB_GROUP,
       a.JOB_GROUP_SEQ,
       a.JOB_RUN_SEQ,
       a.JOB_NAME  ,
       a.JOB_ACTION ,
       a.SOURCE_SCHEMA_NAME ,
       a.SOURCE_TABLE_NAME,
       a.TARGET_SCHEMA_NAME,
       a.TARGET_TABLE_NAME,
       a.TARGET_PRIMARY_KEY,
       a.EXCLUDED_COLUMNS,
       a.SQL_STATEMENT,
       a.COMPARE_BY_JOB_NAME,
       --
	   case 
			when b.jobplan_status = 'Forced Completion' then 'COMPLETED'
			else b.jobplan_status 
	   end 
			as RUN_STATUS,
       c.jobplan_status_dt RUN_TIME,
       e.jobplan_status_msg RUN_MESSAGE,
       --
       b.sql_gen_cdc ,
       c.sql_gen_cdc_dt,
       e.sql_gen_cdc_msg,
       --
       b.sql_gen_expire,
       c.sql_gen_expire_dt,
       e.sql_gen_expire_msg,
       -- 
       b.sql_gen_insert,
       c.sql_gen_insert_dt,
       e.sql_gen_insert_msg,
       --
       b.sql_create_table,
       c.sql_create_table_dt,
       e.sql_create_table_msg,
       --
       b.sql_read,
       c.sql_read_dt,
       e.sql_read_msg,
       --
       b.sql_expire,
       c.sql_expire_dt,
       e.sql_expire_msg,
       --
       b.sql_insert,
       c.sql_insert_dt,
       e.sql_insert_msg,
       --
       d.ROWS_READ,
       d.ROWS_INSERTED,
       d.ROWS_UPDATED,
       d.ROWS_DELETED,
       a.PIPELINE_RUN_ID,
       a.UPDATED_TIMESTAMP 
FROM  CONTROL.JOB_RUN_PLAN a
-- job status 
INNER JOIN 
(
  SELECT JOB_RUN_ID,sql_gen_cdc,sql_gen_expire,sql_gen_insert,sql_create_table,sql_read,sql_expire,sql_insert,jobplan_status
FROM 
( 
  SELECT a.JOB_ACTIVITY, a.JOB_RUN_ID , a.JOB_STATUS 
  FROM AUDIT.JOB_RUN_LOG a
  INNER JOIN 
  (SELECT JOB_RUN_ID,JOB_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.JOB_RUN_LOG 
GROUP BY JOB_RUN_ID,JOB_ACTIVITY) b
  on a.JOB_RUN_ID = b.JOB_RUN_ID
  and a.JOB_ACTIVITY = b.JOB_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(JOB_STATUS) for JOB_ACTIVITY in ('sql_gen_cdc','sql_gen_expire','sql_gen_insert','sql_create_table','sql_read','sql_expire','sql_insert','jobplan_status')
) as P (JOB_RUN_ID,sql_gen_cdc,sql_gen_expire,sql_gen_insert,sql_create_table,sql_read,sql_expire,sql_insert,jobplan_status)
) b on a.JOB_RUN_ID = b.JOB_RUN_ID
-- job message 
INNER JOIN 
(
  SELECT JOB_RUN_ID,sql_gen_cdc_msg,sql_gen_expire_msg,sql_gen_insert_msg,sql_create_table_msg,sql_read_msg,sql_expire_msg,sql_insert_msg,jobplan_status_msg
FROM 
( 
  SELECT a.JOB_ACTIVITY, a.JOB_RUN_ID , a.JOB_MESSAGE 
  FROM AUDIT.JOB_RUN_LOG a
  INNER JOIN 
  (SELECT JOB_RUN_ID,JOB_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.JOB_RUN_LOG 
GROUP BY JOB_RUN_ID,JOB_ACTIVITY) b
  on a.JOB_RUN_ID = b.JOB_RUN_ID
  and a.JOB_ACTIVITY = b.JOB_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(JOB_MESSAGE) for JOB_ACTIVITY in ('sql_gen_cdc','sql_gen_expire','sql_gen_insert','sql_create_table','sql_read','sql_expire','sql_insert','jobplan_status')
) as P (JOB_RUN_ID,sql_gen_cdc_msg,sql_gen_expire_msg,sql_gen_insert_msg,sql_create_table_msg,sql_read_msg,sql_expire_msg,sql_insert_msg,jobplan_status_msg)
) e on a.JOB_RUN_ID = e.JOB_RUN_ID



-- Created time
INNER JOIN 
(
  SELECT JOB_RUN_ID,sql_gen_cdc_dt,sql_gen_expire_dt,sql_gen_insert_dt,sql_create_table_dt,sql_read_dt,sql_expire_dt,sql_insert_dt, JOBPLAN_STATUS_DT
FROM 
( 
  SELECT a.JOB_ACTIVITY, a.JOB_RUN_ID , a.CREATED_TIMESTAMP 
  FROM AUDIT.JOB_RUN_LOG a
  INNER JOIN 
  (SELECT JOB_RUN_ID,JOB_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.JOB_RUN_LOG 
GROUP BY JOB_RUN_ID,JOB_ACTIVITY) b
  on a.JOB_RUN_ID = b.JOB_RUN_ID
  and a.JOB_ACTIVITY = b.JOB_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(CREATED_TIMESTAMP) for JOB_ACTIVITY in ('sql_gen_cdc','sql_gen_expire','sql_gen_insert','sql_create_table','sql_read','sql_expire','sql_insert','jobplan_status')
) as P (JOB_RUN_ID,sql_gen_cdc_dt,sql_gen_expire_dt,sql_gen_insert_dt,sql_create_table_dt,sql_read_dt,sql_expire_dt,sql_insert_dt,JOBPLAN_STATUS_DT)
) c on a.JOB_RUN_ID = c.JOB_RUN_ID
-- row counts 
INNER JOIN 
(
  SELECT JOB_RUN_ID, ROWS_READ,ROWS_INSERTED,ROWS_UPDATED,ROWS_DELETED
FROM 
( 
  SELECT a.JOB_ACTIVITY, a.JOB_RUN_ID , a.ROW_COUNT 
  FROM AUDIT.JOB_RUN_LOG a
  INNER JOIN 
  (SELECT JOB_RUN_ID,JOB_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
FROM AUDIT.JOB_RUN_LOG 
GROUP BY JOB_RUN_ID,JOB_ACTIVITY) b
  on a.JOB_RUN_ID = b.JOB_RUN_ID
  and a.JOB_ACTIVITY = b.JOB_ACTIVITY
  and a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
pivot( 
 max(ROW_COUNT) for JOB_ACTIVITY in ('rows_read','rows_inserted','rows_updated','rows_deleted')
) as P (JOB_RUN_ID,ROWS_READ,ROWS_INSERTED,ROWS_UPDATED,ROWS_DELETED)
) d on a.JOB_RUN_ID = d.JOB_RUN_ID
--WHERE b.jobplan_status = 'PLANNED'
ORDER BY a.JOB_RUN_ID;
create or replace view PRDO_TEST_INTNS.CONTROL.JOB_RUN_PLAN_VW(
	JOB_RUN_ID,
	JOB_ID,
	SOURCE_NAME,
	JOB_GROUP,
	JOB_GROUP_SEQ,
	JOB_RUN_SEQ,
	JOB_NAME,
	JOB_ACTION,
	SOURCE_SCHEMA_NAME,
	SOURCE_TABLE_NAME,
	TARGET_SCHEMA_NAME,
	TARGET_TABLE_NAME,
	TARGET_PRIMARY_KEY,
	EXCLUDED_COLUMNS,
	SQL_STATEMENT,
	COMPARE_BY_JOB_NAME,
	WAREHOUSE,
	TRIM_PRIMARY_KEY,
	RUN_STATUS,
	FILE_NAME,
	FILE_DATE,
	PIPELINE_RUN_ID,
	UPDATED_TIMESTAMP
) as 
/*
--==============================================================================================================================================
-- View CONTROL.JOB_RUN_PLAN_VW: Control.JOB_RUN_PLAN_VW
--
-- Description: 
--
--==============================================================================================================================================
-- Revision History
-- Version	  Modified By				Modified Date		Description
-- ---------- -----------------------	-------------------	------------------------------------------------------------------------------------
-- 0.1		  Anup Ramadas				08/01/2020			Initial version
-- 0.2		  Anup Ramadas				23/01/2020			Added FILE_NAME,FILE_DATE 
-- 0.3	      Anup Ramadas				10/02/2020			Updated jobplan_status filter
-- 0.4		  Fadi Dahbar				10/02/2020			Added WAREHOUSE and TRIM_PRIMARY_KEY to return set
-- 0.5        Aup Ramadas               25/02/2020          Rename DELETE_STATEMENT to SQL_STATEMENT 
-- 0.6        Thiru                     05/05/2020          Added a new coulmn PIPELINE_RUN_ID
-- 0.7        Dhandapani Sudhakar       07/02/2021          "FAILED" status added to the jobplan_status filter in where clause
--==============================================================================================================================================
*/
  
SELECT a.JOB_RUN_ID,
       a.JOB_ID,
       a.SOURCE_NAME,
       a.JOB_GROUP,
       a.JOB_GROUP_SEQ,
       a.JOB_RUN_SEQ,
       a.JOB_NAME  ,
       a.JOB_ACTION ,
       a.SOURCE_SCHEMA_NAME ,
       a.SOURCE_TABLE_NAME,
       a.TARGET_SCHEMA_NAME,
       a.TARGET_TABLE_NAME,
       a.TARGET_PRIMARY_KEY,
       a.EXCLUDED_COLUMNS,
       a.SQL_STATEMENT,
       a.COMPARE_BY_JOB_NAME,
	   a.WAREHOUSE,
	   a.TRIM_PRIMARY_KEY,
       b.jobplan_status RUN_STATUS,
       c.FILE_NAME FILE_NAME,
       c.FILE_DATE FILE_DATE,
       a.PIPELINE_RUN_ID,
       a.UPDATED_TIMESTAMP 
FROM  CONTROL.JOB_RUN_PLAN a
INNER JOIN 
(
  SELECT JOB_RUN_ID,jobplan_status
  FROM 
  ( 
	SELECT a.JOB_ACTIVITY, a.JOB_RUN_ID , a.JOB_STATUS 
	FROM AUDIT.JOB_RUN_LOG a
	INNER JOIN  (
		SELECT JOB_RUN_ID,JOB_ACTIVITY, MAX(CREATED_TIMESTAMP) CREATED_TIMESTAMP
		FROM AUDIT.JOB_RUN_LOG 
		GROUP BY JOB_RUN_ID,JOB_ACTIVITY
	) b
	ON a.JOB_RUN_ID = b.JOB_RUN_ID
	AND a.JOB_ACTIVITY = b.JOB_ACTIVITY
	AND a.CREATED_TIMESTAMP = b.CREATED_TIMESTAMP  
) as TEMP 
PIVOT ( 
	MAX(JOB_STATUS) FOR JOB_ACTIVITY IN ('jobplan_status')
) AS P (JOB_RUN_ID,jobplan_status)
) b
ON a.JOB_RUN_ID = b.JOB_RUN_ID
LEFT JOIN CONTROL.ETL_RUN_PLAN c
ON a.RUN_ID = c.RUN_ID
WHERE b.jobplan_status in ('PLANNED','STARTED','FAILED')
ORDER BY a.JOB_RUN_ID;
create or replace view PRDO_TEST_INTNS.CONTROL.JOB_RUN_WATERMARK_VW(
	JOB_NAME,
	WATERMARK_START_TIMESTAMP,
	WATERMARK_END_TIMESTAMP
) as
/*
--=========================================================================================================================================================
-- View CONTROL.JOB_RUN_WATERMARK_VW: JOB_RUN_WATERMARK_VW
--
-- Description: 
--
--=========================================================================================================================================================
-- Revision History
-- Version           Modified By                        Modified Date      Description
-- ---------- --------------------------------------------------------------------------------------------------------------------
-- 0.1                  Thiru							30/04/2020         Initial version
-- 0.2                  Dhandapani Sudhakar             12/02/2020         Updated the logic to retrieve start timestamp from JOB_META, and end timestamp from JOB_RUN_LOG
-- 0.3     				Thiru                           20/04/2020         Fixed issue with duplication
-- 0.4     				Thiru                           29/06/2021         Changed Logic because of change in watermark column
--=========================================================================================================================================================
*/
SELECT
            CURR.JOB_NAME
            , NVL(COMPLETED.WATERMARK_START_TIMESTAMP
            , CAST('1970-01-01' AS TIMESTAMPLTZ)) AS WATERMARK_START_TIMESTAMP
            , CURR.WATERMARK_END_TIMESTAMP AS WATERMARK_END_TIMESTAMP
FROM
            (
            SELECT
                        PL.JOB_NAME, CAST(LOGTBL.JOB_MESSAGE AS TIMESTAMPLTZ) WATERMARK_END_TIMESTAMP
            FROM
                        CONTROL.JOB_RUN_PLAN_STATUS_VW PL
            INNER JOIN AUDIT.JOB_RUN_LOG LOGTBL ON
                        LOGTBL.JOB_RUN_ID = PL.JOB_RUN_ID
                        AND LOGTBL.JOB_ACTIVITY = 'current_watermark' 
                        QUALIFY ROW_NUMBER() OVER( PARTITION BY PL.JOB_NAME
            ORDER BY
                        LOGTBL.CREATED_TIMESTAMP DESC)= 1 )CURR
LEFT JOIN (
            SELECT
                        PL.JOB_NAME, CAST(LOGTBL.JOB_MESSAGE AS TIMESTAMPLTZ) WATERMARK_START_TIMESTAMP
            FROM
                        CONTROL.JOB_RUN_PLAN_STATUS_VW PL
            INNER JOIN AUDIT.JOB_RUN_LOG LOGTBL ON
                        LOGTBL.JOB_RUN_ID = PL.JOB_RUN_ID
                        AND LOGTBL.JOB_ACTIVITY = 'current_watermark'
                        AND LOGTBL.JOB_STATUS = 'FINAL' QUALIFY ROW_NUMBER() OVER( PARTITION BY PL.JOB_NAME
            ORDER BY
                        LOGTBL.CREATED_TIMESTAMP DESC)= 1 )COMPLETED ON
            COMPLETED.JOB_NAME = CURR.JOB_NAME;
create or replace schema PRDO_TEST_INTNS.PUBLIC;
