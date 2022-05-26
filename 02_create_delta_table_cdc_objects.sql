
use role sysadmin;
use database demo;
use schema azure;


-- create stage using the storage integration (you can also use SAS token if not SI)

CREATE FILE FORMAT IF NOT EXISTS MY_PARQUET_FORMAT
  TYPE = PARQUET;


create stage iotdata_stage
  storage_integration = iotdata_si
  url = 'azure://myaccount.blob.core.windows.net/container1/path1'
  file_format = MY_PARQUET_FORMAT;

-- define source table
create or replace external table iot_device_delta
location = @iotdata_stage/deltalake
FILE_FORMAT = (TYPE = PARQUET)
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
TABLE_FORMAT = DELTA
;

-- External source table
select * from iot_device_delta limit 10;
desc table iot_device_delta;

-- Create a Snowflake Table to load and sync with Delta table. Comes handy for CDC.
create or replace table raw_iot_dbx_to_snf (iotdata variant);
desc table raw_iot_dbx_to_snf;

select count(*) from raw_iot_dbx_to_snf;


-- Streams on the external table

create or replace stream iot_cdc_stream 
    on external table iot_device_delta 
    insert_only = true
    comment = 'This stream is used capture changes in the Databricks Delta tables';

show streams;

select * from iot_cdc_stream;
alter external table iot_device_delta refresh;

-- We can schedule a task to periodically refresh the external table
-- alter external table iotdata_delta_dbx refresh;
CREATE or replace TASK refresh_iot_table
  WAREHOUSE = lab_xs_wh
  SCHEDULE = '1 minute'
AS
  alter external table iot_device_delta refresh;

execute task refresh_iot_table;

select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'refresh_iot_table'));
    

-- Create task to load changed records

CREATE or replace task load_iot_snf
  WAREHOUSE = lab_s_wh
  SCHEDULE = '1 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('iot_cdc_stream')
AS
  INSERT INTO raw_iot_dbx_to_snf(iotdata) SELECT VALUE FROM iot_cdc_stream WHERE METADATA$ACTION = 'INSERT';

alter task load_iot_snf resume;

execute task load_iot_snf;

show tasks;

select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'load_iot_snf'));
    
    
alter task load_iot_snf resume;


-- check if stream now has data
select * from iot_cdc_stream;
-- check data in the target table
select count(*) from raw_iot_dbx_to_snf;

-- run task manually if required
-- execute task load_iot_snf;

-- Read target table
select * from raw_iot_dbx_to_snf limit 10;


select 
    iotdata:"user_id"::number(38,0) user_id,
    iotdata:"miles_walked"::Float miles_walked,
    iotdata:"num_steps"::integer num_steps,
    iotdata:"timestamp"::timestamp time_stamp,
    iotdata:"calories_burnt"::Float calories_burnt,
    iotdata:"device_id"::number(38,0) device_id
from 
    raw_iot_dbx_to_snf
limit 10;


-- Suspend all tasks

alter task refresh_ext_table suspend;
alter task load_snf suspend;
