use role accountadmin;
create database if not exists demo;
use database demo;
create schema if not exists azure;
use schema demo.azure;

grant all on database demo to sysadmin;
grant all on schema azure to sysadmin;

-- ** Very Important steps below **
-- Follow these guides for Azure integration, you need to login into Azure Portal
-- https://docs.snowflake.com/en/user-guide/data-load-azure-config.html
-- https://docs.snowflake.com/en/sql-reference/sql/create-stage.html
-- https://docs.snowflake.com/en/user-guide/data-load-azure-copy.html

-- There are two ways to integrate with Azure, SAS token and storage integeration. We will use SI
-- search for "tenant properties" in azure portal top search bar to get tenant ID
-- your path for container for delta lake might look like this 'azure://myaccount.blob.core.windows.net/container1/path1'

create storage integration iotdata_si
type = external_stage
storage_provider = azure
enabled = true
azure_tenant_id = '<Azure tenant ID>'
storage_allowed_locations = ('azure://<storage acct>.blob.core.windows.net/<delta tables location>/')
;

desc storage integration iotdata_si;

-- grab the 
-- AZURE_CONSENT_URL
-- AZURE_MULTI_TENANT_APP_NAME
-- Follow this step - https://docs.snowflake.com/en/user-guide/data-load-azure-config.html#step-2-grant-snowflake-access-to-the-storage-locations

grant create stage on schema azure to role sysadmin;
grant usage on integration iotdata_si to role sysadmin;

