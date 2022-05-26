# SNOWFLAKE DELTA CONNECTOR DEMO

## Steps
1. If you dont have a delta lake in Azure, you can create one using the 00_Quick_Databricks_Delta~ dbc archive notebook. You will need to provide your storage account, SAS Token and container to store the delta lake artifacts
2. Next, import the sql scripts 01, 02 and 03 in Snowflake worksheet and follow the instructions in the comment to create external table using Delta connector. Worksheet 02 and 03 has an example of CDC (change data capture) from delta lake into snowflake table using task and streams 