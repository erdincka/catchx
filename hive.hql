CREATE EXTERNAL TABLE customers ( 
 `_id` string, 
 name string, 
 address string, 
 account_number string,
 score string,
 transactions_sent array<STRUCT<
        amount:STRING,
        transaction_date:INT,
        receiver_account:INT>>,
 transactions_received array<STRUCT<
        amount:STRING,
        transaction_date:INT,
        receiver_account:INT>>) 
STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' 
TBLPROPERTIES("maprdb.table.name" = "/fraud/gold/combined","maprdb.column.id" = "_id");