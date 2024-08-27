# Using Hive for Delta


## Create Hive table for customers

```sql
DROP TABLE hiveview.default.customers
```

```sql
CREATE TABLE hiveview.default.customers (
    _id varchar,
    blood_group varchar,
    company varchar,
    country varchar,
    country_code varchar,
    county varchar,
    iso3166_2 varchar,
    job varchar,
    sex varchar,
    score BIGINT
  )
  WITH (
    format = 'PARQUET',
    external_location = 'file:///mnt/datasources/datafabric/<cluster_name>/app/gold/customers/')

```

```sql
SELECT * FROM hiveview.default.customers LIMIT 10
```

## Create Hive table for transactions

```sql
DROP TABLE hiveview.default.transactions
```

```sql
CREATE TABLE hiveview.default.transactions (
    _id varchar,
    amount bigint,
    category varchar,
    transaction_date double,
    sender_account varchar,
    receiver_account varchar,
    fraud boolean
  )
  WITH (
    format = 'PARQUET',
    external_location = 'file:///mnt/datasources/datafabric/<cluster_name>/app/gold/transactions/')

```

```sql
SELECT * FROM hiveview.default.transactions LIMIT 10
```
