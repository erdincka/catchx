TODO:


- configure.sh yerine, 
    - selected cluster as first item in mapr-clusters.conf
    - copy maprtrustcreds.* 
    - copy maprkeycreds.*
    - ...?

- monitoring charts/tables - updates for consumed incoming txns

- checkfraudcode not configured yet

- check table types (bronze-txn = docdb, silver-txn = binary, silver-profiles = binary, silver-customers = docdb)

- move gold table to delta -> connect to superset