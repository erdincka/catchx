## Nifi, Airflow and Spark (optional)

Setup Airflow and Spark packages if you plan to use Airflow

`dnf install mapr-spark mapr-spark-master mapr-spark-historyserver mapr-spark-thriftserver`

`dnf install mapr-airflow-webserver mapr-airflow-scheduler mapr-airflow mapr-nifi`

`cp /opt/mapr/spark/spark-3.3.3/conf/workers.template /opt/mapr/spark/spark-3.3.3/conf/workers`

`/opt/mapr/server/configure.sh -R`

`export SPARK_HOME=/opt/mapr/spark/spark-3.3.3`

### Run these as mapr user
`ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa`
`ssh-copy-id <worker_host>`

As root:

`$SPARK_HOME/sbin/start-workers.sh`


`/opt/mapr/nifi/nifi-1.19.1/bin/nifi.sh set-single-user-credentials admin <your password>`


`airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password <your password>`


```bash
maprcli node services -name airflow-webserver  -action restart -nodes `hostname -f`
```

--- or airflow configures mapr/mapr as default user/password ---

**NOTE: NiFi template is configured to use Mysql/MariaDB for Hive tables, though it is removed from the demo setup. TODO: update NiFi processor to use Deltalake in Gold tier, instead of the RDBMS (mysql).**


## Setting up NFSv4 (optional)

Change sectype to sys if not using Kerberos

https://docs.ezmeral.hpe.com/datafabric/77/get_started/known_issues.html?#concept_kg5_cxs_zwb__section_w2t_ntm_n1c


### External NFS Server

Not using Kerberos and ID Mapping

`no_root_squash` allows root user in client to act like root user in server (do not use in production).
`insecure` enables use of port numbers above 1024 for clients - otherwise you'll get 'operation not permitted' errors.

`/etc/exports` file content:

```bash
/export	*(rw,fsid=0,sec=sys,insecure_locks,insecure,no_subtree_check,sync,no_root_squash)

/export/users *(rw,sec=sys,nohide,insecure_locks,insecure,no_subtree_check,sync,no_root_squash)
/export/server *(rw,sec=sys,nohide,insecure_locks,insecure,no_subtree_check,sync,no_root_squash)
```

You should create bind mounts for users & server psudo paths:

```bash
mount --bind /home /export/users/
mount --bind /srv /export/server/
```

And test it:

`mount -t nfs4 -o proto=tcp,nolock,sec=sys <nfs-server-ip>:/ /mnt/`
