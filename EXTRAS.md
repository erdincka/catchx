# Optional cluster extras

**None of this is required to run CatchX.** The demo needs only the cluster REST
API, the Data Access Gateway, NFS and the object store — see the prerequisites
in [README.md](./README.md). These notes are here for building on the demo, or
for setting the cluster up in ways the demo does not require.

## NiFi, Airflow and Spark

Install Airflow and Spark packages if you want to drive the same pipeline from
a scheduler rather than the app.

`dnf install mapr-spark mapr-spark-master mapr-spark-historyserver mapr-spark-thriftserver`

`dnf install mapr-airflow-webserver mapr-airflow-scheduler mapr-airflow mapr-nifi`

`cp /opt/mapr/spark/spark-3.3.3/conf/workers.template /opt/mapr/spark/spark-3.3.3/conf/workers`

`/opt/mapr/server/configure.sh -R`

`export SPARK_HOME=/opt/mapr/spark/spark-3.3.3`

## Run these as mapr user

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

> **Note:** earlier versions of this demo shipped a NiFi template and an Airflow
> DAG that wrote Hive tables via MySQL/MariaDB. Both were removed — the pipeline
> now writes Delta Lake directly to the gold tier, so an RDBMS is no longer part
> of it. If you want to rebuild that integration, target the gold Delta tables
> under `/catchx-demo/gold`.


## NFSv4 (optional)

CatchX mounts the global namespace over **NFSv3** (`mapr-nfs`), which is what the
client configuration step does by default. These notes cover NFSv4
(`mapr-nfs4server`) if you would rather use it — you would need to change the
mount options in `backend/routes/cluster.py` to match.

Change sectype to sys if not using Kerberos.

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
