
# Data Fabric demo app

`docker run -d -t --name catchx -p 3000:3000 --cap-add SYS_ADMIN erdincka/catchx`

It should clone the app and start it right away, you can monitor the logs `docker logs -f catchx`, or get into the container and kill/restart the app if you want.

`docker exec -it catchx bash`

```shell
cd app
python3 main.py
```


## Fraud Detection use case demo with Ezmeral Data Fabric

Uses Data Fabric to process incoming transactions (faked) via streams, and then storing relevant information into JSON Tables (a simple ETL step). Final step is to simulate a fraud detection ML model inferencing on incoming messages.

This application expects certain runtime environment (container) with required libraries and OS environment variables configured. This is done in the app image available on [GitHub](https://github.com/erdincka/catchx-image).

Before running the demo, you have to configure the app to access the cluster that you will run the demo.

App uses `/app/*` volumes on the connected cluster, so do not run this app on a cluster which already has this path/volume configured.

Follow the steps to walk through the demo.

You can run all steps as many times as you like, especially "produce" and "process" steps should be run multiple times.

Once completed, you can delete the stream and the volume to get rid of all app-created artifacts on the Data Fabric cluster.

You can also delete the stream, and then re-start from Step 2, so you can have clear metrics/monitoring on the generated charts.


# REQUIREMENTS

Setup Data Fabric cluster with Spark, create a user with volume, table and stream creation rights, as well as with audit monitoring (for monitoring charts). Or for demo environments, simply use `mapr` user.


Setup MariaDB (possibly used for Hive too), on Data Fabric cluster, and create/allow a user with remote connection, DB creation and permissions.

## Install MariaDB

Follow the doc.

## Install Hive metastore

Install hive and hive metastore

`yum install mapr-hive mapr-hiveserver2 mapr-hivemetastore`

### Download MariaDB packages:

<!-- Not sure why Hive uses 2.5.4 version and NiFi below uses 3.4.1 version -->
`wget https://downloads.mariadb.com/Connectors/java/connector-java-2.5.4/mariadb-java-client-2.5.4.jar`
`wget https://downloads.mariadb.com/Connectors/java/connector-java-2.5.4/mariadb-java-client-2.5.4-sources.jar`
`wget https://downloads.mariadb.com/Connectors/java/connector-java-2.5.4/mariadb-java-client-2.5.4-javadoc.jar`

Copy files to /opt/mapr/hive/hive-3.1.3/lib/

Install MariaDB connector for NiFi
`wget https://dlm.mariadb.com/3852266/Connectors/java/connector-java-3.4.1/mariadb-java-client-3.4.1.jar -O /mapr/fraud/user/root/mariadb-java-client-3.4.1.jar`

Reconfigure cluster:

`/opt/mapr/server/configure.sh -R`

Follow the doc to set up database:

Edit `/opt/mapr/hive/hive-3.1.3/conf/hive-site.xml`

Initialize: `/opt/mapr/hive/hive-3.1.3/bin/schematool -dbType mysql -initSchema`


## Create Dashboard DBs

```shell

mysql -u root -p

CREATE DATABASE fraud;

CREATE USER 'catchx'@'%' IDENTIFIED BY 'catchx';

GRANT ALL ON fraud.* TO 'catchx'@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;

exit

```


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


`/opt/mapr/nifi/nifi-1.19.1/bin/nifi.sh set-single-user-credentials admin Admin123.Admin123.`


`airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin`


```bash
maprcli node services -name airflow-webserver  -action restart -nodes `hostname -f`
```

--- or airflow configures mapr/mapr as default user/password ---


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

`mount -t nfs4 -o proto=tcp,nolock,sec=sys 10.2.50.18:/ /mnt/`


<!-- No longer used, pyspark is not used -->
## Container configuration for Spark (Not used)

In `/opt/mapr/conf/ssl-server.xml`

```xml
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/opt/mapr/conf/maprkeycreds.jceks,localjceks://file/opt/mapr/conf/maprtrustcreds.jceks</value>
  <description>File-based key and trust store credential provider.</description>
</property>
```

`scp mapr@10.2.50.35:/opt/mapr/conf/maprkeycreds.* /opt/mapr/conf/`
`scp mapr@10.2.50.35:/opt/mapr/conf/maprtrustcreds.* /opt/mapr/conf/`
`scp mapr@10.2.50.35:/opt/mapr/conf/maprhsm.conf /opt/mapr/conf/`
