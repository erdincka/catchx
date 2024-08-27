
# Data Fabric demo app


Standalone app can be run using following Docker command:

`docker run -d -t --name catchx -p 3000:3000 --cap-add SYS_ADMIN erdincka/catchx`

It should clone the app and start it right away, you can monitor the logs `docker logs -f catchx`, or get into the container and kill/restart the app if you want.

`docker exec -it catchx bash`

```shell
cd app
python3 main.py
```

You can also install the app itself on Ezmeral Unified Analytics platform as a third-party using [provided helm chart](./helm-package/catchx-0.0.3.tgz) and you can use [provided image](./helm-package/fraud-detection-app.jpg) as its icon.

Just follow the instructions from [Ezmeral documentation](https://docs.ezmeral.hpe.com/unified-analytics/15/ManageClusters/importing-applications.html).


## Fraud Detection pipeline demo with Ezmeral Data Fabric

This is not an accurate representation of a real fraud detection process, but rather an end-to-end demonstration of how a pipeline can be built using some of Ezmeral Data Fabric capabilities for a real-life scenario. We aim to highlight the flexibility and openness of Ezmeral Data Fabric as a converged data platform for various data types and choices of open-source ecosystem tools/frameworks.

The tools and frameworks used in the demo is selected with simplicity of their implementation in mind, but they are not meant to limit user's choice when it comes to real life implementation. Users are free to choose included or third-party tools, as Data Fabric supports various industry-standard protocols to read, process and store data.

The app shows the ingestion of transaction data (json) via Event Streams and batch customer data (csv files) into the fabric, and then sotring and processing them through their lifecycle inside the Fabric, using technologies such as NoSQL Document DBs or Iceberg tables. Then at the final stage we both simulate a fraud detection ML model inferencing on incoming messages as well as providing consolidated information as a Data Product that can be shared within the organisation either for Business Intelligence & Analytics or for other consumption methods through JSON/OJAI APIs.

Before running the demo, you have to configure the app to access the cluster that you will run the steps.

App uses `/app/*` volumes on the connected cluster, so do not run this app on a cluster which already has this path/volume configured.

Follow the steps to walk through the demo.

You can run all steps as many times as you like, especially "produce" and "process" steps can be run multiple times.

Once completed, you can delete the stream and the volume to get rid of all app-created artifacts on the Data Fabric cluster.

You can also delete the stream, and then re-start from Step 2, so you can have clear metrics/monitoring on the monitoring charts.


# REQUIREMENTS

Setup Data Fabric cluster following the instructions below, and optionally create a user with volume, table and stream creation rights. For isolated/standalone demo environments, you can simply use the cluster admin `mapr` user.

Data Fabric should have following packages installed and configured:

```bash
mapr-hivemetastore
mapr-kafka
mapr-nfs4server
mapr-data-access-gateway
mapr-hbase
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


`/opt/mapr/nifi/nifi-1.19.1/bin/nifi.sh set-single-user-credentials admin <your password>`


`airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password <your password>`


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

`mount -t nfs4 -o proto=tcp,nolock,sec=sys 10.1.1.18:/ /mnt/`


# Running Demo

## Initial configuration

Use the disconnected link icon to complete initial setup. This will require you to provide the host to connect to (any Data Fabric node) and then the credentials. It will update the settings and reconfigure the Data Fabric cluster with required settings.

You can use the settings cog to add features:

- (Optional) Provide S3 and NFS server endpoints. Use FQDN format to avoid certificate errors (though they are ignored in the demo).

- Provide S3 credentials taken from Object Store Access Keys page: https://docs.ezmeral.hpe.com/datafabric/78/administration/generating_s3_access_key.html

- (Optional) If you created the dashboard, enter its link (taken from Superset -> Dashboards -> Your Dashboard -> get permanent link from the dashboard's action button).

- (Optional) If you installed/configured Metadata Catalogue, provide its link in "Catalogue" text box.

- Create entities in the given sequence. Note and fix if there are any errors.

If the data/tables have gone too large or you would like to start from clear state, you can use "Delete All!" to delete the created tables, volumes and streams, and re-create them all again using "Create the Entities" section.

# Demo Flow

By default, you should see only "view" options for code and data. Turn on the "Go Live" switch to see action buttons.

Sample data for customers and transactions are already created by the initialisation step, but you can always add new records by using "Add" buttons.

- "Rocket" actions should be used to proceed for each step.

- "Preview" buttons for looking at the sample data at that specific tier,

- "Code" buttons are used for checking the actual python code that runs that action,

- "Secondary" buttons (teal colored) are used for optional/alternative steps.

In "live demo" mode, you would see the metrics and logs at the right and bottom of the page, respectively.

Monitoring metric collection is not enabled by default to preserve resources and app responsiveness. Once enabled (using the "Monitor" switch, it will query the data every few seconds and update the UI with the latest values.
