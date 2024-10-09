
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

The tools and frameworks used in the demo are selected with simplicity of their implementation in mind, but they are not meant to limit user's choice when it comes to real life implementation. Users are free to choose included or third-party tools, as Data Fabric supports various industry-standard protocols to read, process and store data.

The app shows the ingestion of transaction data (json) via Event Streams and batch customer data (csv files) into the fabric, and then storing and processing them through their lifecycle inside the Fabric, using technologies such as NoSQL Document DBs or Iceberg tables. Then at the final stage we both simulate a fraud detection ML model inferencing on incoming messages as well as providing consolidated information as a Data Product that can be shared within the organisation either for Business Intelligence & Analytics or for other consumption methods through query APIs.

Before running the demo, you have to configure the app to access the cluster that you will run the steps.

App uses `/app/*` volumes on the connected cluster, so do not run this app on a cluster which already has this path/volume in use.

Follow the steps to walk through the demo.

You can run all steps as many times as you like, especially "produce" and "process" steps can be run multiple times.

Once completed, you can delete the streams and the volumes to get rid of all app-created artifacts on the Data Fabric cluster.

You can also delete the stream, and then re-start from Step 2, so you can have clear metrics/monitoring on the monitoring charts.


# REQUIREMENTS

Setup Data Fabric cluster following the instructions below, and optionally create a user with volume, table and stream creation rights. For isolated/standalone demo environments, you can simply use the cluster admin `mapr` user.

Data Fabric should have following packages installed and configured:

```bash
mapr-hivemetastore
mapr-kafka
# mapr-nfs4server
mapr-nfs
mapr-data-access-gateway
mapr-hbase
```

For additional features/functions, see [Extras](./EXTRAS.md).

# Running Demo

## Initial configuration

Use the disconnected link icon to complete initial setup. This will require you to provide the host details to connect to the Data Fabric node where Data Access Gateway service is running. It will update the app configuration, and create the /app volumes and streams on the Data Fabric cluster.

You can use the settings cog to add features:

- (Optional) Provide S3 and NFS server endpoints. Use FQDN format to avoid certificate errors (though they are ignored in the demo).

- Provide S3 credentials taken from Object Store Access Keys page: https://docs.ezmeral.hpe.com/datafabric/78/administration/generating_s3_access_key.html

- (Optional) If you created the dashboard, enter its link (taken from Superset -> Dashboards -> Your Dashboard -> get permanent link from the dashboard's action button).

- (Optional) If you installed/configured Metadata Catalogue, provide its link in "Catalogue" text box.

- Create entities in the given sequence. Note and fix if there are any errors.

If the data/tables have gone too large or you would like to start from clear state, you can use "Delete All!" to delete the created tables, volumes and streams, and re-create them from the initial connection page (connect/disconnect button).

## Additional Steps for the Dashboard

If you plan to use Superset dashboard for visualisation, follow the steps in [Hive for Deltalake setup in UA](./HiveForDelta.md) to create the Hive tables that uses the data in the Gold tier Delta Lake.

# Demo Flow

By default, you should see only "view" options for code and data. Turn on the "Go Live" switch to see action buttons.

Sample data for customers and transactions are already created by the initialisation step, but you can always add new records by using "Add" buttons.

- "Rocket" actions should be used to proceed for each step.

- "Preview" buttons for looking at the sample data at that specific tier,

- "Code" buttons are used for checking the actual python code that runs that action,

- "Secondary" buttons (teal colored) are used for optional/alternative steps.

In "live demo" mode, you would see the metrics and logs at the right and bottom of the page, respectively.

Monitoring metric collection is not enabled by default to preserve resources and app responsiveness. Once enabled (using the "Monitor" switch), it will query the data every few seconds and update the UI with the latest values.
