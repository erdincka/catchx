
# Data Fabric demo app

`docker run -d -t --name catchx -p 3000:3000 --cap-add sys_admin erdincka/catchx`

`docker exec -it catchx bash`

```shell
cd app
python3 main.py
```


# Fraud Detection use case demo with Ezmeral Data Fabric

Uses Data Fabric to process incoming transactions (faked) via streams, and then storing relevant information into JSON Tables (a simple ETL step). Final step is to simulate a fraud detection ML model inferencing on incoming messages.

This application expects certain runtime environment (container) with required libraries and OS environment variables configured. This is done in the app image available on [GitHub](https://github.com/erdincka/catchx-image).

Before running the demo, ensure that you have Ezmeral Data Fabric connectivity is established and a user ticket is generated for the user defined in `MAPR_USER` environment variable.

App uses `/apps/catchx_*` volumes on the connected cluster, so do not run this app on a cluster which already has this path/volume configured.

Follow the steps to walk through the demo.

You can run all steps as many times as you like, especially "produce" and "process" steps should be run multiple times.

Once completed, you can delete the stream and the volume to get rid of all app-created artifacts on the Data Fabric cluster.

You can also delete the stream, and then re-start from Step 2, so you can have clear metrics/monitoring on the generated charts.

NOTE:

You can open Log window by selecting the hamburger menu on the footer, and select to see "Debug" messages using the switch at the top-right corner.


# REQUIREMENTS

Setup Data Fabric cluster with Spark, create a user with volume, table and stream creation rights, as well as with audit monitoring (for monitoring charts). Or for demo environments, simply use `mapr` user.

Setup MySQL (possibly used for Hive too), on Data Fabric cluster, and create/allow a user with remote connection, DB creation and permissions.

```shell

mysql -u root -p

CREATE DATABASE fraud;

CREATE USER 'catchx'@'%' IDENTIFIED BY 'catchx';

GRANT ALL ON fraud.* TO 'catchx'@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;

exit

```
