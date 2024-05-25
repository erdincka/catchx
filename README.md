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
