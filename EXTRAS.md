# Optional cluster extras

**None of this is required to run CatchX.** The demo needs only the cluster REST
API, the Data Access Gateway, NFS and the object store — see the prerequisites
in [README.md](./README.md). These notes are here for building on the demo, or
for setting the cluster up in ways the demo does not require.

## Driving the pipeline from a scheduler

Install the Spark and Airflow packages if you want to run the same steps from a
scheduler rather than from the app. The gold tier is Delta Lake in the global
namespace, so anything Delta-aware can read it — target
`/catchx-demo/gold` directly.

```bash
dnf install mapr-spark mapr-spark-master mapr-spark-historyserver mapr-spark-thriftserver
dnf install mapr-airflow-webserver mapr-airflow-scheduler mapr-airflow mapr-nifi

cp /opt/mapr/spark/spark-3.3.3/conf/workers.template /opt/mapr/spark/spark-3.3.3/conf/workers
/opt/mapr/server/configure.sh -R
export SPARK_HOME=/opt/mapr/spark/spark-3.3.3
```

As the `mapr` user, so the master can reach its workers:

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
ssh-copy-id <worker_host>
```

Then, as root:

```bash
$SPARK_HOME/sbin/start-workers.sh
```

Set the NiFi and Airflow credentials before first use:

```bash
/opt/mapr/nifi/nifi-1.19.1/bin/nifi.sh set-single-user-credentials admin <your password>
airflow users create --role Admin --username admin --email admin \
  --firstname admin --lastname admin --password <your password>
maprcli node services -name airflow-webserver -action restart -nodes $(hostname -f)
```

Airflow otherwise configures `mapr`/`mapr` as its default account.

## NFSv4

CatchX mounts the global namespace over **NFSv3** (`mapr-nfs`), which is what the
client configuration step does. To use NFSv4 (`mapr-nfs4server`) instead, change
the mount options in `backend/routes/cluster.py` to match, and set `sectype` to
`sys` if you are not using Kerberos.

See the [known issues](https://docs.ezmeral.hpe.com/datafabric/77/get_started/known_issues.html?#concept_kg5_cxs_zwb__section_w2t_ntm_n1c)
for the NFSv4 caveats.

### An external NFS server

Without Kerberos or ID mapping. `no_root_squash` lets root on the client act as
root on the server — do not use it in production. `insecure` allows client port
numbers above 1024, without which you get "operation not permitted".

`/etc/exports`:

```bash
/export	*(rw,fsid=0,sec=sys,insecure_locks,insecure,no_subtree_check,sync,no_root_squash)

/export/users *(rw,sec=sys,nohide,insecure_locks,insecure,no_subtree_check,sync,no_root_squash)
/export/server *(rw,sec=sys,nohide,insecure_locks,insecure,no_subtree_check,sync,no_root_squash)
```

Create bind mounts for the pseudo paths:

```bash
mount --bind /home /export/users/
mount --bind /srv /export/server/
```

And test:

```bash
mount -t nfs4 -o proto=tcp,nolock,sec=sys <nfs-server-ip>:/ /mnt/
```
