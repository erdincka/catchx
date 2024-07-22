#!/usr/bin/env bash

f=$(grep $CLUSTER_NAME /opt/mapr/conf/mapr-clusters.conf)
nf=$(grep -v $CLUSTER_NAME /opt/mapr/conf/mapr-clusters.conf)
echo -e "$f\n$nf" > /opt/mapr/conf/mapr-clusters.conf
/opt/mapr/server/configure.sh -c -secure -N $CLUSTER_NAME -C $CLUSTER_IP
# ssh-copy-id $MAPR_USER@$CLUSTER_IP
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprkeycreds.* /opt/mapr/conf/
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprtrustcreds.* /opt/mapr/conf/
