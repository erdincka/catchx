#!/usr/bin/env bash

f=$(grep $CLUSTER_NAME /opt/mapr/conf/mapr-clusters.conf)
nf=$(grep -v $CLUSTER_NAME /opt/mapr/conf/mapr-clusters.conf)
echo -e "$f\n$nf" > /opt/mapr/conf/mapr-clusters.conf
/opt/mapr/server/configure.sh -c -secure -N $CLUSTER_NAME -C $CLUSTER_IP

### Update ssl conf
creds_configured=$(grep -s "hadoop.security.credential.provider.path" /opt/mapr/conf/ssl-server.xml)
if [ $? == 1 ]; then
  echo "Adding property to /opt/mapr/conf/ssl-server.xml"
  xml_file=$(grep -v "</configuration>" /opt/mapr/conf/ssl-server.xml)

  read -r -d '' ADDSECTION <<- EOM
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/opt/mapr/conf/maprkeycreds.jceks,localjceks://file/opt/mapr/conf/maprtrustcreds.jceks</value>
  <description>File-based key and trust store credential provider.</description>
</property>
</configuration>
EOM
  echo "${xml_file}" "${ADDSECTION}" > /opt/mapr/conf/ssl-server.xml

else
  echo "Skip /opt/mapr/conf/ssl-server.xml"
fi

# ssh-copy-id $MAPR_USER@$CLUSTER_IP
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprkeycreds.* /opt/mapr/conf/
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprtrustcreds.* /opt/mapr/conf/
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprhsm.conf /opt/mapr/conf/
