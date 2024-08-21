#!/usr/bin/env bash

set -euo pipefail

[ -f /root/.ssh/id_rsa ] || ssh-keygen -t rsa -b 2048 -f /root/.ssh/id_rsa -q -N ""

ssh-keygen -f "/root/.ssh/known_hosts" -R ${CLUSTER_IP} || true # ignore errors/not-found
sshpass -p "${MAPR_PASS}" ssh-copy-id -o StrictHostKeyChecking=no "${MAPR_USER}@${CLUSTER_IP}"

scp -o StrictHostKeyChecking=no $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/ssl_truststore /opt/mapr/conf/

/opt/mapr/server/configure.sh -c -secure -N demo -C $CLUSTER_IP

echo "Finished configuring MapR"

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

scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprkeycreds.* /opt/mapr/conf/
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprtrustcreds.* /opt/mapr/conf/
scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/maprhsm.conf /opt/mapr/conf/

echo "Cluster configuration is complete"
