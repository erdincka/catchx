#!/usr/bin/env bash

set -euo pipefail

## Ensure user
export MAPR_GROUP=mapr \
    MAPR_HOME=/home/mapr \
    MAPR_UID=5000
id mapr || useradd -u ${MAPR_UID} -U -m -d ${MAPR_HOME} -s /bin/bash -G sudo ${MAPR_USER}
echo "${MAPR_USER}:${MAPR_PASS}" | chpasswd
echo "root:${MAPR_PASS}" | chpasswd
[ -f /etc/sudoers.d/${MAPR_USER} ] || echo "${MAPR_USER} ALL=(ALL) NOPASSWD: ALL" | tee /etc/sudoers.d/${MAPR_USER}

[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -q -N ""

# remove old entries
ssh-keygen -f "~/.ssh/known_hosts" -R ${CLUSTER_IP} || true # ignore errors/not-found
sshpass -p "${MAPR_PASS}" ssh-copy-id -o StrictHostKeyChecking=no "${MAPR_USER}@${CLUSTER_IP}"

scp -o StrictHostKeyChecking=no $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/ssl_truststore* /opt/mapr/conf/

/opt/mapr/server/configure.sh -c -secure -N ${CLUSTER_NAME} -C ${CLUSTER_IP}

echo "Finished configuring MapR"

scp -o StrictHostKeyChecking=no ${MAPR_USER}@${CLUSTER_IP}:/opt/mapr/conf/maprkeycreds.* /opt/mapr/conf/
scp -o StrictHostKeyChecking=no ${MAPR_USER}@${CLUSTER_IP}:/opt/mapr/conf/maprtrustcreds.* /opt/mapr/conf/
scp -o StrictHostKeyChecking=no ${MAPR_USER}@${CLUSTER_IP}:/opt/mapr/conf/maprhsm.conf /opt/mapr/conf/

### Update ssl conf for hadoop
if grep hadoop.security.credential.provider.path /opt/mapr/conf/ssl-server.xml ; then
  echo "Skip /opt/mapr/conf/ssl-server.xml"

else
  echo "Adding property to /opt/mapr/conf/ssl-server.xml"

  grep -v "</configuration>" /opt/mapr/conf/ssl-server.xml > /tmp/ssl-server.xml

  echo """
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/opt/mapr/conf/maprkeycreds.jceks,localjceks://file/opt/mapr/conf/maprtrustcreds.jceks</value>
  <description>File-based key and trust store credential provider.</description>
</property>

</configuration>
""" >> /tmp/ssl-server.xml

  mv /tmp/ssl-server.xml /opt/mapr/conf/ssl-server.xml

fi

# create user ticket
echo ${MAPR_PASS} | maprlogin password -user ${MAPR_USER}

# (Re-)Mount /mapr
[ -d /mapr ] && umount -l /mapr || true # ignore errors (no dir or not mounted)
[ -d /mapr ] || mkdir /mapr

mount -t nfs -o nolock,soft ${CLUSTER_IP}:/mapr /mapr

echo "Cluster configuration is complete"
