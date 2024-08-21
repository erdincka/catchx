#!/usr/bin/env bash

set -euo pipefail

# echo ${CLUSTER_IP}
# echo ${MAPR_USER}
# echo ${MAPR_PASS}

[ -f /root/.ssh/id_rsa ] || ssh-keygen -t rsa -b 2048 -f /root/.ssh/id_rsa -q -N ""

# This might be useful for later commands
ssh-keygen -f "/root/.ssh/known_hosts" -R ${CLUSTER_IP}
sshpass -p "${MAPR_PASS}" ssh-copy-id "${MAPR_USER}@${CLUSTER_IP}"

# TODO: no need to copy this, just configure with "configure.sh -c -N demo ..."
# scp $MAPR_USER@$CLUSTER_IP:/opt/mapr/conf/mapr-clusters.conf /opt/mapr/conf/
