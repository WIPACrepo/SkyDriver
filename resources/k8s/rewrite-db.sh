#!/bin/bash
set -euo pipefail
set -ex

########################################################################################
#
# A script for rewriting the 'dev' environment's (k8s namespace) database
# with recent production data.
#
# Use this script before running the 'prod_tester/' test suit.
#
########################################################################################

if [ -z "${1-}" ] || [ -z "${2-}" ]; then
    echo "Usage: rewrite-db.sh BACKUP_DIR MONGO_PASS"
    exit 1
fi
BACKUP_DIR="${1#/}" # ex: 2025-01-14/
MONGO_PASS="$2"     # NOTE: pass in '$(echo "<B64-ENCODED-PASS-FROM-K8S-YAML>" | base64 --decode)'

########################################################################################
# use prod DB's backup

if [[ -d $BACKUP_DIR ]]; then
    echo "using existing db backup at $BACKUP_DIR"
else
    "ERROR: $BACKUP_DIR does not exist"
    exit 2
fi

########################################################################################
# write

# don't copy the backlog, otherwise, those pending scan will attempt to start
rm "$BACKUP_DIR"/SkyDriver_DB/ScanBacklog.* || true

# drop database
mongosh --port 27017 \
    -u skydriver_db \
    -p "$MONGO_PASS" \
    --authenticationDatabase admin \
    --eval "db.dropDatabase()" SkyDriver_DB ||
    (echo "do you need to run \'kubectl --kubeconfig=\$RBN_KUBECONFIG -n skydriver-dev port-forward svc/skydriver-dev-mongo 27017:27017\' ?" && exit 1)

# overwrite db
mongorestore --port 27017 \
    -u skydriver_db \
    -p "$MONGO_PASS" \
    --authenticationDatabase admin \
    --drop \
    --numInsertionWorkersPerCollection 1 \
    --verbose \
    "$BACKUP_DIR"
