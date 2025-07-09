#!/bin/bash
set -euo pipefail
set -x  # turn on debugging
set -e  # exit on fail

if [ -z "${1-}" ] ; then
    echo "Usage: pod-logs.sh SCAN_ID"
    exit 1
fi


lines=$(kubectl get pods -n icecube-skymap-scanner | grep skyscan-worker.*$1)


echo ""
pods=$(echo "$lines" | awk '{ print $1 }')
for p in $pods; do
    kubectl logs $p -n icecube-skymap-scanner --all-containers=true
    break
done