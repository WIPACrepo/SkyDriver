#!/bin/bash
set -euo pipefail
set -x  # turn on debugging
set -e  # exit on fail

echo -n "Nodes "
kubectl get nodes | wc -l

lines=$(kubectl get pods -n icecube-skymap-scanner)

echo -n "Pods Running "
echo -n $(echo "$lines" | grep skyscan-worker.*Running | wc -l)
echo -n "/"
echo -n "$lines" | grep skyscan-worker | wc -l

# echo ""
# echo "$lines" | grep skyscan-worker.*Running | head

echo ""
jobs=$(echo "$lines" | grep skyscan-worker | sed 's/-/ /g' | awk '{ print $3 }' | sort | uniq)
for j in $jobs; do
    echo -n $j
    echo -n " Running "
    echo -n $(echo "$lines" | grep skyscan-worker.*$j.*Running | wc -l)
    echo -n "/"
    echo -n "$lines" | grep skyscan-worker.*$j | wc -l
done
