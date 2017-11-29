#!/bin/bash -e
# Sync code to all nodes in EMR cluster.

SRC_FOLDER=$1

if [ -z "$SRC_FOLDER" ]; then
    echo "Must specify source folder."
    exit 1
fi

# Sync each slave node in parallel.
echo "Syncing slaves"
pids=""
fail=0
yarn node -list | grep -v Node |
{
    # Need to use command grouping according to
    # yarn node -list outputs in the following format
    while read node; do
        HOST=`echo $node | cut -d ' ' -f 1 | cut -d ':' -f 1`
        echo "Syncing $HOST"
        ssh -t -o StrictHostKeyChecking=no hadoop@$HOST /home/hadoop/pyspark/config/sync_node.sh $SRC_FOLDER &
        pids="$pids $!"
    done

    for pid in $pids; do
        wait $pid || let "fail+=1"
    done

    if [ "$fail" != "0" ]; then
        echo "$fail slaves sync failed."
        exit 1
    fi
}

# Sync master node.
/home/hadoop/pyspark/config/sync_node.sh $SRC_FOLDER