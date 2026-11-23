#!/usr/bin/env bash

#if [ $# -ne 1 ]; then
#    echo "Usage: $0 filename"
#    exit 1
#fi

export LOG_LEVEL=debug

filename=$1
LOG_FILE=./test_log/$filename
i=1
while [ $i -le 100 ]
do
    rm -rf /tmp/*test-raftstore*
    time make project2 > $LOG_FILE 2>&1
    #time GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B > $LOG_FILE 2>&1
    if [ $? -ne 0 ]; then
        echo "project2 test $i fail" >> $LOG_FILE
        break
    fi

    if grep -qE "\bfail\b|\bFAIL\b" $LOG_FILE; then
        echo "project2 test $i fail" >> $LOG_FILE
        break
    fi

    time make project3 > $LOG_FILE 2>&1
    if [ $? -ne 0 ]; then
        echo "project3 test $i fail" >> $LOG_FILE
        break
    fi

    if grep -qE "\bfail\b|\bFAIL\b" $LOG_FILE; then
        echo "project3 test $i fail" >> $LOG_FILE
        break
    fi

    echo "test $i times"
    echo "test $i times" >> $LOG_FILE
    echo "" >> $LOG_FILE
    let i++
done