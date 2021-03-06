#!/bin/bash

PID_FILE=/var/run/rerun.pid # need root permission to store in this directory
EXEC=/root/treatment/sludge_server.py # replace it with actual executable 
EXEC1=/root/treatment/waste_server.py
EXEC2=/root/treatment/server.py

function run() {
    # execute the program
    $EXEC &
    $EXEC1 &
    $EXEC2 &
    # save its PID
    echo $! > $PID_FILE
}

if [ -e $PID_FILE ]; then
    # check if program is still running
    pid=$(<$PID_FILE)

    # find the proper process
    ps --pid $pid|grep -q `basename $EXEC`

    if [ $? != 0 ]; then
        # not found - rerun
        run
    fi
else
    # no PID file - just execute
    run
fi
