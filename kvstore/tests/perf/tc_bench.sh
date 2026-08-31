#!/bin/bash
# Run on Master VM. Starts echo server, waits for client from Slave, reports TC counters.

set -e
DIR=/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
PASS=2983372202
PORT=15998

# Setup TC
echo $PASS | sudo -S tc qdisc del dev ens33 clsact 2>/dev/null || true
echo $PASS | sudo -S rm -f /sys/fs/bpf/tc_clone
echo $PASS | sudo -S tc qdisc add dev ens33 clsact
echo $PASS | sudo -S bpftool prog load $DIR/tc_clone.bpf.o /sys/fs/bpf/tc_clone type classifier
echo $PASS | sudo -S tc filter add dev ens33 ingress bpf object-pinned /sys/fs/bpf/tc_clone direct-action
echo $PASS | sudo -S bpftool map update name cfg key 0 0 0 0 value 1 0 0 0
echo $PASS | sudo -S bpftool map update name cfg key 1 0 0 0 value 184 61 0 0
echo $PASS | sudo -S bpftool map update name cfg key 2 0 0 0 value 4 0 0 0
echo "TC_READY"

# Start echo server
$DIR/tiny_echo $PORT &
ECHO_PID=$!
sleep 1
echo "ECHO_READY pid=$ECHO_PID port=$PORT"

# Signal caller that we're ready
echo "READY"
