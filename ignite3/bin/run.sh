#!/usr/bin/env bash

# Use this script to run benchmarks with embedded AI3 node
# Has to be run from the "target/ycsb-ignite3-binding-*" directory

set -e
set -x

SCRIPT_DIR=$(cd $(dirname "$0"); pwd)
SCRIPT_DIR_PARENT=$(dirname "$SCRIPT_DIR")
IGNITE_HOME=${IGNITE_HOME:=$SCRIPT_DIR_PARENT}

IGNITE_DB_HOME="${IGNITE_HOME}/ignite3-db-3.0.0-SNAPSHOT"
if [ ! -d $IGNITE_DB_HOME ]; then
  echo "Cannot find ${IGNITE_DB_HOME}"
  exit 1
fi

IGNITE_CLI_HOME="${IGNITE_HOME}/ignite3-cli-3.0.0-SNAPSHOT"
if [ ! -d $IGNITE_CLI_HOME ]; then
  echo "Cannot find ${IGNITE_CLI_HOME}"
  exit 1
fi

declare -a BINDINGS=(
  "ignite3"
  "ignite3-sql"
  "ignite3-jdbc"
)

declare -a WORKLOADS=(
  "./workloads/workloadc"
  "../../workloads/external_node"
)

GLOBAL_ARGS=
#GLOBAL_ARGS="-p operationcount=10 -p recordcount=10 -p debug=true"

function runYscb() {
  local mode=$1
  local binding=$2
  local logFileName=$3
  local addArgs=$4

  echo ">>> Running YCSB for ${binding}, mode: ${mode}, args: ${addArgs}"

  local workloads=
  for wl in "${WORKLOADS[@]}"; do
    workloads="${workloads} -P ${wl}"
  done

  bin/ycsb.sh "${mode}" "${binding}" \
      -s \
      "${workloads}" \
      "${GLOBAL_ARGS}" \
      "${addArgs}" \
      2>&1 | tee -a "${logFileName}"

  # Save YCSB results in a separate file
  grep -E -e "^\[[A-Z]" -e "Command line" "${logFileName}" > "${logFileName}.res.csv"
}

function runIgnite() {
  $IGNITE_DB_HOME/bin/ignite3db start
}

function activateIgnite() {
  $IGNITE_CLI_HOME/bin/ignite3 "cluster init --cluster-name myCluster1 --cmg-node defaultNode --meta-storage-node defaultNode"
  $IGNITE_CLI_HOME/bin/ignite3 "cluster status"
}

function stopIgnite() {
  $IGNITE_DB_HOME/bin/ignite3db stop
}

function runBinding() {
  local binding=$1

  echo ">> Running YCSB for ${binding}"

  timestamp=$(date +%F-%H-%M-%S)
  logFileName="../../${timestamp}-${binding}.log"

  runIgnite
  activateIgnite

  runYscb "load" "${binding}" "${logFileName}" "-threads 4"
  runYscb "run"  "${binding}" "${logFileName}" "-threads 1"
  runYscb "run"  "${binding}" "${logFileName}" "-threads 4"

  stopIgnite
}

for binding in "${BINDINGS[@]}"; do
  runBinding "${binding}"
done
