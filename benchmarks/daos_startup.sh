#!/bin/bash

#source /home1/06753/soychan/work2/4NODE/BUILDS/files/env_daos

rm -f /tmp/daos_agent.log
rm -f /tmp/daos_client.log

export DAOS_AGENT_LOG=/tmp/daos_agent.log

export DAOS_AGENT_CONF=`sed 's/control/agent/g' <<< "$daos_config"`

export DAOS_AGENT_DIR=/tmp/daos_agent/

mkdir -p $DAOS_AGENT_DIR

pkill daos_agent

#START_TIME=$SECONDS
daos_agent -i -o $DAOS_AGENT_CONF -l $DAOS_AGENT_LOG -s ${DAOS_AGENT_DIR} &
#ELAPSED_TIME=$(($SECONDS - $START_TIME))
#echo "daos agent start time: $ELAPSED_TIME"
