#!/bin/bash

# Run a shell command on all nodes.

usage="Usage: nodes.sh command"

if [ $# -le 0 ]; then
	echo $usage
	exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/sedna-config.sh

NODELIST=$SEDNA_CONF_DIR/nodes

if [ -f "${SEDNA_CONF_DIR}/sedna-env.sh" ]; then
	. "${SEDNA_CONF_DIR}/sedna-env.sh"
fi

if [ "$NODELIST" = "" ]; then
	if [ "SEDNA_NODES" == "" ]; then
		export NODELIST="${SEDNA_CONF_DIR}/nodes"
	else
		export NODELIST="${SEDNA_NODES}"
	fi
fi

nid=1

for node in `cat "$NODELIST" | sed "s/#.*$//;/^$/d"`; do
	ssh $node $"${@// /\\ }" $nid\
		2>&1 | sed "s/^/$node: /" &
	nid=`expr $nid + 1`
done


wait