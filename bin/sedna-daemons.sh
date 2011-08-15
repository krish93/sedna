#!/bin/bash

# Run a Sedna command on all nodes.

usage="Usage: sedna-daemons.sh [--script script] (start|stop) command args"

if [ $# -le 1 ]; then
	echo $usage
	exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/sedna-config.sh

exec "$bin/nodes.sh" cd "$SEDNA_HOME" \; "$bin/sedna-daemon.sh" "$@"