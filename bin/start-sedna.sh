#!/bin/bash

# Start all Sedna daemons. Run this on master node

echo "Please make sure the node running this script can ssh other nodes without password"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/sedna-config.sh


"$SEDNA_COMMON_HOME"/bin/sedna-daemon.sh --script "$bin"/sedna start service
"$SEDNA_COMMON_HOME"/bin/sedna-daemons.sh --script "$bin"/sedna start service
