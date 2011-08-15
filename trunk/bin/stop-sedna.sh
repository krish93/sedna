#!/bin/bash

# Stop Sedna service. Run on the master node

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/sedna-config.sh

"$bin"/sedna-daemon.sh --script "$bin"/sedna stop service
"$bin"/sedna-daemons.sh --script "$bin"/sedna stop service