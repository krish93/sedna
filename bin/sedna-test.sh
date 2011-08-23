#!/bin/bash

# Start Sedna Test Suit.

echo "Please make sure the node running this script can ssh other nodes without password"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/sedna-config.sh

function print_usage(){
	echo "Usage: sedna-test COMMAND"
	echo "       where COMMAND is one of:"
	echo "    zk        run ZooKeeper w/r test "
    echo " sedna        run Sedna w/r test "
}

if [ $# = 0 ]; then
	print_usage
	exit
fi

COMMAND=$1
shift

if [ "$COMMAND" = "zk" ]; then
    "$SEDNA_COMMON_HOME"/bin/sedna-daemon.sh --script "$bin"/sedna zktest zktest 0
    "$SEDNA_COMMON_HOME"/bin/sedna-daemons.sh --script "$bin"/sedna zktest zktest
elif [ "$COMMAND" = "sedna" ]; then
    "$SEDNA_COMMON_HOME"/bin/sedna-daemon.sh --script "$bin"/sedna sdtest sdtest 0
    "$SEDNA_COMMON_HOME"/bin/sedna-daemons.sh --script "$bin"/sedna sdtest sdtest
fi
    