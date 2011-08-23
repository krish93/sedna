#!/bin/bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/sedna-config.sh

usage="Usage: sedna-daemon.sh (start|stop) <sedna-command> <args...>"

# get arguments

#default value
sednaScript="$SEDNA_HOME"/bin/sedna

if [ "--script" == "$1" ]
  then
    shift
    sednaScript=$1
    shift
fi
startStop=$1
shift
command=$1
shift

if [ -f "${SEDNA_CONF_DIR}/sedna-env.sh" ]; then
	. "${SEDNA_CONF_DIR}/sedna-env.sh"
fi

if [ "$SEDNA_LOG_DIR" = "" ]; then
	export SEDNA_LOG_DIR="$SEDNA_HOME/logs"
fi

mkdir -p "$SEDNA_LOG_DIR"

if [ "$SEDNA_PID_DIR" = "" ]; then
	SEDNA_PID_DIR=/tmp
fi

log=$SEDNA_LOG_DIR/sedna-$command-$HOSTNAME.log
pid=$SEDNA_PID_DIR/sedna-$command.pid
zkpid=$SEDNA_PID_DIR/zk.pid
sedpid=$SEDNA_PID_DIR/sed.pid
MEMCACHED_PORT=11211
MEMCACHED_SIZE=2048m

case $startStop in
	
	(start)
	
		mkdir -p "$SEDNA_PID_DIR"
		
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo $command running as process `cat $pid`. Stop it first.
				exit 1
			fi
		fi
		echo starting Memcached on port $MEMCACHED_PORT
		nohup ./memcached/memcached -p $MEMCACHED_PORT -m $MEMCACHED_SIZE -d </dev/null 1>/dev/null 2>&1 &
		
		echo starting $sednaScript $command
		cd "$SEDNA_HOME"
		nohup $sednaScript $command "$@" > "$log" 2>&1 < /dev/null &
		echo $! > $pid
		sleep 1;
		;;
		
	(stop)
		 
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo stopping $command
				kill `cat $pid`
			else
				echo no $command to stop
			fi
		else
			echo no $command to stop
		fi
		
		echo stoping Memcached
		pkill -9 memcached
		
		;;
		
    (zktest)
		
		echo starting ZooKeeper Test Suit...
		cd "$SEDNA_HOME"
		nohup $sednaScript $command "$@" > "$log" 2>&1 < /dev/null &
                echo $! > $zkpid
                ;;

    (sdtest)

		echo starting Sedna Write/Read Test Suit...
		cd "$SEDNA_HOME"
		nohup $sednaScript $command "$@" > "$log" 2>&1 < /dev/null &
                echo $! > $sedpid
                ;;
	(*)
		echo $usage
		exit 1
		;;
esac
