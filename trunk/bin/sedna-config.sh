
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the sedna installation
export SEDNA_HOME=`dirname "$this"`/..
export SEDNA_COMMON_HOME="${SEDNA_HOME}"

# Allow alternate conf dir location.
SEDNA_CONF_DIR="$SEDNA_HOME/conf"
SEDNA_NICENESS=0

if [ -f "${SEDNA_CONF_DIR}/sedna-env.sh" ]; then
	. "${SEDNA_CONF_DIR}/sedna-env.sh"
fi

if [ "$JAVA_HOME" != "" ]; then
	JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
	echo "Error: JAVA_HOME is not set"
	exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m
CLASSPATH="${SEDNA_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
CLASSPATH=${CLASSPATH}:$SEDNA_COMMON_HOME/build/classes

IFS=

if [ -d "$SEDNA_COMMON_HOME/webapps" ]; then
	CLASSPATH=${CLASSPATH}:$SEDNA_COMMON_HOME
fi

for f in $SEDNA_COMMON_HOME/dist/*.jar; do
	CLASSPATH=${CLASSPATH}:$f;
done

for f in $SEDNA_COMMON_HOME/lib/*.jar; do
	CLASSPATH=${CLASSPATH}:$f;
done

SEDNA_LOG_DIR="$SEDNA_HOME/logs"

unset IFS

