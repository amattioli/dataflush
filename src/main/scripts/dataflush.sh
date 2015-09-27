#!/bin/sh

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set DATAFLUSH_HOME if not already set
[ -z "$DATAFLUSH_HOME" ] && DATAFLUSH_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`

JAVACMD="$JAVA_HOME/bin/java"

CLI_MAIN="it.amattioli.dataflush.cli.Main"

LIB=$DATAFLUSH_HOME/lib
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")

exec $JAVACMD -classpath $CLASSPATH $CLI_MAIN $@ 