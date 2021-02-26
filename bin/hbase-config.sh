#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# included in all the hbase scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
# Modelled after $HADOOP_HOME/bin/hadoop-env.sh.

# resolve links - "${BASH_SOURCE-$0}" may be a softlink

this="${BASH_SOURCE-$0}"
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
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

# the root of the hbase installation
if [ -z "$HBASE_HOME" ]; then
  export HBASE_HOME=`dirname "$this"`/..
fi

#check to see if the conf dir or hbase home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    HBASE_CONF_DIR=$confdir
  elif [ "--hosts" = "$1" ]
  then
    shift
    hosts=$1
    shift
    HBASE_REGIONSERVERS=$hosts
  elif [ "--auth-as-server" = "$1" ]
  then
    shift
    # shellcheck disable=SC2034
    AUTH_AS_SERVER="true"
  elif [ "--autostart-window-size" = "$1" ]
  then
    shift
    AUTOSTART_WINDOW_SIZE=$(( $1 + 0 ))
    if [ $AUTOSTART_WINDOW_SIZE -lt 0 ]; then
      echo "Invalid value for --autostart-window-size, should be a positive integer"
      exit 1
    fi
    shift
  elif [ "--autostart-window-retry-limit" = "$1" ]
  then
    shift
    AUTOSTART_WINDOW_RETRY_LIMIT=$(( $1 + 0 ))
    if [ $AUTOSTART_WINDOW_RETRY_LIMIT -lt 0 ]; then
      echo "Invalid value for --autostart-window-retry-limit, should be a positive integer"
      exit 1
    fi
    shift
  elif [ "--internal-classpath" = "$1" ]
  then
    shift
    # shellcheck disable=SC2034
    INTERNAL_CLASSPATH="true"
  elif [ "--debug" = "$1" ]
  then
    shift
    # shellcheck disable=SC2034
    DEBUG="true"
  elif [ "--help" = "$1" ] || [ "-h" = "$1" ]
  then
    echo "Error usage! You should use it like 'hbase --help' or 'hbase -h'";
    exit 1
  else
    # Presume we are at end of options and break
    break
  fi
done
 
# Allow alternate hbase conf dir location.
HBASE_CONF_DIR="${HBASE_CONF_DIR:-$HBASE_HOME/conf}"
# List of hbase regions servers.
HBASE_REGIONSERVERS="${HBASE_REGIONSERVERS:-$HBASE_CONF_DIR/regionservers}"
# List of hbase secondary masters.
HBASE_BACKUP_MASTERS="${HBASE_BACKUP_MASTERS:-$HBASE_CONF_DIR/backup-masters}"
if [ -n "$HBASE_JMX_BASE" ] && [ -z "$HBASE_JMX_OPTS" ]; then
  HBASE_JMX_OPTS="$HBASE_JMX_BASE"
fi
# Thrift JMX opts
if [ -n "$HBASE_JMX_OPTS" ] && [ -z "$HBASE_THRIFT_JMX_OPTS" ]; then
  HBASE_THRIFT_JMX_OPTS="$HBASE_JMX_OPTS -Dcom.sun.management.jmxremote.port=10103"
fi
# Thrift opts
if [ -z "$HBASE_THRIFT_OPTS" ]; then
  export HBASE_THRIFT_OPTS="$HBASE_THRIFT_JMX_OPTS"
fi

# REST JMX opts
if [ -n "$HBASE_JMX_OPTS" ] && [ -z "$HBASE_REST_JMX_OPTS" ]; then
  HBASE_REST_JMX_OPTS="$HBASE_JMX_OPTS -Dcom.sun.management.jmxremote.port=10105"
fi
# REST opts
if [ -z "$HBASE_REST_OPTS" ]; then
  export HBASE_REST_OPTS="$HBASE_REST_JMX_OPTS"
fi

# Source the hbase-env.sh.  Will have JAVA_HOME defined.
# HBASE-7817 - Source the hbase-env.sh only if it has not already been done. HBASE_ENV_INIT keeps track of it.
if [ -z "$HBASE_ENV_INIT" ] && [ -f "${HBASE_CONF_DIR}/hbase-env.sh" ]; then
  . "${HBASE_CONF_DIR}/hbase-env.sh"
  export HBASE_ENV_INIT="true"
fi

# These variables can be specified in hbase-env.sh, so these defines follow sourcing of that script.
export GREP="${GREP-grep}"
export SED="${SED-sed}"

# Verify if hbase has the mlock agent
if [ "$HBASE_REGIONSERVER_MLOCK" = "true" ]; then
  MLOCK_AGENT="$HBASE_HOME/lib/native/libmlockall_agent.so"
  if [ ! -f "$MLOCK_AGENT" ]; then
    cat 1>&2 <<EOF
Unable to find mlockall_agent, hbase must be compiled with -Pnative
EOF
    exit 1
  fi
  if [ -z "$HBASE_REGIONSERVER_UID" ] || [ "$HBASE_REGIONSERVER_UID" == "$USER" ]; then
      HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -agentpath:$MLOCK_AGENT"
  else
      HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -agentpath:$MLOCK_AGENT=user=$HBASE_REGIONSERVER_UID"
  fi
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# Now having JAVA_HOME defined is required 
if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|                    Error: JAVA_HOME is not set                       |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|     > http://www.oracle.com/technetwork/java/javase/downloads        |
|                                                                      |
| HBase requires Java 1.8 or later.                                    |
+======================================================================+
EOF
    exit 1
fi

function read_java_version() {
  properties="$("${JAVA_HOME}/bin/java" -XshowSettings:properties -version 2>&1)"
  echo "${properties}" | "${GREP}" java.runtime.version | head -1 | "${SED}" -e 's/.* = \([^ ]*\)/\1/'
}

# Inspect the system properties exposed by this JVM to identify the major
# version number. Normalize on the popular version number, thus consider JDK
# 1.8 as version "8".
function parse_java_major_version() {
  complete_version=$1
  # split off suffix version info like '-b10' or '+10' or '_10'
  # careful to not use GNU Sed extensions
  version="$(echo "$complete_version" | "${SED}" -e 's/+/_/g' -e 's/-/_/g' | cut -d'_' -f1)"
  case "$version" in
  1.*)
    echo "$version" | cut -d'.' -f2
    ;;
  *)
    echo "$version" | cut -d'.' -f1
    ;;
  esac
}
