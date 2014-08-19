#
#/**
# * Copyright The Apache Software Foundation
# *
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
function get_canonical_dir() {
  target="$1"

  canonical_name=`readlink -f ${target} 2>/dev/null`
  if [[ $? -eq 0 ]]; then
    canonical_dir=`dirname $canonical_name`
    echo ${canonical_dir}
    return
  fi

  # Mac has no readlink -f
  cd `dirname ${target}`
  target=`basename ${target}`
  # chase down the symlinks
  while [ -L ${target} ]; do
    target=`readlink ${target}`
    cd `dirname ${target}`
    target=`basename ${target}`
  done
  canonical_dir=`pwd -P`
  ret=${canonical_dir}
  echo $ret
}
bin=$(get_canonical_dir "$0")
LIBHBASE_HOME=`cd "$bin/..">/dev/null; pwd`

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

HBASE_NATIVE_DIR=${LIBHBASE_HOME}/lib/native

HBASE_LIBRARY_PATH="$HBASE_LIBRARY_PATH:${HBASE_NATIVE_DIR}"
#Add libjvm.so's location
if [ -d "$JAVA_HOME/jre/lib/amd64/server" ]; then
  HBASE_LIBRARY_PATH="$HBASE_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server"
fi
if [ -d "$JAVA_HOME/jre/lib/i386/server" ]; then
  HBASE_LIBRARY_PATH="$HBASE_LIBRARY_PATH:$JAVA_HOME/jre/lib/i386/server"
fi
LD_LIBRARY_PATH="${HBASE_LIBRARY_PATH#:}"

HBASE_LIB_DIR=${LIBHBASE_HOME}/lib

if $cygwin; then
  LIBHBASE_HOME=`cygpath -d "$LIBHBASE_HOME"`
  LD_LIBRARY_PATH=`cygpath -d "$LD_LIBRARY_PATH"`
  HBASE_NATIVE_DIR=`cygpath -d "$HBASE_NATIVE_DIR"`
fi

#This is passed to JVM by libhbase
LIBHBASE_OPTS="${LIBHBASE_OPTS} -Dlibhbase.log.dir=${LIBHBASE_HOME}/logs -Dlibhbase.log.name=perftest-java.log -Dlibhbase.log.level=info -Dlibhbase.logger=RFA"
LIBHBASE_OPTS="${LIBHBASE_OPTS} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/ -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"

export LIBHBASE_OPTS=${LIBHBASE_OPTS}
export HBASE_LIB_DIR=${HBASE_LIB_DIR}
export HBASE_CONF_DIR=${LIBHBASE_HOME}/conf
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}

if [ $# -ne 0 ]; then
  mkdir -p ${LIBHBASE_HOME}/logs
  LOGFILE_OPTION="-logFilePath ${LIBHBASE_HOME}/logs/perftest.log"
fi

exec ${HBASE_NATIVE_DIR}/perftest ${LOGFILE_OPTION} $*
