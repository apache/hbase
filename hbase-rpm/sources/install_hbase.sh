#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

usage() {
  echo "
usage: $0 <options>
  Required not-so-options:
     --mvn-target-dir=DIR        path to the output of the mvn assembly
     --prefix=PREFIX             path to install into

  Optional options:
     --doc-dir=DIR               path to install docs into [/usr/share/doc/hbase]
     --lib-dir=DIR               path to install hbase home [/usr/lib/hbase]
     --installed-lib-dir=DIR     path where lib-dir will end up on target system
     --bin-dir=DIR               path to install bins [/usr/bin]
     --examples-dir=DIR          path to install examples [doc-dir/examples]
     ... [ see source for more similar options ]
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'prefix:' \
  -l 'doc-dir:' \
  -l 'lib-dir:' \
  -l 'installed-lib-dir:' \
  -l 'bin-dir:' \
  -l 'examples-dir:' \
  -l 'conf-dir:' \
  -l 'input-tar:' -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"
while true ; do
    case "$1" in
        --prefix)
        PREFIX=$2 ; shift 2
        ;;
        --input-tar)
        INPUT_TAR=$2 ; shift 2
        ;;
        --doc-dir)
        DOC_DIR=$2 ; shift 2
        ;;
        --lib-dir)
        LIB_DIR=$2 ; shift 2
        ;;
        --bin-dir)
        BIN_DIR=$2 ; shift 2
        ;;
        --examples-dir)
        EXAMPLES_DIR=$2 ; shift 2
        ;;
        --conf-dir)
        CONF_DIR=$2 ; shift 2
        ;;
        --)
        shift ; break
        ;;
        *)
        echo "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
done

for var in PREFIX INPUT_TAR ; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

MAN_DIR=${MAN_DIR:-/usr/share/man/man1}
DOC_DIR=${DOC_DIR:-/usr/share/doc/hbase}
LIB_DIR=${LIB_DIR:-/usr/lib/hbase}

BIN_DIR=${BIN_DIR:-/usr/lib/hbase/bin}
ETC_DIR=${ETC_DIR:-/etc/hbase}
CONF_DIR=${CONF_DIR:-${ETC_DIR}/conf.dist}
THRIFT_DIR=${THRIFT_DIR:-${LIB_DIR}/include/thrift}

EXTRACT_DIR=extracted
rm -rf $EXTRACT_DIR
mkdir $EXTRACT_DIR

version_part=$SET_VERSION
if [ -z "$version_part" ]; then
  version_part=$HBASE_VERSION
fi

tar -C $EXTRACT_DIR --strip-components=1 -xzf $INPUT_TAR

# we do not need the shaded clients in our rpm. they bloat the size and cause classpath issues for hbck2.
rm -rf $EXTRACT_DIR/lib/shaded-clients

install -d -m 0755 $PREFIX/$LIB_DIR
install -d -m 0755 $PREFIX/$LIB_DIR/lib
install -d -m 0755 $PREFIX/$DOC_DIR
install -d -m 0755 $PREFIX/$BIN_DIR
install -d -m 0755 $PREFIX/$ETC_DIR
install -d -m 0755 $PREFIX/$MAN_DIR
install -d -m 0755 $PREFIX/$THRIFT_DIR

cp -ra $EXTRACT_DIR/lib/* ${PREFIX}/${LIB_DIR}/lib/
cp $EXTRACT_DIR/lib/hbase*.jar $PREFIX/$LIB_DIR

# We do not currently run "mvn site", so do not have a docs dir.
# Only copy contents if dir exists
if [ -n "$(ls -A $EXTRACT_DIR/docs 2>/dev/null)" ]; then
  cp -a $EXTRACT_DIR/docs/* $PREFIX/$DOC_DIR
  cp $EXTRACT_DIR/*.txt $PREFIX/$DOC_DIR/
else
  echo "Doc generation is currently disabled in our RPM build. If this is an issue, it should be possible to enable them with some work. See https://git.hubteam.com/HubSpot/apache-hbase/blob/hubspot-2/rpm/sources/do-component-build#L17-L24 for details." > $PREFIX/$DOC_DIR/README.txt
fi

cp -a $EXTRACT_DIR/conf $PREFIX/$CONF_DIR
cp -a $EXTRACT_DIR/bin/* $PREFIX/$BIN_DIR

# Purge scripts that don't work with packages
for file in rolling-restart.sh graceful_stop.sh local-regionservers.sh \
            master-backup.sh regionservers.sh zookeepers.sh hbase-daemons.sh \
            start-hbase.sh stop-hbase.sh local-master-backup.sh ; do
  rm -f $PREFIX/$BIN_DIR/$file
done


ln -s $ETC_DIR/conf $PREFIX/$LIB_DIR/conf

# Make a symlink of hbase.jar to hbase-version.jar
pushd `pwd`
cd $PREFIX/$LIB_DIR
for i in `ls hbase*jar | grep -v tests.jar`
do
    ln -s $i `echo $i | sed -n 's/\(.*\)\(-[0-9].*\)\(.jar\)/\1\3/p'`
done
popd

wrapper=$PREFIX/usr/bin/hbase
mkdir -p `dirname $wrapper`
cat > $wrapper <<EOF
#!/bin/bash

DEFAULTS_DIR=\${DEFAULTS_DIR-/etc/default}
[ -n "\${DEFAULTS_DIR}" -a -r \${DEFAULTS_DIR}/hbase ] && . \${DEFAULTS_DIR}/hbase
[ -n "\${DEFAULTS_DIR}" -a -r \${DEFAULTS_DIR}/hadoop ] && . \${DEFAULTS_DIR}/hadoop

export HADOOP_CONF_DIR=\${HADOOP_CONF_DIR:-/etc/hadoop/conf}
export HBASE_CLASSPATH=\$HADOOP_CONF_DIR:\$HBASE_CLASSPATH

exec /usr/lib/hbase/bin/hbase "\$@"
EOF
chmod 755 $wrapper

install -d -m 0755 $PREFIX/usr/bin

rm -f $PREFIX/$CONF_DIR/*.cmd
rm -f $PREFIX/$BIN_DIR/*.cmd
