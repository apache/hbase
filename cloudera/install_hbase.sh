#!/bin/sh
# Copyright 2009 Cloudera, inc.
set -ex

usage() {
  echo "
usage: $0 <options>
  Required not-so-options:
     --cloudera-source-dir=DIR   path to cloudera distribution files
     --build-dir=DIR             path to hbase dist.dir
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
  -l 'cloudera-source-dir:' \
  -l 'prefix:' \
  -l 'doc-dir:' \
  -l 'lib-dir:' \
  -l 'installed-lib-dir:' \
  -l 'bin-dir:' \
  -l 'examples-dir:' \
  -l 'build-dir:' -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"
while true ; do
    case "$1" in
        --cloudera-source-dir)
        CLOUDERA_SOURCE_DIR=$2 ; shift 2
        ;;
        --prefix)
        PREFIX=$2 ; shift 2
        ;;
        --build-dir)
        BUILD_DIR=$2 ; shift 2
        ;;
        --doc-dir)
        DOC_DIR=$2 ; shift 2
        ;;
        --lib-dir)
        LIB_DIR=$2 ; shift 2
        ;;
        --installed-lib-dir)
        INSTALLED_LIB_DIR=$2 ; shift 2
        ;;
        --bin-dir)
        BIN_DIR=$2 ; shift 2
        ;;
        --examples-dir)
        EXAMPLES_DIR=$2 ; shift 2
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

for var in CLOUDERA_SOURCE_DIR PREFIX BUILD_DIR ; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

DOC_DIR=${DOC_DIR:-/usr/share/doc/hbase}
LIB_DIR=${LIB_DIR:-/usr/lib/hbase}
BIN_DIR=${BIN_DIR:-/usr/lib/hbase/bin}
ETC_DIR=${ETC_DIR:-/etc/hbase}

install -d -m 0755 $PREFIX/$LIB_DIR
install -d -m 0755 $PREFIX/$LIB_DIR/lib
install -d -m 0755 $PREFIX/$DOC_DIR
install -d -m 0755 $PREFIX/$BIN_DIR
install -d -m 0755 $PREFIX/$ETC_DIR

cp -ra lib/* ${PREFIX}/${LIB_DIR}/lib/
cp hbase*.jar $PREFIX/$LIB_DIR
cp -a docs/* $PREFIX/$DOC_DIR
cp *.txt $PREFIX/$DOC_DIR/
cp -a hbase-webapps $PREFIX/$LIB_DIR

cp -a conf $PREFIX/$ETC_DIR/conf
cp bin/* $PREFIX/$BIN_DIR

ln -s $ETC_DIR/conf $PREFIX/$LIB_DIR/conf

wrapper=$PREFIX/usr/bin/hbase
mkdir -p `dirname $wrapper`
cat > $wrapper <<EOF
#!/bin/sh
export ZOOKEEPER_CONF=\${ZOOKEEPER_CONF:-/etc/zookeeper}
export HADOOP_CONF=\${HADOOP_CONF:-/etc/hadoop-0.20/conf}
export ZOOKEEPER_HOME=\${ZOOKEEPER_HOME:-/usr/lib/zookeeper}
export HADOOP_HOME=\${HADOOP_HOME:-/usr/lib/hadoop-0.20}
export HBASE_CLASSPATH=\$ZOOKEEPER_CONF:\$HADOOP_CONF:\$HADOOP_HOME/*:\$HADOOP_HOME/lib/*:\$ZOOKEEPER_HOME/*:\$ZOOKEEPER_HOME/lib/*:\$HBASE_CLASSPATH
export HBASE_PID_DIR=/var/run/hbase
exec /usr/lib/hbase/bin/hbase "\$@"
EOF
chmod 755 $wrapper

wrapper=$PREFIX/usr/bin/hbase-daemon.sh
mkdir -p `dirname $wrapper`
cat > $wrapper <<EOF
#!/bin/sh
export ZOOKEEPER_CONF=\${ZOOKEEPER_CONF:-/etc/zookeeper}
export HADOOP_CONF=\${HADOOP_CONF:-/etc/hadoop-0.20/conf}
export ZOOKEEPER_HOME=\${ZOOKEEPER_HOME:-/usr/lib/zookeeper}
export HADOOP_HOME=\${HADOOP_HOME:-/usr/lib/hadoop-0.20}
export HBASE_CLASSPATH=\$ZOOKEEPER_CONF:\$HADOOP_CONF:\$HADOOP_HOME/*:\$ZOOKEEPER_HOME/*:\$HBASE_CLASSPATH
export HBASE_PID_DIR=/var/run/hbase
exec /usr/lib/hbase/bin/hbase-daemon.sh "\$@"
EOF
chmod 755 $wrapper
