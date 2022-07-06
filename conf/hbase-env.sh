#!/usr/bin/env bash
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

# Set environment variables here.

# This script sets variables multiple times over the course of starting an hbase process,
# so try to keep things idempotent unless you want to take an even deeper look
# into the startup scripts (bin/hbase, etc.)

# The java implementation to use.  Java 1.8+ required.
# export JAVA_HOME=/usr/java/jdk1.8.0/

# Extra Java CLASSPATH elements.  Optional.
# export HBASE_CLASSPATH=

# The maximum amount of heap to use. Default is left to JVM default.
# export HBASE_HEAPSIZE=1G

# Uncomment below if you intend to use off heap cache. For example, to allocate 8G of
# offheap, set the value to "8G".
# export HBASE_OFFHEAPSIZE=1G

# Extra Java runtime options.
# Default settings are applied according to the detected JVM version. Override these default
# settings by specifying a value here. For more details on possible settings,
# see http://hbase.apache.org/book.html#_jvm_tuning
# export HBASE_OPTS

# Uncomment one of the below three options to enable java garbage collection logging for the server-side processes.

# This enables basic gc logging to the .out file.
# export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"

# This enables basic gc logging to its own file.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH>"

# This enables basic GC logging to its own file with automatic log rolling. Only applies to jdk 1.6.0_34+ and 1.7.0_2+.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH> -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=512M"

# Uncomment one of the below three options to enable java garbage collection logging for the client processes.

# This enables basic gc logging to the .out file.
# export CLIENT_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"

# This enables basic gc logging to its own file.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export CLIENT_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH>"

# This enables basic GC logging to its own file with automatic log rolling. Only applies to jdk 1.6.0_34+ and 1.7.0_2+.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export CLIENT_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH> -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=512M"

# See the package documentation for org.apache.hadoop.hbase.io.hfile for other configurations
# needed setting up off-heap block caching.

# Uncomment and adjust to enable JMX exporting
# See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
# More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
# NOTE: HBase provides an alternative JMX implementation to fix the random ports issue, please see JMX
# section in HBase Reference Guide for instructions.

# export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
# export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10101"
# export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10102"
# export HBASE_THRIFT_OPTS="$HBASE_THRIFT_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10103"
# export HBASE_ZOOKEEPER_OPTS="$HBASE_ZOOKEEPER_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10104"
# export HBASE_REST_OPTS="$HBASE_REST_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10105"

# File naming hosts on which HRegionServers will run.  $HBASE_HOME/conf/regionservers by default.
# export HBASE_REGIONSERVERS=${HBASE_HOME}/conf/regionservers

# Uncomment and adjust to keep all the Region Server pages mapped to be memory resident
#HBASE_REGIONSERVER_MLOCK=true
#HBASE_REGIONSERVER_UID="hbase"

# File naming hosts on which backup HMaster will run.  $HBASE_HOME/conf/backup-masters by default.
# export HBASE_BACKUP_MASTERS=${HBASE_HOME}/conf/backup-masters

# Extra ssh options.  Empty by default.
# export HBASE_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HBASE_CONF_DIR"

# Where log files are stored.  $HBASE_HOME/logs by default.
# export HBASE_LOG_DIR=${HBASE_HOME}/logs

# Enable remote JDWP debugging of major HBase processes. Meant for Core Developers
# export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8070"
# export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8071"
# export HBASE_THRIFT_OPTS="$HBASE_THRIFT_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8072"
# export HBASE_ZOOKEEPER_OPTS="$HBASE_ZOOKEEPER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8073"
# export HBASE_REST_OPTS="$HBASE_REST_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8074"

# A string representing this instance of hbase. $USER by default.
# export HBASE_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HBASE_NICENESS=10

# The directory where pid files are stored. /tmp by default.
# export HBASE_PID_DIR=/var/hadoop/pids

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HBASE_SLAVE_SLEEP=0.1

# Tell HBase whether it should manage it's own instance of ZooKeeper or not.
# export HBASE_MANAGES_ZK=true

# The default log rolling policy is RFA, where the log file is rolled as per the size defined for the
# RFA appender. Please refer to the log4j2.properties file to see more details on this appender.
# In case one needs to do log rolling on a date change, one should set the environment property
# HBASE_ROOT_LOGGER to "<DESIRED_LOG LEVEL>,DRFA".
# For example:
# export HBASE_ROOT_LOGGER=INFO,DRFA
# The reason for changing default to RFA is to avoid the boundary case of filling out disk space as
# DRFA doesn't put any cap on the log size. Please refer to HBase-5655 for more context.

# Tell HBase whether it should include Hadoop's lib when start up,
# the default value is false,means that includes Hadoop's lib.
# export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP="true"

# Override text processing tools for use by these launch scripts.
# export GREP="${GREP-grep}"
# export SED="${SED-sed}"

#
## OpenTelemetry Tracing
#
# HBase is instrumented for tracing using OpenTelemetry. None of the other OpenTelemetry signals
# are supported at this time. Configuring tracing involves setting several configuration points,
# via environment variable or system property. This configuration prefers setting environment
# variables whenever possible because they are picked up by all processes launched by `bin/hbase`.
# Use system properties when you launch multiple processes from the same configuration directory --
# when you need to specify different configuration values for different hbase processes that are
# launched using the same HBase configuration (i.e., a single-host pseudo-distributed cluster or
# launching the `bin/hbase shell` from a host that is also running an instance of the master). See
# https://github.com/open-telemetry/opentelemetry-java/tree/v1.15.0/sdk-extensions/autoconfigure
# for an inventory of configuration points and detailed explanations of each of them.
#
# Note also that as of this writing, the javaagent logs to stderr and is not configured along with
# the rest of HBase's logging configuration.
#
# `HBASE_OTEL_TRACING_ENABLED`, required. Enable attaching the opentelemetry javaagent to the
# process via support provided by `bin/hbase`. When this value us `false`, the agent is not added
# to the process launch arguments and all further OpenTelemetry configuration is ignored.
#export HBASE_OTEL_TRACING_ENABLED=true
#
# `OPENTELEMETRY_JAVAAGENT_PATH`, optional. Override the javaagent provided by HBase in `lib/trace`
# with an alternate. Use when you need to upgrade the agent version or swap out the official one
# for an alternative implementation.
#export OPENTELEMETRY_JAVAAGENT_PATH=""
#
# `OTEL_FOO_EXPORTER`, required. Specify an Exporter implementation per signal type. HBase only
# makes explicit use of the traces signal at this time, so the important one is
# `OTEL_TRACES_EXPORTER`. Specify its value based on the exporter required for your tracing
# environment. The other two should be uncommented and specified as `none`, otherwise the agent
# may report errors while attempting to export these other signals to an unconfigured destination.
# https://github.com/open-telemetry/opentelemetry-java/tree/v1.15.0/sdk-extensions/autoconfigure#exporters
#export OTEL_TRACES_EXPORTER=""
#export OTEL_METRICS_EXPORTER="none"
#export OTEL_LOGS_EXPORTER="none"
#
# `OTEL_SERVICE_NAME`, required. Specify "resource attributes", and specifically the `service.name`,
# as a unique value for each HBase process. OpenTelemetry allows for specifying this value in one
# of two ways, via environment variables with the `OTEL_` prefix, or via system properties with the
# `otel.` prefix. Which you use with HBase is decided based on whether this configuration file is
# read by a single process or shared by multiple HBase processes. For the default standalone mode
# or an environment where all processes share the same configuration file, use the `otel` system
# properties by uncommenting all of the `HBASE_FOO_OPTS` exports below. When this configuration file
# is being consumed by only a single process -- for example, from a systemd configuration or in a
# container template -- replace use of `HBASE_FOO_OPTS` with the standard `OTEL_SERVICE_NAME` and/or
# `OTEL_RESOURCE_ATTRIBUTES` environment variables. For further details, see
# https://github.com/open-telemetry/opentelemetry-java/tree/v1.15.0/sdk-extensions/autoconfigure#opentelemetry-resource
#export HBASE_CANARY_OPTS="${HBASE_CANARY_OPTS} -Dotel.resource.attributes=service.name=hbase-canary"
#export HBASE_HBCK_OPTS="${HBASE_HBCK_OPTS} -Dotel.resource.attributes=service.name=hbase-hbck"
#export HBASE_HBTOP_OPTS="${HBASE_HBTOP_OPTS} -Dotel.resource.attributes=service.name=hbase-hbtop"
#export HBASE_JSHELL_OPTS="${HBASE_JSHELL_OPTS} -Dotel.resource.attributes=service.name=hbase-jshell"
#export HBASE_LTT_OPTS="${HBASE_LTT_OPTS} -Dotel.resource.attributes=service.name=hbase-loadtesttool"
#export HBASE_MASTER_OPTS="${HBASE_MASTER_OPTS} -Dotel.resource.attributes=service.name=hbase-master"
#export HBASE_PE_OPTS="${HBASE_PE_OPTS} -Dotel.resource.attributes=service.name=hbase-performanceevaluation"
#export HBASE_REGIONSERVER_OPTS="${HBASE_REGIONSERVER_OPTS} -Dotel.resource.attributes=service.name=hbase-regionserver"
#export HBASE_REST_OPTS="${HBASE_REST_OPTS} -Dotel.resource.attributes=service.name=hbase-rest"
#export HBASE_SHELL_OPTS="${HBASE_SHELL_OPTS} -Dotel.resource.attributes=service.name=hbase-shell"
#export HBASE_THRIFT_OPTS="${HBASE_THRIFT_OPTS} -Dotel.resource.attributes=service.name=hbase-thrift"
#export HBASE_ZOOKEEPER_OPTS="${HBASE_ZOOKEEPER_OPTS} -Dotel.resource.attributes=service.name=hbase-zookeeper"

#
# JDK11+ JShell
#
# Additional arguments passed to jshell invocation
# export HBASE_JSHELL_ARGS="--startup DEFAULT --startup PRINTING --startup hbase_startup.jsh"
