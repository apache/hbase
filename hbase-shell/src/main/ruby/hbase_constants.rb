# frozen_string_literal: true

#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# HBase ruby classes.
# Has wrapper classes for org.apache.hadoop.hbase.client.Admin
# and for org.apache.hadoop.hbase.client.Table.  Classes take
# Formatters on construction and outputs any results using
# Formatter methods.  These classes are only really for use by
# the hirb.rb HBase Shell script; they don't make much sense elsewhere.
# For example, the exists method on Admin class prints to the formatter
# whether the table exists and returns nil regardless.

# HBase constants module providing access to HBase Java constants and
# dynamically loading column family and table descriptor constants
module HBaseConstants
  include Java

  java_import('java.lang.Integer') { |_package, name| "J#{name}" }
  java_import('java.lang.Long') { |_package, name| "J#{name}" }
  java_import('java.lang.Boolean') { |_package, name| "J#{name}" }

  ALLOW_PARTIAL_RESULTS = 'ALLOW_PARTIAL_RESULTS'
  ALL_METRICS = 'ALL_METRICS'
  ATTRIBUTES = 'ATTRIBUTES'
  AUTHORIZATIONS = 'AUTHORIZATIONS'
  BATCH = 'BATCH'
  CACHE = 'CACHE'
  CACHE_BLOCKS = 'CACHE_BLOCKS'
  CLASSNAME = 'CLASSNAME'
  CLONE_SFT = 'CLONE_SFT'
  CLUSTER_KEY = 'CLUSTER_KEY'
  COLUMN = 'COLUMN'
  COLUMNS = 'COLUMNS'
  CONFIG = 'CONFIG'
  CONFIGURATION = org.apache.hadoop.hbase.HConstants::CONFIGURATION
  CONSISTENCY = 'CONSISTENCY'
  COPROCESSOR = 'COPROCESSOR'
  DATA = 'DATA'
  ENDPOINT_CLASSNAME = 'ENDPOINT_CLASSNAME'
  FILTER = 'FILTER'
  FOREVER = org.apache.hadoop.hbase.HConstants::FOREVER
  FORMATTER = 'FORMATTER'
  FORMATTER_CLASS = 'FORMATTER_CLASS'
  INTERVAL = 'INTERVAL'
  IN_MEMORY = org.apache.hadoop.hbase.HConstants::IN_MEMORY
  IN_MEMORY_COMPACTION = org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder::IN_MEMORY_COMPACTION
  ISOLATION_LEVEL = 'ISOLATION_LEVEL'
  IS_ROOT = 'IS_ROOT'
  JAR_PATH = 'JAR_PATH'
  LIMIT = 'LIMIT'
  LOCALITY_THRESHOLD = 'LOCALITY_THRESHOLD'
  MAXLENGTH = 'MAXLENGTH'
  MAX_RESULT_SIZE = 'MAX_RESULT_SIZE'
  METADATA = org.apache.hadoop.hbase.HConstants::METADATA
  METHOD = 'METHOD'
  METRICS = 'METRICS'
  NAME = org.apache.hadoop.hbase.HConstants::NAME
  NAMESPACE = 'NAMESPACE'
  NAMESPACES = 'NAMESPACES'
  NONE = 'NONE'
  NUMREGIONS = 'NUMREGIONS'
  POLICY = 'POLICY'
  PRIORITY = 'PRIORITY'
  PROPERTIES = 'PROPERTIES'
  RAW = 'RAW'
  READ_TYPE = 'READ_TYPE'
  REGEX = 'REGEX'
  REGIONSERVER = 'REGIONSERVER'
  REGION_REPLICATION = 'REGION_REPLICATION'
  REGION_REPLICA_ID = 'REGION_REPLICA_ID'
  REMOTE_WAL_DIR = 'REMOTE_WAL_DIR'
  REPLICATION_SCOPE = 'REPLICATION_SCOPE'
  REPLICATION_SCOPE_BYTES = org.apache.hadoop.hbase.client.ColumnFamilyDescriptor::REPLICATION_SCOPE_BYTES
  RESTORE_ACL = 'RESTORE_ACL'
  REVERSED = 'REVERSED'
  ROWPREFIXFILTER = 'ROWPREFIXFILTER'
  SERIAL = 'SERIAL'
  SERVER_NAME = 'SERVER_NAME'
  SKIP_FLUSH = 'SKIP_FLUSH'
  SPLITALGO = 'SPLITALGO'
  SPLITS = 'SPLITS'
  SPLITS_FILE = 'SPLITS_FILE'
  STARTROW = 'STARTROW'
  STATE = 'STATE'
  STOPROW = 'STOPROW'
  TABLE = 'TABLE'
  TABLE_CFS = 'TABLE_CFS'
  TABLE_NAME = 'TABLE_NAME'
  TABLE_NAMES = 'TABLE_NAMES'
  TIMERANGE = 'TIMERANGE'
  TIMESTAMP = 'TIMESTAMP'
  TYPE = 'TYPE'
  USER = 'USER'
  VALUE = 'VALUE'
  VERSIONS = org.apache.hadoop.hbase.HConstants::VERSIONS
  VISIBILITY = 'VISIBILITY'
  REOPEN_REGIONS = 'REOPEN_REGIONS'

  # aliases
  ENDKEY = STOPROW
  ENDROW = STOPROW
  STARTKEY = STARTROW

  # Load constants from hbase java API
  def self.promote_constants(constants)
    # The constants to import are all in uppercase
    constants.each do |c|
      next if c =~ /DEFAULT_.*/ || c != c.upcase
      next if const_defined?(c)

      const_set(c, c.to_s)
    end
  end

  promote_constants(org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.constants)
  promote_constants(org.apache.hadoop.hbase.client.TableDescriptorBuilder.constants)
end

# Ensure that hbase class definitions are imported
require 'hbase/hbase'
require 'hbase/admin'
require 'hbase/taskmonitor'
require 'hbase/table'
require 'hbase/quotas'
require 'hbase/replication_admin'
require 'hbase/security'
require 'hbase/visibility_labels'
require 'hbase/rsgroup_admin'
require 'hbase/keymeta_admin'
