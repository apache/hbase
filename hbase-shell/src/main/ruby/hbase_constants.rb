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
include Java

java_import('java.lang.Integer') { |_package, name| "J#{name}" }
java_import('java.lang.Long') { |_package, name| "J#{name}" }
java_import('java.lang.Boolean') { |_package, name| "J#{name}" }

module HBaseConstants
  ALLOW_PARTIAL_RESULTS = 'ALLOW_PARTIAL_RESULTS'.freeze
  ALL_METRICS = 'ALL_METRICS'.freeze
  ATTRIBUTES = 'ATTRIBUTES'.freeze
  AUTHORIZATIONS = 'AUTHORIZATIONS'.freeze
  BATCH = 'BATCH'.freeze
  CACHE = 'CACHE'.freeze
  CACHE_BLOCKS = 'CACHE_BLOCKS'.freeze
  CLUSTER_KEY = 'CLUSTER_KEY'.freeze
  COLUMN = 'COLUMN'.freeze
  COLUMNS = 'COLUMNS'.freeze
  CONFIG = 'CONFIG'.freeze
  CONFIGURATION = org.apache.hadoop.hbase.HConstants::CONFIGURATION
  CONSISTENCY = 'CONSISTENCY'.freeze
  DATA = 'DATA'.freeze
  ENDPOINT_CLASSNAME = 'ENDPOINT_CLASSNAME'.freeze
  FILTER = 'FILTER'.freeze
  FORMATTER = 'FORMATTER'.freeze
  FORMATTER_CLASS = 'FORMATTER_CLASS'.freeze
  INTERVAL = 'INTERVAL'.freeze
  IN_MEMORY = org.apache.hadoop.hbase.HConstants::IN_MEMORY
  IN_MEMORY_COMPACTION = org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY_COMPACTION
  ISOLATION_LEVEL = 'ISOLATION_LEVEL'.freeze
  LIMIT = 'LIMIT'.freeze
  LOCALITY_THRESHOLD = 'LOCALITY_THRESHOLD'.freeze
  MAXLENGTH = 'MAXLENGTH'.freeze
  MAX_RESULT_SIZE = 'MAX_RESULT_SIZE'.freeze
  METADATA = org.apache.hadoop.hbase.HConstants::METADATA
  METHOD = 'METHOD'.freeze
  METRICS = 'METRICS'.freeze
  NAME = org.apache.hadoop.hbase.HConstants::NAME
  NAMESPACE = 'NAMESPACE'.freeze
  NAMESPACES = 'NAMESPACES'.freeze
  NONE = 'NONE'.freeze
  NUMREGIONS = 'NUMREGIONS'.freeze
  POLICY = 'POLICY'.freeze
  RAW = 'RAW'.freeze
  READ_TYPE = 'READ_TYPE'.freeze
  REGEX = 'REGEX'.freeze
  REGIONSERVER = 'REGIONSERVER'.freeze
  REGION_REPLICATION = 'REGION_REPLICATION'.freeze
  REGION_REPLICA_ID = 'REGION_REPLICA_ID'.freeze
  REPLICATION_SCOPE = 'REPLICATION_SCOPE'.freeze
  RESTORE_ACL = 'RESTORE_ACL'.freeze
  REVERSED = 'REVERSED'.freeze
  ROWPREFIXFILTER = 'ROWPREFIXFILTER'.freeze
  SERIAL = 'SERIAL'.freeze
  SERVER_NAME = 'SERVER_NAME'.freeze
  SKIP_FLUSH = 'SKIP_FLUSH'.freeze
  SPLITALGO = 'SPLITALGO'.freeze
  SPLITS = 'SPLITS'.freeze
  SPLITS_FILE = 'SPLITS_FILE'.freeze
  STARTROW = 'STARTROW'.freeze
  STATE = 'STATE'.freeze
  STOPROW = 'STOPROW'.freeze
  TABLE = 'TABLE'.freeze
  TABLE_CFS = 'TABLE_CFS'.freeze
  TABLE_NAME = 'TABLE_NAME'.freeze
  TABLE_NAMES = 'TABLE_NAMES'.freeze
  TIMERANGE = 'TIMERANGE'.freeze
  TIMESTAMP = 'TIMESTAMP'.freeze
  TYPE = 'TYPE'.freeze
  USER = 'USER'.freeze
  VALUE = 'VALUE'.freeze
  VERSIONS = org.apache.hadoop.hbase.HConstants::VERSIONS
  VISIBILITY = 'VISIBILITY'.freeze

  # aliases
  ENDKEY = STOPROW
  ENDROW = STOPROW
  STARTKEY = STARTROW

  # Load constants from hbase java API
  def self.promote_constants(constants)
    # The constants to import are all in uppercase
    constants.each do |c|
      next if c =~ /DEFAULT_.*/ || c != c.upcase
      next if eval("defined?(#{c})")
      eval("#{c} = '#{c}'")
    end
  end

  promote_constants(org.apache.hadoop.hbase.HColumnDescriptor.constants)
  promote_constants(org.apache.hadoop.hbase.HTableDescriptor.constants)
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
