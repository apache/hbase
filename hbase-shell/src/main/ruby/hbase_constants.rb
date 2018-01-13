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
  COLUMN = 'COLUMN'.freeze
  COLUMNS = 'COLUMNS'.freeze
  TIMESTAMP = 'TIMESTAMP'.freeze
  TIMERANGE = 'TIMERANGE'.freeze
  NAME = org.apache.hadoop.hbase.HConstants::NAME
  VERSIONS = org.apache.hadoop.hbase.HConstants::VERSIONS
  IN_MEMORY = org.apache.hadoop.hbase.HConstants::IN_MEMORY
  IN_MEMORY_COMPACTION = org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY_COMPACTION
  METADATA = org.apache.hadoop.hbase.HConstants::METADATA
  STOPROW = 'STOPROW'.freeze
  STARTROW = 'STARTROW'.freeze
  ROWPREFIXFILTER = 'ROWPREFIXFILTER'.freeze
  ENDROW = STOPROW
  RAW = 'RAW'.freeze
  LIMIT = 'LIMIT'.freeze
  METHOD = 'METHOD'.freeze
  MAXLENGTH = 'MAXLENGTH'.freeze
  CACHE_BLOCKS = 'CACHE_BLOCKS'.freeze
  ALL_METRICS = 'ALL_METRICS'.freeze
  METRICS = 'METRICS'.freeze
  REVERSED = 'REVERSED'.freeze
  REPLICATION_SCOPE = 'REPLICATION_SCOPE'.freeze
  INTERVAL = 'INTERVAL'.freeze
  CACHE = 'CACHE'.freeze
  FILTER = 'FILTER'.freeze
  SPLITS = 'SPLITS'.freeze
  SPLITS_FILE = 'SPLITS_FILE'.freeze
  SPLITALGO = 'SPLITALGO'.freeze
  NUMREGIONS = 'NUMREGIONS'.freeze
  REGION_REPLICATION = 'REGION_REPLICATION'.freeze
  REGION_REPLICA_ID = 'REGION_REPLICA_ID'.freeze
  CONFIGURATION = org.apache.hadoop.hbase.HConstants::CONFIGURATION
  ATTRIBUTES = 'ATTRIBUTES'.freeze
  VISIBILITY = 'VISIBILITY'.freeze
  AUTHORIZATIONS = 'AUTHORIZATIONS'.freeze
  SKIP_FLUSH = 'SKIP_FLUSH'.freeze
  CONSISTENCY = 'CONSISTENCY'.freeze
  USER = 'USER'.freeze
  TABLE = 'TABLE'.freeze
  NAMESPACE = 'NAMESPACE'.freeze
  TYPE = 'TYPE'.freeze
  NONE = 'NONE'.freeze
  VALUE = 'VALUE'.freeze
  ENDPOINT_CLASSNAME = 'ENDPOINT_CLASSNAME'.freeze
  CLUSTER_KEY = 'CLUSTER_KEY'.freeze
  REMOTE_WAL_DIR = 'REMOTE_WAL_DIR'.freeze
  TABLE_CFS = 'TABLE_CFS'.freeze
  NAMESPACES = 'NAMESPACES'.freeze
  STATE = 'STATE'.freeze
  CONFIG = 'CONFIG'.freeze
  DATA = 'DATA'.freeze
  SERVER_NAME = 'SERVER_NAME'.freeze
  LOCALITY_THRESHOLD = 'LOCALITY_THRESHOLD'.freeze
  RESTORE_ACL = 'RESTORE_ACL'.freeze
  FORMATTER = 'FORMATTER'.freeze
  FORMATTER_CLASS = 'FORMATTER_CLASS'.freeze
  POLICY = 'POLICY'.freeze
  REGIONSERVER = 'REGIONSERVER'.freeze

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

# Include classes definition
require 'hbase/hbase'
require 'hbase/admin'
require 'hbase/taskmonitor'
require 'hbase/table'
require 'hbase/quotas'
require 'hbase/replication_admin'
require 'hbase/security'
require 'hbase/visibility_labels'
require 'hbase/rsgroup_admin'

include HBaseQuotasConstants
