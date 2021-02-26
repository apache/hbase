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

require 'hbase_shell'
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

java_import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot
java_import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy
java_import org.apache.hadoop.hbase.TableName

module Hbase
  class NoClusterSpaceQuotasTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

    define_test '_parse_size accepts various forms of byte shorthand' do
      qa = ::Hbase::QuotasAdmin.new(nil)
      KILO = 1024
      MEGA = KILO * KILO
      GIGA = MEGA * KILO
      TERA = GIGA * KILO
      PETA = TERA * KILO
      assert_equal(1, qa._parse_size("1"))
      assert_equal(1, qa._parse_size("1b"))
      assert_equal(1, qa._parse_size("1B"))
      assert_equal(KILO * 2, qa._parse_size("2k"))
      assert_equal(KILO * 2, qa._parse_size("2K"))
      assert_equal(MEGA * 5, qa._parse_size("5m"))
      assert_equal(MEGA * 5, qa._parse_size("5M"))
      assert_equal(GIGA * 3, qa._parse_size("3g"))
      assert_equal(GIGA * 3, qa._parse_size("3G"))
      assert_equal(TERA * 4, qa._parse_size("4t"))
      assert_equal(TERA * 4, qa._parse_size("4T"))
      assert_equal(PETA * 32, qa._parse_size("32p"))
      assert_equal(PETA * 32, qa._parse_size("32P"))
      assert_equal(GIGA * 4, qa._parse_size("4096m"))
      assert_equal(GIGA * 4, qa._parse_size("4096M"))
    end

    define_test 'get policy name for status not in violation' do
      okStatus = SpaceQuotaSnapshot::SpaceQuotaStatus::notInViolation()
      # By default, statuses are in violation
      violatedStatus = SpaceQuotaSnapshot::SpaceQuotaStatus.new(SpaceViolationPolicy::NO_INSERTS)
      # Pass in nil for the Shell instance (that we don't care about)
      quotaSnapshotCommand = ::Shell::Commands::ListQuotaSnapshots.new(nil)
      assert_equal('None', quotaSnapshotCommand.get_policy(okStatus))
      assert_equal('NO_INSERTS', quotaSnapshotCommand.get_policy(violatedStatus))
    end

    define_test 'table and namespace filtering in list_quota_snapshots' do
      cmd = ::Shell::Commands::ListQuotaSnapshots.new(nil)
      assert cmd.accept?(TableName.valueOf('t1')) == true
      assert cmd.accept?(TableName.valueOf('t1'), nil, nil) == true
      assert cmd.accept?(TableName.valueOf('t1'), 't1', nil) == true
      assert cmd.accept?(TableName.valueOf('t1'), 't2', nil) == false
      assert cmd.accept?(TableName.valueOf('t1'), nil, 'ns1') == false
      assert cmd.accept?(TableName.valueOf('ns1:t1'), nil, 'ns1') == true
      assert cmd.accept?(TableName.valueOf('ns1:t1'), 't1', nil) == true
      assert cmd.accept?(TableName.valueOf('ns1:t1'), 't1', 'ns1') == true
    end
  end
end
