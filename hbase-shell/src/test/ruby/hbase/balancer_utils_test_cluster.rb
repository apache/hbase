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

java_import org.apache.hadoop.hbase.client.BalanceRequest

module Hbase
  class BalancerUtilsTest < Test::Unit::TestCase
    include TestHelpers

    def create_balance_request(*args)
       ::Hbase::BalancerUtils.create_balance_request(args)
    end

    define_test "should raise ArgumentError on unknown string argument" do
      assert_raise(ArgumentError) do
        request = create_balance_request('foo')
      end
    end

    define_test "should raise ArgumentError on non-array argument" do
      assert_raise(ArgumentError) do
        request = create_balance_request({foo: 'bar' })
      end
    end

    define_test "should raise ArgumentError on non-string array item" do
      assert_raise(ArgumentError) do
        request = create_balance_request('force', true)
      end
    end

    define_test "should parse empty args" do
      request = create_balance_request()
      assert(!request.isDryRun())
      assert(!request.isIgnoreRegionsInTransition())
    end

    define_test "should parse 'force' string" do
      request = create_balance_request('force')
      assert(!request.isDryRun())
      assert(request.isIgnoreRegionsInTransition())
    end
    
    define_test "should parse 'ignore_rit' string" do
      request = create_balance_request('ignore_rit')
      assert(!request.isDryRun())
      assert(request.isIgnoreRegionsInTransition())
    end

    define_test "should parse 'dry_run' string" do
      request = create_balance_request('dry_run')
      assert(request.isDryRun())
      assert(!request.isIgnoreRegionsInTransition())
    end

    define_test "should parse multiple string args" do
      request = create_balance_request('dry_run', 'ignore_rit')
      assert(request.isDryRun())
      assert(request.isIgnoreRegionsInTransition())
    end
  end
end
