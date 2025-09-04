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

java_import org.apache.hadoop.hbase.client.Get
java_import org.apache.hadoop.hbase.util.Bytes


module Hbase
  class EncryptedTableKeymetaTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      @test_table = 'enctest'
    end

    define_test 'Test table put/get with encryption' do
      custAndNamespace = $CUST1_ENCODED + ':*'
      @shell.command(:enable_key_management, custAndNamespace)
      @shell.command(:create, @test_table, {'NAME' => 'f', 'ENCRYPTION' => 'AES'})
      test_table = table(@test_table)
      test_table.put('1', 'f:a', '2')
      puts "Added a row, now flushing table #{@test_table}"
      command(:flush, @test_table)
      get = Get.new(Bytes.toBytes('1'))
      res = test_table.table.get(get)
      puts "res for row '1' and column f:a: #{res}"
      assert_false(res.isEmpty())
      assert_equal('2', Bytes.toString(res.getValue(Bytes.toBytes('f'), Bytes.toBytes('a'))))
    end
  end
end