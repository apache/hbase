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
require 'hbase_constants'
require 'hbase_shell'

class NonInteractiveTest < Test::Unit::TestCase
  def setup
    @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
    @shell = Shell::Shell.new(@hbase, false)
  end

  define_test "Shell::Shell noninteractive mode should throw" do
    # XXX Exception instead of StandardError because we throw things
    #     that aren't StandardError
    assert_raise(ArgumentError) do
      # incorrect number of arguments
      @shell.command('create', 'foo')
    end
    @shell.command('create', 'foo', 'family_1')
    exception = assert_raise(RuntimeError) do
      # create a table that exists
      @shell.command('create', 'foo', 'family_1')
    end
    assert_equal("Table already exists: foo!", exception.message)
  end
end
