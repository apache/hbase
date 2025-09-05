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

require 'hbase_constants'
require 'hbase_shell'

module Hbase
  class ListProceduresTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

    def setup
      setup_hbase

      @master = $TEST_CLUSTER.getHBaseClusterInterface.getMaster
      @executor = @master.getMasterProcedureExecutor

      @list_procedures = Shell::Commands::ListProcedures.new(@shell)
    end

    def teardown
      shutdown
    end

    def create_procedure_regexp(table_name)
      regexp_string = '[0-9]+ .*ShellTestProcedure SUCCESS.*' \
        "\[{\"value\"=>\"#{table_name}\"}\]";
      Regexp.new(regexp_string)
    end

    define_test 'list procedures' do
      procedure = org.apache.hadoop.hbase.client.procedure.ShellTestProcedure.new
      procedure.tableNameString = 'table1'

      proc_id = @executor.submitProcedure(procedure)
      sleep(0.1) until @executor.isFinished(proc_id)

      output = capture_stdout { @list_procedures.command }

      regexp = create_procedure_regexp('table1')
      matching_lines = 0

      lines = output.split(/\n/)
      lines.each do |line|
        if regexp.match(line)
          matching_lines += 1
        end
      end

      assert_equal(1, matching_lines)
    end
  end
end
