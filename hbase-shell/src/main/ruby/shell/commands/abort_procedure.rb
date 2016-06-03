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

module Shell
  module Commands
    class AbortProcedure < Command
      def help
        return <<-EOF
Given a procedure Id (and optional boolean may_interrupt_if_running parameter,
default is true), abort a procedure in hbase. Use with caution. Some procedures
might not be abortable. For experts only.

If this command is accepted and the procedure is in the process of aborting,
it will return true; if the procedure could not be aborted (eg. procedure
does not exist, or procedure already completed or abort will cause corruption),
this command will return false.

Examples:

  hbase> abort_procedure proc_id
  hbase> abort_procedure proc_id, true
  hbase> abort_procedure proc_id, false
EOF
      end

      def command(proc_id, may_interrupt_if_running=nil)
        formatter.row([admin.abort_procedure?(proc_id, may_interrupt_if_running).to_s])
      end
    end
  end
end