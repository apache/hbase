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
    class Command

      def initialize(shell)
        @shell = shell
      end

      #wrap an execution of cmd to catch hbase exceptions
      # cmd - command name to execute
      # args - arguments to pass to the command
      def command_safe(debug, cmd = :command, *args)
        # send is internal ruby method to call 'cmd' with *args
        #(everything is a message, so this is just the formal semantics to support that idiom)
        translate_hbase_exceptions(*args) { send(cmd,*args) }
      rescue => e
        rootCause = e
        while rootCause != nil && rootCause.respond_to?(:cause) && rootCause.cause != nil
          rootCause = rootCause.cause
        end
        if @shell.interactive?
          puts
          puts "ERROR: #{rootCause}"
          puts "Backtrace: #{rootCause.backtrace.join("\n           ")}" if debug
          puts
          puts "Here is some help for this command:"
          puts help
          puts
        else
          raise rootCause
        end
      end

      # Convenience functions to get different admins

      # Returns HBase::Admin ruby class.
      def admin
        @shell.hbase_admin
      end

      def taskmonitor
        @shell.hbase_taskmonitor
      end

      def table(name)
        @shell.hbase_table(name)
      end

      def replication_admin
        @shell.hbase_replication_admin
      end

      def security_admin
        @shell.hbase_security_admin
      end

      def visibility_labels_admin
        @shell.hbase_visibility_labels_admin
      end

      def quotas_admin
        @shell.hbase_quotas_admin
      end

      def rsgroup_admin
        @shell.hbase_rsgroup_admin
      end

      #----------------------------------------------------------------------
      # Creates formatter instance first time and then reuses it.
      def formatter
        @formatter ||= ::Shell::Formatter::Console.new
      end

      def format_simple_command
        now = Time.now
        yield
        formatter.header
        formatter.footer(now)
      end

      def format_and_return_simple_command
        now = Time.now
        ret = yield
        formatter.header
        formatter.footer(now)
        return ret
      end

      def translate_hbase_exceptions(*args)
        yield
      rescue => e
        # Since exceptions will be thrown from the java code, 'e' will always be NativeException.
        # Check for the original java exception and use it if present.
        raise e unless e.respond_to?(:cause) && e.cause != nil
        cause = e.cause

         # let individual command handle exceptions first
        if self.respond_to?(:handle_exceptions)
          self.handle_exceptions(cause, *args)
        end
        # Global HBase exception handling below if not handled by respective command above
        if cause.kind_of?(org.apache.hadoop.hbase.TableNotFoundException) then
          raise "Unknown table #{args.first}!"
        end
        if cause.kind_of?(org.apache.hadoop.hbase.UnknownRegionException) then
          raise "Unknown region #{args.first}!"
        end
        if cause.kind_of?(org.apache.hadoop.hbase.NamespaceNotFoundException) then
          raise "Unknown namespace #{args.first}!"
        end
        if cause.kind_of?(org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException) then
          raise "Unknown snapshot #{args.first}!"
        end
        if cause.kind_of?(org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException) then
          exceptions = cause.getCauses
          exceptions.each do |exception|
            if exception.kind_of?(org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException) then
              valid_cols = table(args.first).get_all_columns.map { |c| c + '*' }
              raise "Unknown column family! Valid column names: #{valid_cols.join(", ")}"
            end
          end
        end
        if cause.kind_of?(org.apache.hadoop.hbase.TableExistsException) then
          raise "Table already exists: #{args.first}!"
        end
        # To be safe, here only AccessDeniedException is considered. In future
        # we might support more in more generic approach when possible.
        if cause.kind_of?(org.apache.hadoop.hbase.security.AccessDeniedException) then
          str = java.lang.String.new("#{cause}")
          # Error message is merged with stack trace, reference StringUtils.stringifyException
          # This is to parse and get the error message from the whole.
          strs = str.split("\n")
          if strs.size > 0 then
            raise "#{strs[0]}"
          end
        end

        # Throw the other exception which hasn't been handled above
        raise e
      end
    end
  end
end
