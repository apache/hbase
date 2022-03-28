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

require 'shell/formatter'

module Shell
  module Commands
    # rubocop:disable Metrics/ClassLength
    class Command
      def initialize(shell)
        @shell = shell
      end

      # gets the name that an operator would type into the shell

      def command_name
        klass_name = self.class.name.split('::').last
        command = klass_name.gsub(/([^\^])([A-Z])/, '\1_\2').downcase
        command
      end

      # wrap an execution of cmd to catch hbase exceptions
      # cmd - command name to execute
      # args - arguments to pass to the command

      # rubocop:disable Metrics/AbcSize
      def command_safe(debug, cmd = :command, *args)
        # Commands can overwrite start_time to skip time used in some kind of setup.
        # See count.rb for example.
        @start_time = Time.now
        # send is internal ruby method to call 'cmd' with *args
        # (everything is a message, so this is just the formal semantics to support that idiom)
        translate_hbase_exceptions(*args) { send(cmd, *args) }
      rescue => e
        rootCause = e

        # JRuby9000 made RubyException respond to cause, ignore it for back compat
        while !rootCause.is_a?(Exception) && rootCause.respond_to?(:cause) && !rootCause.cause.nil?
          rootCause = rootCause.cause
        end
        if @shell.interactive?
          puts
          puts "ERROR: #{rootCause}"
          puts "Backtrace: #{rootCause.backtrace.join("\n           ")}" if debug
          puts
          puts "For usage try 'help \"#{command_name}\"'"
          puts
        else
          raise rootCause
        end
      ensure
        # If end_time is not already set by the command, use current time.
        @end_time ||= Time.now
        formatter.output_str(format('Took %.4f seconds', @end_time - @start_time))
      end
      # rubocop:enable Metrics/AbcSize

      # Convenience functions to get different admins
      # Returns HBase::Admin ruby class.
      def admin
        @shell.admin
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

      # for testing purposes to catch the output of the commands
      def set_formatter(formatter)
        @formatter = formatter
      end

      # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
      # rubocop:disable Metrics/MethodLength, Metrics/PerceivedComplexity
      def translate_hbase_exceptions(*args)
        yield
      rescue => cause
        # let individual command handle exceptions first
        cause = cause.getCause if cause.is_a? java.io.UncheckedIOException
        handle_exceptions(cause, *args) if respond_to?(:handle_exceptions)
        # Global HBase exception handling below if not handled by respective command above
        if cause.is_a?(org.apache.hadoop.hbase.TableNotFoundException)
          strs = cause.to_s.split(' ')
          raise "Unknown table #{strs[0]}!" if strs.size == 1
          raise "Unknown table #{args.first}!"
        end
        if cause.is_a?(org.apache.hadoop.hbase.TableNotEnabledException)
          raise "Table #{args.first} is disabled!"
        end
        if cause.is_a?(org.apache.hadoop.hbase.TableNotDisabledException)
          raise "Table #{cause.message} should be disabled!"
        end
        if cause.is_a?(org.apache.hadoop.hbase.UnknownRegionException)
          raise cause.message
        end
        if cause.is_a?(org.apache.hadoop.hbase.exceptions.MergeRegionException)
          strs = cause.message.split("\n")
          raise(strs[0]).to_s unless strs.empty?
        end
        if cause.is_a?(org.apache.hadoop.hbase.NamespaceNotFoundException)
          s = /.*NamespaceNotFoundException: (?<namespace>[^\n]+).*/.match(cause.message)
          raise "Unknown namespace #{s['namespace']}!"
        end
        if cause.is_a?(org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException)
          raise "Unknown snapshot #{args.first}!"
        end
        if cause.is_a?(org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException)
          exceptions = cause.getCauses
          exceptions.each do |exception|
            if exception.is_a?(org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException)
              valid_cols = table(args.first).get_all_columns.map { |c| c + '*' }
              raise "Unknown column family! Valid column names: #{valid_cols.join(', ')}"
            end
          end
        end
        if cause.is_a?(org.apache.hadoop.hbase.TableExistsException)
          strs = cause.to_s.split(' ')
          raise "Table already exists: #{strs[0]}!" if strs.size == 1
          raise "Table already exists: #{args.first}!"
        end
        # To be safe, here only AccessDeniedException is considered. In future
        # we might support more in more generic approach when possible.
        if cause.is_a?(org.apache.hadoop.hbase.security.AccessDeniedException)
          str = java.lang.String.new(cause.to_s)
          # Error message is merged with stack trace, reference StringUtils.stringifyException
          # This is to parse and get the error message from the whole.
          strs = str.split("\n")
          raise (strs[0]).to_s unless strs.empty?
        end
        if cause.is_a?(org.apache.hadoop.hbase.quotas.SpaceLimitingException)
          strs = cause.message.split("\n")
          raise(strs[0]).to_s unless strs.empty?
        end
        if cause.is_a?(org.apache.hadoop.hbase.client.RetriesExhaustedException)
          str = cause.cause.to_s
          regex = /.*RpcThrottlingException: (?<message>[^\n]+).*/
          error = regex.match(str)
          raise error[:message].capitalize unless error.nil?
        end
        if cause.is_a?(org.apache.hadoop.hbase.DoNotRetryIOException)
          regex = /.*UnsupportedOperationException: quota support disabled.*/
          error = regex.match(cause.message)
          error_msg = 'Quota Support disabled. Please enable in configuration.'
          raise error_msg unless error.nil?
        end

        # Throw the other exception which hasn't been handled above
        raise cause
      end
      # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity
      # rubocop:enable Metrics/MethodLength, Metrics/PerceivedComplexity
    end
    # rubocop:enable Metrics/ClassLength
  end
end
