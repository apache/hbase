#
# Copyright 2010 The Apache Software Foundation
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
    class Processlist < Command
      def help
        <<-EOF
Show regionserver task list.

  hbase> processlist
  hbase> processlist 'all'
  # list non-RPC Tasks, such as compact, flush etc
  hbase> processlist 'general'
  # list RPC Handler Tasks
  hbase> processlist 'handler'
  # list RPC Handler Tasks which state is RUNNING
  hbase> processlist 'rpc'
  # list RPC Handler Tasks which state is RUNNING and from client
  hbase> processlist 'operation'
  hbase> processlist 'all','host187.example.com'
  hbase> processlist 'all','host187.example.com,16020'
  hbase> processlist 'all','host187.example.com,16020,1289493121758'

EOF
      end

      def command(*args)
        if %w[all general handler rpc operation].include? args[0]
          # if the first argument is a valid filter specifier, use it as such
          filter = args[0]
          hosts = args[1, args.length]
        else
          # otherwise, treat all arguments as host addresses by default
          filter = 'general'
          hosts = args
        end

        hosts = admin.getServerNames(hosts, true)

        if hosts.nil?
          puts 'No regionservers available.'
        else
          _processlist_impl(filter, hosts)
        end
      end

      def _processlist_impl(filter, hosts)
        # put all tasks on all requested hosts in the same list
        tasks = []
        hosts.each do |host|
          tasks.concat(taskmonitor.tasksOnHost(filter, host))
        end

        puts(format('%d tasks as of: %s', tasks.size, Time.now.strftime('%Y-%m-%d %H:%M:%S')))

        if tasks.empty?
          puts('No ' + filter + ' tasks currently running.')
          return
        end

        # determine table width
        longestStatusWidth = 0
        longestDescriptionWidth = 0
        tasks.each do |t|
          longestStatusWidth = [longestStatusWidth, t.status.length].max
          longestDescriptionWidth = [longestDescriptionWidth, t.description.length].max
        end

        # set the maximum character width of each column, without padding
        hostWidth = 15
        startTimeWidth = 19
        stateWidth = 8
        descriptionWidth = [32, longestDescriptionWidth].min
        statusWidth = [36, longestStatusWidth + 27].min

        headers = ['Host', 'Start Time', 'State', 'Description', 'Status']
        widths = [hostWidth, startTimeWidth, stateWidth, descriptionWidth, statusWidth]

        table_formatter.start_table({ headers: headers, widths: widths })
        tasks.each do |t|
          table_formatter.row(task_to_row(t))
        end
        table_formatter.close_table
        nil
      end

      def task_to_row(t)
        [
          t.host.hostname,
          t.starttime.strftime('%Y-%m-%d %H:%M:%S'),
          t.state,
          t.description,
          format('%s (since %d seconds ago)', t.status, Time.now - t.statustime)
        ]
      end
    end
  end
end
