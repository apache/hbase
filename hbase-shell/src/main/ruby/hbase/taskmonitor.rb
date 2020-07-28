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

include Java

# Add the $HBASE_HOME/lib directory to the ruby load_path to load jackson
if File.exist?(File.join(File.dirname(__FILE__), '..', 'lib'))
  $LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', 'lib')
end

module Hbase
  class TaskMonitor
    #---------------------------------------------------------------------------------------------
    # Represents information reported by a server on a single MonitoredTask
    class Task
      def initialize(taskMap, host)
        taskMap.entrySet.each do |entry|
          k = entry.getKey
          v = entry.getValue
          case k
          when 'statustimems'
            @statustime = Time.at(v.getAsLong / 1000)
          when 'status'
            @status = v.getAsString
          when 'starttimems'
            @starttime = Time.at(v.getAsLong / 1000)
          when 'description'
            @description = v.getAsString
          when 'state'
            @state = v.getAsString
          end
        end

        @host = host
      end

      def statustime
        # waiting IPC handlers often have statustime = -1, in this case return starttime
        return @statustime if @statustime > Time.at(-1)
        @starttime
      end

      attr_reader :host
      attr_reader :status
      attr_reader :starttime
      attr_reader :description
      attr_reader :state
    end

    def initialize(configuration)
      @conf = configuration
      @conn = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(@conf)
      @admin = @conn.getAdmin
    end

    #---------------------------------------------------------------------------------------------------
    # Returns a filtered list of tasks on the given host
    def tasksOnHost(filter, host)
      java_import 'java.net.URL'
      java_import 'java.net.SocketException'
      java_import 'java.io.InputStreamReader'
      java_import 'org.apache.hbase.thirdparty.com.google.gson.JsonParser'

      infoport = @admin.getClusterStatus.getLoad(host).getInfoServerPort.to_s

      begin
        schema = "http://"
        url = schema + host.hostname + ':' + infoport + '/rs-status?format=json&filter=' + filter
        json = URL.new(url).openStream
      rescue SocketException => e
        # Let's try with https when SocketException occur
        schema = "https://"
        url = schema + host.hostname + ':' + infoport + '/rs-status?format=json&filter=' + filter
        json = URL.new(url).openStream
      end

      parser = JsonParser.new

      # read and parse JSON
      begin
        tasks_array_list = parser.parse(InputStreamReader.new(json, 'UTF-8')).getAsJsonArray
      ensure
        json.close
      end
      # convert to an array of TaskMonitor::Task instances
      tasks = []
      tasks_array_list.each do |t|
        tasks.unshift Task.new(t.getAsJsonObject, host)
      end

      tasks
    end

    #---------------------------------------------------------------------------------------------------
    # Prints a table of filtered tasks on requested hosts
    def tasks(filter, hosts)
      # put all tasks on all requested hosts in the same list
      tasks = []
      hosts.each do |host|
        tasks.concat(tasksOnHost(filter, host))
      end

      puts(format('%d tasks as of: %s', tasks.size, Time.now.strftime('%Y-%m-%d %H:%M:%S')))

      if tasks.empty?
        puts('No ' + filter + ' tasks currently running.')
      else

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

        rowSeparator = '+' + '-' * (hostWidth + 2) +
                       '+' + '-' * (startTimeWidth + 2) +
                       '+' + '-' * (stateWidth + 2) +
                       '+' + '-' * (descriptionWidth + 2) +
                       '+' + '-' * (statusWidth + 2) + '+'

        # print table header
        cells = [setCellWidth('Host', hostWidth),
                 setCellWidth('Start Time', startTimeWidth),
                 setCellWidth('State', stateWidth),
                 setCellWidth('Description', descriptionWidth),
                 setCellWidth('Status', statusWidth)]

        line = format('| %s | %s | %s | %s | %s |', *cells)

        puts(rowSeparator)
        puts(line)

        # print table content
        tasks.each do |t|
          cells = [setCellWidth(t.host.hostname, hostWidth),
                   setCellWidth(t.starttime.strftime('%Y-%m-%d %H:%M:%S'), startTimeWidth),
                   setCellWidth(t.state, stateWidth),
                   setCellWidth(t.description, descriptionWidth),
                   setCellWidth(format('%s (since %d seconds ago)', t.status, Time.now - t.statustime), statusWidth)]

          line = format('| %s | %s | %s | %s | %s |', *cells)

          puts(rowSeparator)
          puts(line)
        end
        puts(rowSeparator)

      end
    end

    #---------------------------------------------------------------------------------------------------
    #
    # Helper methods
    #

    # right-pad with spaces or truncate with ellipses to match passed width
    def setCellWidth(cellContent, width)
      numCharsTooShort = width - cellContent.length
      if numCharsTooShort < 0
        # cellContent is too long, so truncate
        return cellContent[0, [width - 3, 0].max] + '.' * [3, width].min
      else
        # cellContent is requested width or too short, so right-pad with zero or more spaces
        return cellContent + ' ' * numCharsTooShort
      end
    end
  end
end
