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
if File.exists?(File.join(File.dirname(__FILE__), "..", "lib"))
    $LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")
end

module Hbase
  class TaskMonitor
    include HBaseConstants

    #---------------------------------------------------------------------------------------------
    # Represents information reported by a server on a single MonitoredTask
    class Task

      def initialize(taskMap,host)

        taskMap.each_pair do |k,v|
          case k
            when "statustimems"
              @statustime = Time.at(v/1000)
            when "status"
              @status = v
            when "starttimems"
              @starttime = Time.at(v/1000)
            when "description"
              @description = v
            when "state"
              @state = v
          end
        end

        @host = host

      end

      def statustime
        # waiting IPC handlers often have statustime = -1, in this case return starttime
        if @statustime > Time.at(-1)
          return @statustime
        end
        return @starttime
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
      @admin = @conn.getAdmin()
    end

    #---------------------------------------------------------------------------------------------------
    # Returns a filtered list of tasks on the given host
    def tasksOnHost(filter,host)

      java_import 'java.net.URL'
      java_import 'org.codehaus.jackson.map.ObjectMapper'

      infoport = @admin.getClusterStatus().getLoad(host).getInfoServerPort().to_s

      # Note: This condition use constants from hbase-server
      #if (!@admin.getConfiguration().getBoolean(org.apache.hadoop.hbase.http.ServerConfigurationKeys::HBASE_SSL_ENABLED_KEY,
      #  org.apache.hadoop.hbase.http.ServerConfigurationKeys::HBASE_SSL_ENABLED_DEFAULT))
      #  schema = "http://"
      #else
      #  schema = "https://"
      #end
      schema = "http://"
      url = schema + host.hostname + ":" + infoport + "/rs-status?format=json&filter=" + filter

      json = URL.new(url)
      mapper = ObjectMapper.new

      # read and parse JSON
      tasksArrayList = mapper.readValue(json,java.lang.Object.java_class)

      # convert to an array of TaskMonitor::Task instances
      tasks = Array.new
      tasksArrayList.each do |t|
        tasks.unshift Task.new(t,host)
      end

      return tasks

    end

    #---------------------------------------------------------------------------------------------------
    # Prints a table of filtered tasks on requested hosts
    def tasks(filter,hosts)

      # put all tasks on all requested hosts in the same list
      tasks = []
      hosts.each do |host|
        tasks.concat(tasksOnHost(filter,host))
      end

      puts("%d tasks as of: %s" % [tasks.size,Time.now.strftime("%Y-%m-%d %H:%M:%S")])

      if tasks.size() == 0
        puts("No " + filter + " tasks currently running.")
      else

        # determine table width
        longestStatusWidth = 0
        longestDescriptionWidth = 0
        tasks.each do |t|
          longestStatusWidth = [longestStatusWidth,t.status.length].max
          longestDescriptionWidth = [longestDescriptionWidth,t.description.length].max
        end

        # set the maximum character width of each column, without padding
        hostWidth = 15
        startTimeWidth = 19
        stateWidth = 8
        descriptionWidth = [32,longestDescriptionWidth].min
        statusWidth = [36,longestStatusWidth + 27].min

        rowSeparator = "+" + "-" * (hostWidth + 2) +
            "+" + "-" * (startTimeWidth + 2) +
            "+" + "-" * (stateWidth + 2) +
            "+" + "-" * (descriptionWidth + 2) +
            "+" + "-" * (statusWidth + 2) + "+"

        # print table header
        cells = [setCellWidth("Host",hostWidth),
              setCellWidth("Start Time",startTimeWidth),
              setCellWidth("State",stateWidth),
              setCellWidth("Description",descriptionWidth),
              setCellWidth("Status",statusWidth)]

        line = "| %s | %s | %s | %s | %s |" % cells

        puts(rowSeparator)
        puts(line)

        # print table content
        tasks.each do |t|

          cells = [setCellWidth(t.host.hostname,hostWidth),
              setCellWidth(t.starttime.strftime("%Y-%m-%d %H:%M:%S"),startTimeWidth),
              setCellWidth(t.state,stateWidth),
              setCellWidth(t.description,descriptionWidth),
              setCellWidth("%s (since %d seconds ago)" %
                           [t.status,Time.now - t.statustime], statusWidth)]

          line = "| %s | %s | %s | %s | %s |" % cells

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
    def setCellWidth(cellContent,width)
      numCharsTooShort = width-cellContent.length
      if numCharsTooShort < 0
        # cellContent is too long, so truncate
        return cellContent[0,[width-3,0].max] + "." * [3,width].min
      else
        # cellContent is requested width or too short, so right-pad with zero or more spaces
        return cellContent + " " * numCharsTooShort
      end
    end

  end
end
