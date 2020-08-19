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

      infoport = @admin.getClusterMetrics.getLiveServerMetrics.get(host).getInfoServerPort.to_s

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
  end
end
