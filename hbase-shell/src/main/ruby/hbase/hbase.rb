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

include Java
java_import org.apache.hadoop.hbase.client.ConnectionFactory
java_import org.apache.hadoop.hbase.HBaseConfiguration

require 'hbase/admin'
require 'hbase/table'
require 'hbase/taskmonitor'
require 'hbase/quotas'
require 'hbase/security'
require 'hbase/visibility_labels'

module Hbase
  class Hbase
    attr_accessor :configuration

    def initialize(config = nil)
      # Create configuration
      if config
        self.configuration = config
      else
        self.configuration = HBaseConfiguration.create
        # Turn off retries in hbase and ipc.  Human doesn't want to wait on N retries.
        configuration.setInt('hbase.client.retries.number', 7)
        configuration.setInt('hbase.ipc.client.connect.max.retries', 3)
      end
      @connection = ConnectionFactory.createConnection(configuration)
    end

    # Returns ruby's Admin class from admin.rb
    def admin
      ::Hbase::Admin.new(@connection)
    end

    def rsgroup_admin
      ::Hbase::RSGroupAdmin.new(@connection)
    end

    def taskmonitor
      ::Hbase::TaskMonitor.new(configuration)
    end

    # Create new one each time
    def table(table, shell)
      ::Hbase::Table.new(@connection.getTable(TableName.valueOf(table)), shell)
    end

    def replication_admin
      ::Hbase::RepAdmin.new(configuration)
    end

    def security_admin
      ::Hbase::SecurityAdmin.new(@connection.getAdmin)
    end

    def visibility_labels_admin
      ::Hbase::VisibilityLabelsAdmin.new(@connection.getAdmin)
    end

    def quotas_admin
      ::Hbase::QuotasAdmin.new(@connection.getAdmin)
    end

    def shutdown
      @connection.close
    end
  end
end
