#
# Copyright 2013 The Apache Software Foundation
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
# This script can be used to make the HMaster reload its configuration 
# from disk.
#
# To run this script, do:
#  ${HBASE_HOME}/bin/hbase org.jruby.Main ${HBASE_HOME}/bin/reload_master_config.rb 
#
include Java
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.commons.logging.LogFactory

# Get configuration to use.
c = HBaseConfiguration.create()

# Taken from add_table.rb script
# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))

NAME = 'reload_master_config.rb'

# Get a logger instance.
LOG = LogFactory.getLog(NAME)

# Get the admin interface
admin = HBaseAdmin.new(c)

# Get the HMaster
master = admin.getMaster()

# Make the HMaster update its configuration
master.updateConfiguration()
LOG.info("Asked the master to update its configuration by reloading it from disk.");

