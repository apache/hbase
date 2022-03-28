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

require 'rubygems'
require 'rake'
require 'set'

puts "Ruby description: #{RUBY_DESCRIPTION}"

unless defined?($TEST_CLUSTER)
  include Java

  # Set logging level to avoid verboseness
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.hdfs").setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.ipc.HBaseServer").setLevel(org.apache.log4j.Level::OFF)

  java_import org.apache.hadoop.hbase.HBaseTestingUtility

  $TEST_CLUSTER = HBaseTestingUtility.new
  $TEST_CLUSTER.configuration.setInt("hbase.regionserver.msginterval", 100)
  $TEST_CLUSTER.configuration.setInt("hbase.client.pause", 250)
  $TEST_CLUSTER.configuration.set("hbase.quota.enabled", "true")
  $TEST_CLUSTER.configuration.set('hbase.master.quotas.snapshot.chore.period', 5000)
  $TEST_CLUSTER.configuration.set('hbase.master.quotas.snapshot.chore.delay', 5000)
  $TEST_CLUSTER.configuration.setInt(org.apache.hadoop.hbase.HConstants::HBASE_CLIENT_RETRIES_NUMBER, 6)
  $TEST_CLUSTER.startMiniCluster
  @own_cluster = true
end

require 'test_helper'

puts "Running tests..."

if java.lang.System.get_property('shell.test.include')
  includes = Set.new(java.lang.System.get_property('shell.test.include').split(','))
end

if java.lang.System.get_property('shell.test.exclude')
  excludes = Set.new(java.lang.System.get_property('shell.test.exclude').split(','))
end

files = Dir[ File.dirname(__FILE__) + "/**/*_test.rb" ]
files.each do |file|
  filename = File.basename(file)
  if includes != nil && !includes.include?(filename)
    puts "Skip #{filename} because of not included"
    next
  end
  if excludes != nil && excludes.include?(filename)
    puts "Skip #{filename} because of excluded"
    next
  end
  begin
    puts "loading test file '#{filename}'."
    load(file)
  rescue => e
    puts "ERROR: #{e}"
    raise
  end
end

# If this system property is set, we'll use it to filter the test cases.
runner_args = []
if java.lang.System.get_property('shell.test')
  shell_test_pattern = java.lang.System.get_property('shell.test')
  puts "Only running tests that match #{shell_test_pattern}"
  runner_args << "--testcase=#{shell_test_pattern}"
end
begin
  # first couple of args are to match the defaults, so we can pass options to limit the tests run
  unless Test::Unit::AutoRunner.run(false, nil, runner_args)
    raise 'Shell unit tests failed. Check output file for details.'
  end
rescue SystemExit => e
  # Unit tests should not raise uncaught SystemExit exceptions. This could cause tests to be ignored.
  raise 'Caught SystemExit during unit test execution! Check output file for details.'
end

puts "Done with tests! Shutting down the cluster..."
if @own_cluster
  $TEST_CLUSTER.shutdownMiniCluster
  java.lang.System.exit(0)
end
