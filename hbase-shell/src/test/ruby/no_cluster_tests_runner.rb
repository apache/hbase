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

# This runner will only launch shell tests that don't require a HBase cluster running.
# Please keep any relevant changes in sync between all *tests_runner.rb

require_relative 'base_test_runner'

puts "Ruby description: #{RUBY_DESCRIPTION}"

require 'test_helper'

puts "Running tests without a cluster..."

# Get test filters and runner args
includes, excludes = BaseTestRunner.get_test_filters
runner_args = BaseTestRunner.get_runner_args

# Load test files
BaseTestRunner.load_test_files("*_no_cluster.rb", includes, excludes)

# Run tests
BaseTestRunner.run_tests("no_cluster", runner_args)