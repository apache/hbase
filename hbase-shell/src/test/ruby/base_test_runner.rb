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

# Common test runner functionality shared between cluster and no-cluster test runners
module BaseTestRunner
  def self.load_test_files(file_pattern, includes = nil, excludes = nil)
    files = Dir[File.dirname(__FILE__) + "/**/#{file_pattern}"]
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
  end

  def self.run_tests(test_type, runner_args = [])
    begin
      # first couple of args are to match the defaults, so we can pass options to limit the tests run
      unless Test::Unit::AutoRunner.run(false, nil, runner_args)
        raise "Shell integration tests with #{test_type} failed. Check output file for details."
      end
    rescue SystemExit => e
      # Unit tests should not raise uncaught SystemExit exceptions. This could cause tests to be ignored.
      raise 'Caught SystemExit during unit test execution! Check output file for details.'
    end
  end

  def self.get_test_filters
    includes = nil
    excludes = nil

    if java.lang.System.get_property('shell.test.include')
      includes = Set.new(java.lang.System.get_property('shell.test.include').split(','))
    end

    if java.lang.System.get_property('shell.test.exclude')
      excludes = Set.new(java.lang.System.get_property('shell.test.exclude').split(','))
    end

    [includes, excludes]
  end

  def self.get_runner_args
    runner_args = []
    if java.lang.System.get_property('shell.test')
      shell_test_pattern = java.lang.System.get_property('shell.test')
      puts "Only running tests that match #{shell_test_pattern}"
      runner_args << "--testcase=#{shell_test_pattern}"
    end
    runner_args
  end
end
