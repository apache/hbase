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

module Hbase
  ##
  # HBase::Loader serves a similar purpose to IRB::IrbLoader, but with a different separation of
  # concerns. This loader allows us to directly get the path for a filename in ruby's load path,
  # and then use that in combination with something like HBase::Shell#eval_io.
  module Loader
    ##
    # Determine the loadable path for a given filename by searching through $LOAD_PATH
    #
    # This serves a similar purpose to IRB::IrbLoader#search_file_from_ruby_path, but uses JRuby's
    # loader, which allows us to find special paths like "uri:classloader" inside of a Jar.
    #
    # @param [String] filename
    # @return [String] path
    def self.path_for_load(filename)
      return File.absolute_path(filename) if File.exist? filename

      # Get JRuby's LoadService from the global (singleton) instance of the runtime
      # (org.jruby.Ruby), which allows us to use JRuby's tools for searching the load path.
      runtime = org.jruby.Ruby.getGlobalRuntime
      loader = runtime.getLoadService
      search_state = loader.findFileForLoad filename
      raise LoadError, "no such file to load -- #{filename}" if search_state.library.nil?

      search_state.loadName
    end

    ##
    # Return a file handle for the given file found in the load path
    #
    # @param [String] filename
    # @return [File] file handle
    def self.file_for_load(filename)
      File.new(path_for_load(filename))
    end
  end
end
