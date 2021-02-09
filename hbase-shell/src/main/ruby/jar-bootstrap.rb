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
# File passed to org.jruby.Main by bin/hbase.  Pollutes jirb with hbase imports
# and hbase  commands and then loads jirb.  Outputs a banner that tells user
# where to find help, shell version, and loads up a custom hirb.
#
# In noninteractive mode, runs commands from stdin until completion or an error.
# On success will exit with status 0, on any problem will exit non-zero. Callers
# should only rely on "not equal to 0", because the current error exit code of 1
# will likely be updated to diffentiate e.g. invalid commands, incorrect args,
# permissions, etc.

# TODO: Interrupt a table creation or a connection to a bad master.  Currently
# has to time out.  Below we've set down the retries for rpc and hbase but
# still can be annoying (And there seem to be times when we'll retry for
# ever regardless)
# TODO: Add support for listing and manipulating catalog tables, etc.
# TODO: Encoding; need to know how to go from ruby String to UTF-8 bytes

# Run the java magic include and import basic HBase types that will help ease
# hbase hacking.
include Java

# Some goodies for hirb. Should these be left up to the user's discretion?
require 'irb/completion'
require 'pathname'
require 'getoptlong'

# Add the directory names in hbase.jruby.sources commandline option
# to the ruby load path so I can load up my HBase ruby modules
# in case we are trying to get them out of source instead of jar
# packaging.
sources = java.lang.System.getProperty('hbase.ruby.sources')
unless sources.nil?
  $LOAD_PATH.unshift Pathname.new(sources)
end

cmdline_help = <<HERE # HERE document output as shell usage
Usage: shell [OPTIONS] [SCRIPTFILE [ARGUMENTS]]

 -d | --debug            Set DEBUG log levels.
 -h | --help             This help.
 -n | --noninteractive   Do not run within an IRB session and exit with non-zero
                         status on first error.
 --top-level-defs        Compatibility flag to export HBase shell commands onto
                         Ruby's main object
 -Dkey=value             Pass hbase-*.xml Configuration overrides. For example, to
                         use an alternate zookeeper ensemble, pass:
                           -Dhbase.zookeeper.quorum=zookeeper.example.org
                         For faster fail, pass the below and vary the values:
                           -Dhbase.client.retries.number=7
                           -Dhbase.ipc.client.connect.max.retries=3
HERE

# Takes configuration and an arg that is expected to be key=value format.
# If c is empty, creates one and returns it
def add_to_configuration(c, arg)
  kv = arg.split('=')
  kv.length == 2 || (raise "Expected parameter #{kv} in key=value format")
  c = org.apache.hadoop.hbase.HBaseConfiguration.create if c.nil?
  c.set(kv[0], kv[1])
  c
end

opts = GetoptLong.new(
  [ '--help', '-h', GetoptLong::NO_ARGUMENT ],
  [ '--debug', '-d', GetoptLong::OPTIONAL_ARGUMENT ],
  [ '--noninteractive', '-n', GetoptLong::OPTIONAL_ARGUMENT ],
  [ '--top-level-defs', GetoptLong::OPTIONAL_ARGUMENT ],
  [ '--Dkey=value', '-D', GetoptLong::NO_ARGUMENT ]
)

found = []
script2run = nil
log_level = org.apache.log4j.Level::ERROR
@shell_debug = false
interactive = true
top_level_definitions = false
_configuration = nil
D_ARG = '-D'.freeze

opts.each do |opt, arg|
  case opt || arg
  when '--help' || '-h'
    puts cmdline_help
  when D_ARG
    argValue = ARGV.shift || (raise "#{D_ARG} takes a 'key=value' parameter")
    _configuration = add_to_configuration(_configuration, argValue)
    found.push(arg)
    found.push(argValue)
  when arg.start_with?(D_ARG)
    _configuration = add_to_configuration(_configuration, arg[2..-1])
    found.push(arg)
  when '--debug'|| '-d'
    log_level = org.apache.log4j.Level::DEBUG
    $fullBackTrace = true
    @shell_debug = true
    found.push(arg)
    puts 'Setting DEBUG log level...'
   when '--noninteractive'||  '-n'
     interactive = false
     found.push(arg)
   when '--return-values' || 'r'
     warn '[INFO] the -r | --return-values option is ignored. we always behave '\
           'as though it was given.'
     found.push(arg)
   when '--top-level-defs'
     top_level_definitions = true
   else
      # Presume it a script. Save it off for running later below
      # after we've set up some environment.
      script2run = arg
      found.push(arg)
      # Presume that any other args are meant for the script.
  end
end


# Delete all processed args
found.each { |arg| ARGV.delete(arg) }
# Make sure debug flag gets back to IRB
ARGV.unshift('-d') if @shell_debug

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger('org.apache.zookeeper').setLevel(log_level)
org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').setLevel(log_level)

# Require HBase now after setting log levels
require 'hbase_constants'

# Load hbase shell
require 'hbase_shell'

# Require formatter
require 'shell/formatter'

# Setup the HBase module.  Create a configuration.
@hbase = _configuration.nil? ? Hbase::Hbase.new : Hbase::Hbase.new(_configuration)

# Setup console
@shell = Shell::Shell.new(@hbase, interactive)
@shell.debug = @shell_debug

##
# Toggle shell debugging
#
# @return [Boolean] true if debug is turned on after updating the flag
def debug
  if @shell_debug
    @shell_debug = false
    conf.back_trace_limit = 0
    log_level = org.apache.log4j.Level::ERROR
  else
    @shell_debug = true
    conf.back_trace_limit = 100
    log_level = org.apache.log4j.Level::DEBUG
  end
  org.apache.log4j.Logger.getLogger('org.apache.zookeeper').setLevel(log_level)
  org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').setLevel(log_level)
  debug?
end

##
# Print whether debug is on or off
def debug?
  puts "Debug mode is #{@shell_debug ? 'ON' : 'OFF'}\n\n"
  nil
end


# For backwards compatibility, this will export all the HBase shell commands, constants, and
# instance variables (@hbase and @shell) onto Ruby's top-level receiver object known as "main".
@shell.export_all(self) if top_level_definitions

# If script2run, try running it.  If we're in interactive mode, will go on to run the shell unless
# script calls 'exit' or 'exit 0' or 'exit errcode'.
require 'shell/hbase_loader'
if script2run
  ::Shell::Shell.exception_handler(!$fullBackTrace) { @shell.eval_io(Hbase::Loader.file_for_load(script2run), filename = script2run) }
end

# If we are not running interactively, evaluate standard input
::Shell::Shell.exception_handler(!$fullBackTrace) { @shell.eval_io(STDIN) } unless interactive

if interactive
  # Output a banner message that tells users where to go for help
  @shell.print_banner

  require 'irb'
  require 'irb/ext/change-ws'
  require 'irb/hirb'

  module IRB
    # Override of the default IRB.start
    def self.start(ap_path = nil)
      $0 = File.basename(ap_path, '.rb') if ap_path

      IRB.setup(ap_path)
      IRB.conf[:PROMPT][:CUSTOM] = {
        :PROMPT_I => "%N:%03n:%i> ",
        :PROMPT_S => "%N:%03n:%i%l ",
        :PROMPT_C => "%N:%03n:%i* ",
        :RETURN => "=> %s\n"
      }

      @CONF[:IRB_NAME] = 'hbase'
      @CONF[:AP_NAME] = 'hbase'
      @CONF[:PROMPT_MODE] = :CUSTOM
      @CONF[:BACK_TRACE_LIMIT] = 0 unless $fullBackTrace

      hirb = if @CONF[:SCRIPT]
               HIRB.new(nil, @CONF[:SCRIPT])
             else
               HIRB.new
             end

      shl = TOPLEVEL_BINDING.receiver.instance_variable_get :'@shell'
      hirb.context.change_workspace shl.get_workspace

      @CONF[:IRB_RC].call(hirb.context) if @CONF[:IRB_RC]
      # Storing our current HBase IRB Context as the main context is imperative for several reasons,
      # including auto-completion.
      @CONF[:MAIN_CONTEXT] = hirb.context

      catch(:IRB_EXIT) do
        hirb.eval_input
      end
    end
  end

  IRB.start
end
