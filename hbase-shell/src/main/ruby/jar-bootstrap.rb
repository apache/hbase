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
if $stdin.tty?
  require 'irb/completion'
end
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

conf_from_cli = nil

# strip out any config definitions that won't work with GetoptLong
D_ARG = '-D'.freeze
ARGV.delete_if do |arg|
  if arg.start_with?(D_ARG) && arg.include?('=')
    conf_from_cli = add_to_configuration(conf_from_cli, arg[2..-1])
    true
  else
    false
  end
end

opts = GetoptLong.new(
  ['--help', '-h', GetoptLong::NO_ARGUMENT],
  ['--debug', '-d', GetoptLong::NO_ARGUMENT],
  ['--noninteractive', '-n', GetoptLong::NO_ARGUMENT],
  ['--top-level-defs', GetoptLong::NO_ARGUMENT],
  ['-D', GetoptLong::REQUIRED_ARGUMENT],
  ['--return-values', '-r', GetoptLong::NO_ARGUMENT]
)
opts.ordering = GetoptLong::REQUIRE_ORDER

script2run = nil
log_level = org.apache.log4j.Level::ERROR
@shell_debug = false
interactive = true
full_backtrace = false
top_level_definitions = false

opts.each do |opt, arg|
  case opt
  when '--help'
    puts cmdline_help
    exit
  when D_ARG
    conf_from_cli = add_to_configuration(conf_from_cli, arg)
  when '--debug'
    log_level = org.apache.log4j.Level::DEBUG
    full_backtrace = true
    @shell_debug = true
    puts 'Setting DEBUG log level...'
  when '--noninteractive'
    interactive = false
  when '--return-values'
    warn '[INFO] the -r | --return-values option is ignored. we always behave '\
           'as though it was given.'
  when '--top-level-defs'
    top_level_definitions = true
  end
end

script2run = ARGV.shift unless ARGV.empty?

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
@hbase = conf_from_cli.nil? ? Hbase::Hbase.new : Hbase::Hbase.new(conf_from_cli)

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

require 'irb'
require 'irb/ext/change-ws'
require 'irb/hirb'

# Configure IRB
IRB.setup(nil)
IRB.conf[:PROMPT][:CUSTOM] = {
  PROMPT_I: '%N:%03n:%i> ',
  PROMPT_S: '%N:%03n:%i%l ',
  PROMPT_C: '%N:%03n:%i* ',
  RETURN: "=> %s\n"
}

IRB.conf[:IRB_NAME] = 'hbase'
IRB.conf[:AP_NAME] = 'hbase'
IRB.conf[:PROMPT_MODE] = :CUSTOM
IRB.conf[:BACK_TRACE_LIMIT] = 0 unless full_backtrace

# Create a workspace we'll use across sessions.
workspace = @shell.get_workspace

# If script2run, try running it.  If we're in interactive mode, will go on to run the shell unless
# script calls 'exit' or 'exit 0' or 'exit errcode'.
if script2run
  ::Shell::Shell.exception_handler(!full_backtrace) do
    IRB::HIRB.new(workspace, interactive, IRB::HBaseLoader.file_for_load(script2run)).run
  end
  exit @shell.exit_code unless @shell.exit_code.nil?
end

if interactive
  # Output a banner message that tells users where to go for help
  @shell.print_banner
end
IRB::HIRB.new(workspace, interactive).run
exit @shell.exit_code unless interactive || @shell.exit_code.nil?
