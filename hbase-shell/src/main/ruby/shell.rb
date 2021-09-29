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
require 'irb'
require 'irb/workspace'

#
# Simple class to act as the main receiver for an IRB Workspace (and its respective ruby Binding)
# in our HBase shell. This will hold all the commands we want in our shell.
#
class HBaseReceiver < Object
  def get_binding
    binding
  end
end

##
# HBaseIOExtensions is a module to be "mixed-in" (ie. included) to Ruby's IO class. It is required
# if you want to use RubyLex with an IO object. RubyLex claims to take an IO but really wants an
# InputMethod.
module HBaseIOExtensions
  def encoding
    external_encoding
  end
end


# Shell commands module
module Shell
  @@commands = {}
  def self.commands
    @@commands
  end

  @@command_groups = {}
  def self.command_groups
    @@command_groups
  end

  def self.load_command(name, group, aliases = [])
    return if commands[name]

    # Register command in the group
    raise ArgumentError, "Unknown group: #{group}" unless command_groups[group]
    command_groups[group][:commands] << name

    # Load command
    begin
      require "shell/commands/#{name}"
      klass_name = name.to_s.gsub(/(?:^|_)(.)/) { Regexp.last_match(1).upcase } # camelize
      commands[name] = eval("Commands::#{klass_name}")
      aliases.each do |an_alias|
        commands[an_alias] = commands[name]
      end
    rescue => e
      raise "Can't load hbase shell command: #{name}. Error: #{e}\n#{e.backtrace.join("\n")}"
    end
  end

  def self.load_command_group(group, opts)
    raise ArgumentError, "No :commands for group #{group}" unless opts[:commands]

    command_groups[group] = {
      commands: [],
      command_names: opts[:commands],
      full_name: opts[:full_name] || group,
      comment: opts[:comment]
    }

    all_aliases = opts[:aliases] || {}

    opts[:commands].each do |command|
      aliases = all_aliases[command] || []
      load_command(command, group, aliases)
    end
  end

  #----------------------------------------------------------------------
  # rubocop:disable Metrics/ClassLength
  class Shell
    attr_accessor :hbase
    attr_accessor :interactive
    alias interactive? interactive

    @debug = false
    attr_accessor :debug

    # keep track of the passed exit code. nil means never called.
    @exit_code = nil
    attr_accessor :exit_code

    alias __exit__ exit
    # exit the interactive shell and save that this
    # happend via a call to exit
    def exit(ret = 0)
      @exit_code = ret
      IRB.irb_exit(IRB.CurrentContext.irb, ret)
    end

    def initialize(hbase, interactive = true)
      self.hbase = hbase
      self.interactive = interactive
    end

    # Returns Admin class from admin.rb
    def admin
      @admin ||= hbase.admin
    end

    def hbase_taskmonitor
      @hbase_taskmonitor ||= hbase.taskmonitor
    end

    def hbase_table(name)
      hbase.table(name, self)
    end

    def hbase_replication_admin
      @hbase_replication_admin ||= hbase.replication_admin
    end

    def hbase_security_admin
      @hbase_security_admin ||= hbase.security_admin
    end

    def hbase_visibility_labels_admin
      @hbase_visibility_labels_admin ||= hbase.visibility_labels_admin
    end

    def hbase_quotas_admin
      @hbase_quotas_admin ||= hbase.quotas_admin
    end

    def hbase_rsgroup_admin
      @rsgroup_admin ||= hbase.rsgroup_admin
    end

    ##
    # Create singleton methods on the target receiver object for all the loaded commands
    #
    # Therefore, export_commands will create "class methods" if passed a Module/Class and if passed
    # an instance the methods will not exist on any other instances of the instantiated class.
    def export_commands(target)
      # We need to store a reference to this Shell instance in the scope of this method so that it
      # can be accessed later in the scope of the target object.
      shell_inst = self
      # Define each method as a lambda. We need to use a lambda (rather than a Proc or block) for
      # its properties: preservation of local variables and return
      ::Shell.commands.keys.each do |cmd|
        target.send :define_singleton_method, cmd.to_sym, lambda { |*args|
          ret = shell_inst.command(cmd.to_s, *args)
          puts
          ret
        }
      end
      # Export help method
      target.send :define_singleton_method, :help, lambda { |command = nil|
        shell_inst.help(command)
        nil
      }
      # Export tools method for backwards compatibility
      target.send :define_singleton_method, :tools, lambda {
        shell_inst.help_group('tools')
        nil
      }
    end

    # Export HBase commands, constants, and variables to target receiver
    def export_all(target)
      raise ArgumentError, 'target should not be a module' if target.is_a? Module

      # add constants to class of target
      target.class.include ::HBaseConstants
      target.class.include ::HBaseQuotasConstants
      # add instance variables @hbase and @shell for backwards compatibility
      target.instance_variable_set :'@hbase', @hbase
      target.instance_variable_set :'@shell', self
      # add commands to target
      export_commands(target)
    end

    def command_instance(command)
      ::Shell.commands[command.to_s].new(self)
    end

    # call the method 'command' on the specified command
    def command(command, *args)
      internal_command(command, :command, *args)
    end

    # call a specific internal method in the command instance
    # command  - name of the command to call
    # method_name - name of the method on the command to call. Defaults to just 'command'
    # args - to be passed to the named method
    def internal_command(command, method_name = :command, *args)
      command_instance(command).command_safe(debug, method_name, *args)
    end

    def print_banner
      puts 'HBase Shell'
      puts 'Use "help" to get list of supported commands.'
      puts 'Use "exit" to quit this interactive shell.'
      puts 'For Reference, please visit: http://hbase.apache.org/2.0/book.html#shell'
      print 'Version '
      command('version')
      puts
    end

    def help_multi_command(command)
      puts "Command: #{command}"
      puts command_instance(command).help
      puts
      nil
    end

    def help_command(command)
      puts command_instance(command).help
      nil
    end

    def help_group(group_name)
      group = ::Shell.command_groups[group_name.to_s]
      group[:commands].sort.each { |cmd| help_multi_command(cmd) }
      if group[:comment]
        puts '-' * 80
        puts
        puts group[:comment]
        puts
      end
      nil
    end

    def help(command = nil)
      if command
        return help_command(command) if ::Shell.commands[command.to_s]
        return help_group(command) if ::Shell.command_groups[command.to_s]
        puts "ERROR: Invalid command or command group name: #{command}"
        puts
      end

      puts help_header
      puts
      puts 'COMMAND GROUPS:'
      ::Shell.command_groups.each do |name, group|
        puts '  Group name: ' + name
        puts '  Commands: ' + group[:command_names].sort.join(', ')
        puts
      end
      unless command
        puts 'SHELL USAGE:'
        help_footer
      end
      nil
    end

    def help_header
      "HBase Shell, version #{org.apache.hadoop.hbase.util.VersionInfo.getVersion}, " \
             "r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision}, " \
             "#{org.apache.hadoop.hbase.util.VersionInfo.getDate}" + "\n" \
             "Type 'help \"COMMAND\"', (e.g. 'help \"get\"' -- the quotes are necessary) for help on a specific command.\n" \
             "Commands are grouped. Type 'help \"COMMAND_GROUP\"', (e.g. 'help \"general\"') for help on a command group."
    end

    def help_footer
      puts <<-HERE
Quote all names in HBase Shell such as table and column names.  Commas delimit
command parameters.  Type <RETURN> after entering a command to run it.
Dictionaries of configuration used in the creation and alteration of tables are
Ruby Hashes. They look like this:

  {'key1' => 'value1', 'key2' => 'value2', ...}

and are opened and closed with curley-braces.  Key/values are delimited by the
'=>' character combination.  Usually keys are predefined constants such as
NAME, VERSIONS, COMPRESSION, etc.  Constants do not need to be quoted.  Type
'Object.constants' to see a (messy) list of all constants in the environment.

If you are using binary keys or values and need to enter them in the shell, use
double-quote'd hexadecimal representation. For example:

  hbase> get 't1', "key\\x03\\x3f\\xcd"
  hbase> get 't1', "key\\003\\023\\011"
  hbase> put 't1', "test\\xef\\xff", 'f1:', "\\x01\\x33\\x40"

The HBase shell is the (J)Ruby IRB with the above HBase-specific commands added.
For more on the HBase Shell, see http://hbase.apache.org/book.html
      HERE
    end

    @irb_workspace = nil
    ##
    # Returns an IRB Workspace for this shell instance with all the IRB and HBase commands installed
    def get_workspace
      return @irb_workspace unless @irb_workspace.nil?

      hbase_receiver = HBaseReceiver.new
      # install all the IRB commands onto our receiver
      IRB::ExtendCommandBundle.extend_object(hbase_receiver)
      # Install all the hbase commands, constants, and instance variables @shell and @hbase. This
      # will override names that conflict with IRB methods like "help".
      export_all(hbase_receiver)
      # make it so calling exit will hit our pass-through rather than going directly to IRB
      hbase_receiver.send :define_singleton_method, :exit, lambda { |rc = 0|
        @shell.exit(rc)
      }
      ::IRB::WorkSpace.new(hbase_receiver.get_binding)
    end

    ##
    # Runs a block and logs exception from both Ruby and Java, optionally discarding the traceback
    #
    # @param [Boolean] hide_traceback if true, Exceptions will be converted to
    #   a SystemExit so that the traceback is not printed
    def self.exception_handler(hide_traceback)
      begin
        yield
      rescue Exception => e
        message = e.to_s
        # exception unwrapping in shell means we'll have to handle Java exceptions
        # as a special case in order to format them properly.
        if e.is_a? java.lang.Exception
          warn 'java exception'
          message = e.get_message
        end
        # Include the 'ERROR' string to try to make transition easier for scripts that
        # may have already been relying on grepping output.
        puts "ERROR #{e.class}: #{message}"
        raise e unless hide_traceback

        exit 1
      end
      nil
    end
  end
  # rubocop:enable Metrics/ClassLength
end

# Load commands base class
require 'shell/commands'

# Load all commands
Shell.load_command_group(
  'general',
  full_name: 'GENERAL HBASE SHELL COMMANDS',
  commands: %w[
    status
    version
    table_help
    whoami
    processlist
  ]
)

Shell.load_command_group(
  'ddl',
  full_name: 'TABLES MANAGEMENT COMMANDS',
  commands: %w[
    alter
    create
    describe
    disable
    disable_all
    is_disabled
    drop
    drop_all
    enable
    enable_all
    is_enabled
    exists
    list
    show_filters
    alter_status
    alter_async
    get_table
    locate_region
    list_regions
    clone_table_schema
  ],
  aliases: {
    'describe' => ['desc']
  }
)

Shell.load_command_group(
  'namespace',
  full_name: 'NAMESPACE MANAGEMENT COMMANDS',
  commands: %w[
    create_namespace
    drop_namespace
    alter_namespace
    describe_namespace
    list_namespace
    list_namespace_tables
  ]
)

Shell.load_command_group(
  'dml',
  full_name: 'DATA MANIPULATION COMMANDS',
  commands: %w[
    count
    delete
    deleteall
    get
    get_counter
    incr
    put
    scan
    truncate
    truncate_preserve
    append
    get_splits
  ]
)

Shell.load_command_group(
  'tools',
  full_name: 'HBASE SURGERY TOOLS',
  comment: "WARNING: Above commands are for 'experts'-only as misuse can damage an install",
  commands: %w[
    assign
    balancer
    balance_switch
    balancer_enabled
    normalize
    normalizer_switch
    normalizer_enabled
    is_in_maintenance_mode
    clear_slowlog_responses
    close_region
    compact
    compaction_switch
    flush
    get_balancer_decisions
    get_balancer_rejections
    get_slowlog_responses
    get_largelog_responses
    major_compact
    move
    split
    merge_region
    unassign
    zk_dump
    wal_roll
    hbck_chore_run
    catalogjanitor_run
    catalogjanitor_switch
    catalogjanitor_enabled
    cleaner_chore_run
    cleaner_chore_switch
    cleaner_chore_enabled
    compact_rs
    compaction_state
    trace
    snapshot_cleanup_switch
    snapshot_cleanup_enabled
    splitormerge_switch
    splitormerge_enabled
    clear_compaction_queues
    list_deadservers
    clear_deadservers
    clear_block_cache
    stop_master
    stop_regionserver
    regioninfo
    rit
    list_decommissioned_regionservers
    decommission_regionservers
    recommission_regionserver
  ],
  # TODO: remove older hlog_roll command
  aliases: {
    'wal_roll' => ['hlog_roll']
  }
)

Shell.load_command_group(
  'replication',
  full_name: 'CLUSTER REPLICATION TOOLS',
  commands: %w[
    add_peer
    remove_peer
    list_peers
    enable_peer
    disable_peer
    set_peer_replicate_all
    set_peer_serial
    set_peer_namespaces
    append_peer_namespaces
    remove_peer_namespaces
    set_peer_exclude_namespaces
    append_peer_exclude_namespaces
    remove_peer_exclude_namespaces
    show_peer_tableCFs
    set_peer_tableCFs
    set_peer_exclude_tableCFs
    append_peer_exclude_tableCFs
    remove_peer_exclude_tableCFs
    set_peer_bandwidth
    list_replicated_tables
    append_peer_tableCFs
    remove_peer_tableCFs
    enable_table_replication
    disable_table_replication
    get_peer_config
    list_peer_configs
    update_peer_config
  ]
)

Shell.load_command_group(
  'snapshots',
  full_name: 'CLUSTER SNAPSHOT TOOLS',
  commands: %w[
    snapshot
    clone_snapshot
    restore_snapshot
    delete_snapshot
    delete_all_snapshot
    delete_table_snapshots
    list_snapshots
    list_table_snapshots
  ]
)

Shell.load_command_group(
  'configuration',
  full_name: 'ONLINE CONFIGURATION TOOLS',
  commands: %w[
    update_config
    update_all_config
  ]
)

Shell.load_command_group(
  'quotas',
  full_name: 'CLUSTER QUOTAS TOOLS',
  commands: %w[
    set_quota
    list_quotas
    list_quota_table_sizes
    list_quota_snapshots
    list_snapshot_sizes
    enable_rpc_throttle
    disable_rpc_throttle
    enable_exceed_throttle_quota
    disable_exceed_throttle_quota
  ]
)

Shell.load_command_group(
  'security',
  full_name: 'SECURITY TOOLS',
  comment: 'NOTE: Above commands are only applicable if running with the AccessController coprocessor',
  commands: %w[
    list_security_capabilities
    grant
    revoke
    user_permission
  ]
)

Shell.load_command_group(
  'procedures',
  full_name: 'PROCEDURES & LOCKS MANAGEMENT',
  commands: %w[
    list_procedures
    list_locks
  ]
)

Shell.load_command_group(
  'visibility labels',
  full_name: 'VISIBILITY LABEL TOOLS',
  comment: 'NOTE: Above commands are only applicable if running with the VisibilityController coprocessor',
  commands: %w[
    add_labels
    list_labels
    set_auths
    get_auths
    clear_auths
    set_visibility
  ]
)

Shell.load_command_group(
  'rsgroup',
  full_name: 'RSGroups',
  comment: "NOTE: The rsgroup Coprocessor Endpoint must be enabled on the Master else commands fail with:
  UnknownProtocolException: No registered Master Coprocessor Endpoint found for RSGroupAdminService",
  commands: %w[
    list_rsgroups
    get_rsgroup
    add_rsgroup
    remove_rsgroup
    balance_rsgroup
    move_servers_rsgroup
    move_tables_rsgroup
    move_namespaces_rsgroup
    move_servers_tables_rsgroup
    move_servers_namespaces_rsgroup
    get_server_rsgroup
    get_table_rsgroup
    remove_servers_rsgroup
    rename_rsgroup
    alter_rsgroup_config
    show_rsgroup_config
    get_namespace_rsgroup
  ]
)
