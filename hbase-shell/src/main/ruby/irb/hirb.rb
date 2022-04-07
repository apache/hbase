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
require 'rbconfig'

module IRB
  WINDOZE = RbConfig::CONFIG['host_os'] =~ /mswin|mingw/

  # Subclass of IRB so can intercept methods
  class HIRB < Irb
    def initialize(workspace = nil, interactive = true, input_method = nil)
      # This is ugly.  Our 'help' method above provokes the following message
      # on irb construction: 'irb: warn: can't alias help from irb_help.'
      # Below, we reset the output so its pointed at /dev/null during irb
      # construction just so this message does not come out after we emit
      # the banner.  Other attempts at playing with the hash of methods
      # down in IRB didn't seem to work. I think the worst thing that can
      # happen is the shell exiting because of failed IRB construction with
      # no error (though we're not blanking STDERR)

      # Map the '/dev/null' according to the running platform
      # Under Windows platform the 'dev/null' is not fully compliant with unix,
      # and the 'NUL' object need to be use instead.
      devnull = '/dev/null'
      devnull = 'NUL' if WINDOZE
      f = File.open(devnull, 'w')
      $stdout = f
      # This is a workaround for the jruby issue 1372.
      # The stderr is an input to stty to re-adjust the terminal for the error('stdin isnt a terminal')
      # in case the command is piped with hbase shell(eg - >echo 'list' | bin/hbase shell)
      if $stdin.tty?
        `stty icrnl <&2`
      end
      @interactive = interactive
      super(workspace, input_method)
    ensure
      f.close
      $stdout = STDOUT
    end

    def output_value
      # Suppress output if last_value is 'nil'
      # Otherwise, when user types help, get ugly 'nil'
      # after all output.
      super unless @context.last_value.nil?
    end

    # Copied from irb.rb and overrides the rescue Exception block so the
    # Shell::exception_handler can deal with the exceptions.
    def eval_input
      @scanner.set_prompt do
      |ltype, indent, continue, line_no|
        if ltype
          f = @context.prompt_s
        elsif continue
          f = @context.prompt_c
        elsif indent > 0
          f = @context.prompt_n
        else
          f = @context.prompt_i
        end
        f = "" unless f
        if @context.prompting?
          @context.io.prompt = p = prompt(f, ltype, indent, line_no)
        else
          @context.io.prompt = p = ""
        end
        if @context.auto_indent_mode
          unless ltype
            ind = prompt(@context.prompt_i, ltype, indent, line_no)[/.*\z/].size +
              indent * 2 - p.size
            ind += 2 if continue
            @context.io.prompt = p + " " * ind if ind > 0
          end
        end
      end

      @scanner.set_input(@context.io) do
        signal_status(:IN_INPUT) do
          if l = @context.io.gets
            print l if @context.verbose?
          else
            if @context.ignore_eof? and @context.io.readable_after_eof?
              l = "\n"
              if @context.verbose?
                printf "Use \"exit\" to leave %s\n", @context.ap_name
              end
            else
              print "\n"
            end
          end
          l
        end
      end

      @scanner.each_top_level_statement do |line, line_no|
        signal_status(:IN_EVAL) do
          begin
            line.untaint
            @context.evaluate(line, line_no)
            output_value if @context.echo?
            exc = nil
          rescue Interrupt => exc
          rescue SystemExit, SignalException
            raise
          rescue NameError => exc
            raise exc unless @interactive
            # HBASE-26880: Ignore NameError to prevent exiting Shell on mistyped commands.
          rescue Exception => exc
            # HBASE-26741: Raise exception so Shell::exception_handler can catch it.
            # This modifies this copied method from JRuby so that the HBase shell can
            # manage the exception and set a proper exit code on the process.
            raise exc
          end
          if exc
            if exc.backtrace && exc.backtrace[0] =~ /irb(2)?(\/.*|-.*|\.rb)?:/ && exc.class.to_s !~ /^IRB/ &&
              !(SyntaxError === exc)
              irb_bug = true
            else
              irb_bug = false
            end

            messages = []
            lasts = []
            levels = 0
            if exc.backtrace
              count = 0
              exc.backtrace.each do |m|
                m = @context.workspace.filter_backtrace(m) or next unless irb_bug
                m = sprintf("%9d: from %s", (count += 1), m)
                if messages.size < @context.back_trace_limit
                  messages.push(m)
                elsif lasts.size < @context.back_trace_limit
                  lasts.push(m).shift
                  levels += 1
                end
              end
            end
            attr = STDOUT.tty? ? ATTR_TTY : ATTR_PLAIN
            print "#{attr[1]}Traceback#{attr[]} (most recent call last):\n"
            unless lasts.empty?
              puts lasts.reverse
              printf "... %d levels...\n", levels if levels > 0
            end
            puts messages.reverse
            messages = exc.to_s.split(/\n/)
            print "#{attr[1]}#{exc.class} (#{attr[4]}#{messages.shift}#{attr[0, 1]})#{attr[]}\n"
            puts messages.map {|s| "#{attr[1]}#{s}#{attr[]}\n"}
            print "Maybe IRB bug!\n" if irb_bug
          end
        end
      end
    end
  end

  ##
  # HBaseLoader serves a similar purpose to IRB::IrbLoader, but with a different separation of
  # concerns. This loader allows us to directly get the path for a filename in ruby's load path,
  # and then use that in IRB::Irb
  module HBaseLoader
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
    # @return [FileInputMethod] InputMethod for passing to IRB session
    def self.file_for_load(filename)
      FileInputMethod.new(File.new(path_for_load(filename)))
    end
  end
end
