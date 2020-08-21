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

module Shell
  module Formatter
    ##
    # Stateful output formatter for data with a fixed number of output columns
    #
    # Goals
    # - Do not require more than one cell loaded in memory at a time
    # - Support many implementations like ascii tables, csv, and html
    #
    # Non-goals
    # - Don't load all the headers into memory at once.
    #   - This may seem like a goal with merit, but we are unlikely to find a use case for this
    #     formatter with many columns. For example: since HBase tables aren't relational, our scan
    #     output will not have an output column for every HBase column. Instead, each output row
    #     will correspond to an HBase cell.
    #
    # Advanced Usage Pattern
    # 1. call start_table to reset the formatter's state and pass configuration options
    # 2. call start_row to start writing a row
    # 3. call cell to write a single cell
    # 4. call close_row
    # 5. call close_table
    #
    # Sometimes, it will feel like this is a lot of method calls, but these calls act as "hooks"
    # and give each of the formatter implementations a chance to fill out all the content necessary
    # between cells. There are shortcut methods like row and single_value_table to reduce
    # boilerplate.
    class TableFormatter
      ##
      # Special subclass of Hash that allows entries to be accessed via dot notation similarly to
      # OpenStruct
      class OptionsHash < Hash
        def method_missing(name, *args, &blk)
          return self[name.to_sym] if keys.map(&:to_sym).include? name.to_sym

          super
        end

        def self.from_hash(hash)
          new.merge(hash)
        end
      end

      ##
      # Class for managing the state of the formatter internally
      class TableScope
        # TABLE indicates that we are writing tables
        TABLE = :TABLE
        # ROW indicates that we are in a table and writing rows
        ROW = :ROW
        # CELL indicates that we are in a row and writing cells
        CELL = :CELL

        attr_accessor :parent
        attr_accessor :kind
        attr_accessor :written

        def get_child_kind(kind)
          case kind
          when nil then :TABLE
          when TABLE then :ROW
          when ROW then :CELL
          when CELL then raise ArgumentError, 'Cannot create child of cell'
          else raise ArgumentError, 'Cannot create child of given kind'
          end
        end

        def initialize(parent = nil, written = 0)
          self.parent = parent
          self.written = written
          self.kind = get_child_kind(parent && parent.kind)
        end
      end

      # DEFAULT_GLOBAL_OPTIONS should contain global options that are NOT reset when a new table is
      # started
      DEFAULT_GLOBAL_OPTIONS = {
        output_stream: Kernel
      }.freeze
      # DEFAULT_TABLE_OPTIONS should contain options that MUST be reset when a new table is started
      DEFAULT_TABLE_OPTIONS = {
        num_cols: 0,
        headers: []
      }.freeze
      @current_scope = nil

      def initialize(**kwargs)
        @options ||= OptionsHash.from_hash self.class.const_get(:DEFAULT_GLOBAL_OPTIONS)
        reset kwargs
      end

      ##
      # Reset the table scope and table-specific options
      def reset(opts = {})
        @current_scope = TableScope.new
        @options = @options.merge self.class.const_get(:DEFAULT_TABLE_OPTIONS)
        configure(opts)
      end

      def configure(opts = {})
        unless @current_scope.kind == TableScope::TABLE
          raise 'Cannot change options while writing a table'
        end

        @options = @options.merge(opts)

        # rectify provided information
        given_cols = @options[:num_cols]
        width_cols = @options.fetch(:widths, []).length
        header_cols = @options.fetch(:headers, []).length

        @options[:num_cols] = [given_cols, width_cols, header_cols].max

        @out = @options[:output_stream]

        @options
      end

      private def expect_scope(expected_scope)
        unless @current_scope.kind == expected_scope
          raise "Expected #{expected_scope}, but current scope is of kind #{@current_scope.kind}"
        end
      end

      ##
      # Update the current scope to indicate that we want to start writing child entities of the
      # current scope.
      #
      # For example, if the current entity is a row, we switch to writing cells.
      #
      # @param [Symbol] :TABLE, :ROW, or :CELL
      # @return [TableScope]
      def start_child(expected_scope = nil)
        expect_scope expected_scope unless expected_scope.nil?
        @current_scope = TableScope.new @current_scope
      end

      ##
      # Pop the current table scope and increment the number of entities written
      #
      # For example, if the current table scope indicates that our formatter
      # is writing cells, this will now reflect that we are writing rows, and
      # will also increment the number of rows written by one.
      #
      # @param [TableScope] expected_scope
      def close_child(expected_scope = nil)
        expect_scope expected_scope unless expected_scope.nil?
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end

      def start_table(opts = {})
        reset(opts)
        start_child TableScope::TABLE
      end

      def start_row
        start_child TableScope::ROW
      end

      def cell(content)
        expect_scope TableScope::CELL
        @current_scope.written += 1
      end

      def close_row
        expect_scope TableScope::CELL
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end

      FRIENDLY_FOOTER_NAMES = {
        "NUM_ROWS": 'Number of row(s)'
      }.freeze

      ##
      # Close the table we are currently writing
      #
      # footer_fields is less structured than the column-row output, so implementation will vary
      # between formatters (especially JSON). The purpose of this is to show special information
      # such as the number of rows retrieved or the amount of time to perform the operation.
      #
      # @param [Hash] footer_fields any additional key-value fields to show the user
      def close_table(footer_fields = {})
        expect_scope TableScope::ROW
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end

      ##
      # High-level shortcut method to write all the cells in a row
      #
      # This method should not be specific to a particular implementation (like JSON or Aligned)
      # such that it does not need to be overridden.
      #
      # @param [Array] contents array of strings
      def row(contents)
        self.start_row
        contents.each { |content| self.cell(content) }
        self.close_row

        nil
      end

      ##
      # High-level shortcut method to write a full table with one column and one row
      #
      # This method should not be specific to a particular implementation (like JSON or Aligned)
      # such that it does not need to be overridden.
      #
      # This method will automatically shrink the table if possible since it
      # is not expensive to calculate the maximum width of only two strings!
      #
      # @param [String] title
      # @param [String] value
      def single_value_table(title, value, **kwargs)
        w = [title.length, value.length].max
        self.start_table({ num_cols: 1, headers: [title], widths: [w] }.merge(kwargs))
        self.start_row
        self.cell(value)
        self.close_row
        self.close_table

        nil
      end

      ##
      # Helper method to report the number of rows already written in the currently open table
      #
      # @return [Integer] number of rows written
      def get_rows_printed
        case @current_scope.kind
        when TableScope::CELL then @current_scope.parent.written
        when TableScope::ROW then @current_scope.written
        when TableScope::TABLE then 0
        end
      end
    end

    class AlignedTableFormatter < TableFormatter
      BORDER_MODE = OptionsHash.from_hash({
                                            NONE: 'NONE',
                                            FULL: 'FULL'
                                          })
      OVERFLOW_MODE = OptionsHash.from_hash({
                                              TRUNCATE: 'TRUNCATE',
                                              OVERFLOW: 'OVERFLOW'
                                            })
      DEFAULT_GLOBAL_OPTIONS = DEFAULT_GLOBAL_OPTIONS.merge({
                                                              padding: 1,
                                                              border: BORDER_MODE.NONE,
                                                              overflow: OVERFLOW_MODE.OVERFLOW
                                                            }).freeze
      DEFAULT_TABLE_OPTIONS = DEFAULT_TABLE_OPTIONS.merge({
                                                            widths: []
                                                          }).freeze

      # If we are not running in a TTY and cannot get the width of the terminal,
      # NONTTY_TABLE_WIDTH will be used as the full width of the table.
      NONTTY_TABLE_WIDTH = 100

      def initialize(*args, **kwargs, &block)
        super
      end

      def configure(*args, **kwargs, &block)
        super
        # derive extra properties for fast access
        @options[:vertical_borders] = @options[:border] == BORDER_MODE.FULL
        @options[:horizontal_borders] = @options[:border] == BORDER_MODE.FULL
        @options[:truncate] = @options[:overflow] == OVERFLOW_MODE.TRUNCATE
        refresh_column_widths
      end

      BORDER_CORNER = '+'.freeze
      BORDER_HORIZONTAL = '-'.freeze
      BORDER_VERTICAL = '|'.freeze

      ##
      # Calculate the actual width of each output column. If :widths is not set in options, or if
      # some widths are nil, this method will allocate the full width of the terminal to all the
      # columns.
      #
      # @return [Array] column widths matching the length of :headers
      def refresh_column_widths
        max_width = NONTTY_TABLE_WIDTH
        if $stdout.tty?
          begin
            max_width = Java.jline.TerminalFactory.get.getWidth
          rescue NameError => e
            # nocommit debug log and ignore
          end
        end

        reserved_for_border = @options.num_cols + 1
        reserved_for_padding = @options.num_cols * @options.padding * 2
        reserved_width = @options.widths.map { |x| x.nil? ? 0 : x }.reduce { |a, b| a + b } || 0
        reserved_width += reserved_for_border + reserved_for_padding
        free_width = max_width - reserved_width

        num_reserved_columns = @options.widths.map { |x| x.nil? ? 0 : 1 }.reduce { |a, b| a + b } || 0
        num_free_columns = @options.num_cols - num_reserved_columns

        if num_free_columns == 0
          # if there are no free columns, than this value does not really matter
          width_per_column = 0
        else
          # allocate the free width to the flexible columns
          width_per_column = free_width / num_free_columns
        end

        @widths = (0..@options.num_cols - 1).map { |i| @options.widths.fetch(i, width_per_column) || width_per_column }
      end

      private def hr
        between_cells = @options.vertical_borders ? BORDER_CORNER : ''
        between_cells + @widths.map { |n| BORDER_HORIZONTAL * (n + 2 * @options.padding) }.join(between_cells) + between_cells
      end

      private def print_hr
        return unless @options.horizontal_borders

        @out.print hr
        @out.print "\n"
      end

      private def print_cell(content)
        width = @widths.fetch @current_scope.written
        @out.print BORDER_VERTICAL if @options.vertical_borders
        @out.print ' ' * @options.padding
        @out.print ::Shell::Formatter::Util.set_text_width(content, width, truncate: @options.truncate)
        @out.print ' ' * @options.padding
      end

      def start_table(opts = {})
        super
        print_hr
        start_row
        @options.headers.map { |h| cell(h) }
        close_row
        @current_scope.written = 0
      end

      def start_row
        start_child TableScope::ROW
      end

      def cell(content)
        expect_scope TableScope::CELL
        print_cell content
        @current_scope.written += 1
      end

      def close_row
        expect_scope TableScope::CELL
        # print all the remaining cells
        cell('') while @current_scope.written < @options.num_cols
        @out.print BORDER_VERTICAL if @options.vertical_borders
        @out.print "\n"
        print_hr
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end

      def close_table(footer_fields = {})
        expect_scope TableScope::ROW
        footer_fields.each do |k, v|
          if k == :DURATION
            @out.puts format('Took %.4f seconds', v)
            next
          end
          name = FRIENDLY_FOOTER_NAMES.fetch(k, k)
          @out.puts "#{name}: #{v}"
        end
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end
    end

    class UnalignedTableFormatter < TableFormatter
      DEFAULT_GLOBAL_OPTIONS = DEFAULT_GLOBAL_OPTIONS.merge({
                                                              padding: 1,
                                                              row_separator: "\n",
                                                              cell_separator: '|'
                                                            }).freeze
      DEFAULT_TABLE_OPTIONS = DEFAULT_TABLE_OPTIONS.merge({
                                                            widths: []
                                                          }).freeze

      def initialize(*args, **kwargs, &block)
        super
      end

      private def print_cell(content)
        @out.print @options.cell_separator unless @current_scope.written == 0
        @out.print ' ' * @options.padding + content + ' ' * @options.padding
      end

      def start_table(opts = {})
        super
        start_row
        @options.headers.map { |h| cell(h) }
        close_row
        @current_scope.written = 0
      end

      def cell(content)
        expect_scope TableScope::CELL
        print_cell content
        @current_scope.written += 1
      end

      def close_row
        expect_scope TableScope::CELL
        # print all the remaining cells
        cell('') while @current_scope.written < @options.num_cols
        @out.print @options.row_separator
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end

      def close_table(footer_fields = {})
        expect_scope TableScope::ROW
        footer_fields.each do |k, v|
          if k == 'DURATION'
            @out.puts format('Took %.4f seconds', v)
            next
          end
          name = FRIENDLY_FOOTER_NAMES.fetch(k, k)
          @out.puts "#{name}: #{v}"
        end
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end
    end

    class JsonTableFormatter < TableFormatter
      DEFAULT_GLOBAL_OPTIONS = DEFAULT_GLOBAL_OPTIONS.merge({
                                                              use_objects: true
                                                            }).freeze

      def initialize(*args, **kwargs, &block)
        super
      end

      def start_table(opts = {})
        super
        @out.print('{"rows": [')
      end

      def start_row
        @out.print ',' unless @current_scope.written == 0
        @out.print '[' unless @options.use_objects
        @out.print '{' if @options.use_objects
        start_child TableScope::ROW
      end

      def cell(content)
        expect_scope TableScope::CELL
        @out.print ',' unless @current_scope.written == 0
        # key and content may be printable strings, in which case the call to_json
        # will just add surrounding quotes. In the case that content includes
        # non-printable characters, to_json will save us by escaping these
        # characters and ensuring that the output is valid JSON.
        if @options.use_objects
          key = @options.headers.fetch @current_scope.written, ''
          @out.print key.to_json + ':'
        end
        @out.print content.to_json
        @current_scope.written += 1
      end

      def close_row
        expect_scope TableScope::CELL

        cell('') while @current_scope.written < @options.num_cols
        @current_scope = @current_scope.parent
        @out.print ']' unless @options.use_objects
        @out.print '}' if @options.use_objects
        @current_scope.written += 1
      end

      def close_table(footer_fields = {})
        expect_scope TableScope::ROW

        @out.print "], \"row_count\": #{@current_scope.written}}"
        @current_scope = @current_scope.parent
        @current_scope.written += 1
      end
    end
  end
end
