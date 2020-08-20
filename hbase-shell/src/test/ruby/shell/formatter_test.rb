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

require 'shell/formatter'

class ShellFormatterTest < Test::Unit::TestCase
  # Helper method to construct a null formatter
  def formatter
    Shell::Formatter::Base.new(:output_stream => STDOUT)
  end

  #
  # Constructor tests
  #
  define_test "Formatter constructor should not raise error valid IO streams" do
    assert_nothing_raised do
      Shell::Formatter::Base.new(:output_stream => STDOUT)
    end
  end

  define_test "Formatter constructor should not raise error when no IO stream passed" do
    assert_nothing_raised do
      Shell::Formatter::Base.new()
    end
  end

  define_test "Formatter constructor should raise error on non-IO streams" do
    assert_raise TypeError do
      Shell::Formatter::Base.new(:output_stream => 'foostring')
    end
  end

  #-------------------------------------------------------------------------------------------------------
  # Printing methods tests
  # FIXME: The tests are just checking that the code has no typos, try to figure out a better way to test
  #
  define_test "Formatter#header should work" do
    formatter.header(['a', 'b'])
    formatter.header(['a', 'b'], [10, 20])
  end

  define_test "Formatter#row should work" do
    formatter.row(['a', 'b'])
    formatter.row(['xxxxxxxxx xxxxxxxxxxx xxxxxxxxxxx xxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxx xxxxxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxxxx'])
    formatter.row(['yyyyyy yyyyyy yyyyy yyy', 'xxxxxxxxx xxxxxxxxxxx xxxxxxxxxxx xxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxx xxxxxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxxxx  xxx xx x xx xxx xx xx xx x xx x x xxx x x xxx x x xx x x x x x x xx '])
    formatter.row(["NAME => 'table1', FAMILIES => [{NAME => 'fam2', VERSIONS => 3, COMPRESSION => 'NONE', IN_MEMORY => false, BLOCKCACHE => false, LENGTH => 2147483647, TTL => FOREVER, BLOOMFILTER => NONE}, {NAME => 'fam1', VERSIONS => 3, COMPRESSION => 'NONE', IN_MEMORY => false, BLOCKCACHE => false, LENGTH => 2147483647, TTL => FOREVER, BLOOMFILTER => NONE}]"])
  end

  define_test "Froematter#footer should work" do
    formatter.footer()
  end
end

class ShellTableFormatterTest < Test::Unit::TestCase
  include Hbase::TestHelpers

  TableFormatter = ::Shell::Formatter::TableFormatter
  AlignedTableFormatter = ::Shell::Formatter::AlignedTableFormatter
  UnalignedTableFormatter = ::Shell::Formatter::UnalignedTableFormatter
  JsonTableFormatter = ::Shell::Formatter::JsonTableFormatter

  define_test 'TableFormatter::OptionsHash should allow dot-notation access' do
    my_hash = TableFormatter::OptionsHash.new
    my_hash[:foo] = 'bar'
    assert_equal('bar', my_hash.foo)
  end

  define_test 'TableFormatter::TableScope should correctly determine child scope' do
    table_scope = TableFormatter::TableScope.new
    assert_equal(TableFormatter::TableScope::TABLE, table_scope.kind)

    row_scope = TableFormatter::TableScope.new table_scope
    assert_equal(TableFormatter::TableScope::ROW, row_scope.kind)

    cell_scope = TableFormatter::TableScope.new row_scope
    assert_equal(TableFormatter::TableScope::CELL, cell_scope.kind)

    assert_raise(ArgumentError) { TableFormatter::TableScope.new cell_scope }
  end

  define_test 'AlignedTableFormatter#single_value_table produces correct output' do
    expected_output = <<~EOF
      +-----+
      | FOO |
      +-----+
      | BAR |
      +-----+
    EOF
    io = StringIO.new
    f = AlignedTableFormatter.new
    f.single_value_table('FOO', 'BAR', :output_stream => io)
    assert_equal(expected_output, io.string)
  end

  define_test 'UnalignedTableFormatter#single_value_table produces correct output' do
    io = StringIO.new
    f = UnalignedTableFormatter.new
    f.single_value_table('FOO', 'BAR', :output_stream => io)
    assert_equal("FOO\nBAR\n", io.string)
  end

  define_test 'JsonTableFormatter#single_value_table produces correct output' do
    io = StringIO.new
    f = JsonTableFormatter.new
    f.single_value_table('FOO', 'BAR', :output_stream => io)
    output = io.string
    parsed = JSON.load(output)
    assert_equal('BAR', parsed['rows'][0]['FOO'])
  end

  define_test 'AlignedTableFormatter#single_value_table shows footer fields' do
    io = StringIO.new
    f = AlignedTableFormatter.new
    f.start_table({ :output_stream => io, :headers => %w[FOO] })
    f.row(['BAR'])
    f.close_table({ 'CUSTOM FOOTER': 'VALUE'})
    output = io.string
    assert_match(/CUSTOM FOOTER: VALUE/, output)
  end

  define_test 'UnalignedTableFormatter#single_value_table shows footer fields' do
    io = StringIO.new
    f = AlignedTableFormatter.new
    f.start_table({ :output_stream => io, :headers => %w[FOO] })
    f.row(['BAR'])
    f.close_table({ 'CUSTOM FOOTER': 'VALUE'})
    output = io.string
    assert_match(/CUSTOM FOOTER: VALUE/, output)
  end
end
