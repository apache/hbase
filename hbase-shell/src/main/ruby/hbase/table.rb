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

include Java

java_import org.apache.hadoop.hbase.util.Bytes
java_import org.apache.hadoop.hbase.client.RegionReplicaUtil

# Wrapper for org.apache.hadoop.hbase.client.Table

module Hbase
  # rubocop:disable Metrics/ClassLength
  class Table
    include HBaseConstants
    @@thread_pool = nil

    # Add the command 'name' to table s.t. the shell command also called via 'name'
    # and has an internal method also called 'name'.
    #
    # e.g. name = scan, adds table.scan which calls Scan.scan
    def self.add_shell_command(name)
      add_command(name, name, name)
    end

    # add a named command to the table instance
    #
    # name - name of the command that should added to the table
    #    (eg. sending 'scan' here would allow you to do table.scan)
    # shell_command - name of the command in the shell
    # internal_method_name - name of the method in the shell command to forward the call
    def self.add_command(name, shell_command, internal_method_name)
      method = name.to_sym
      class_eval do
        define_method method do |*args|
          @shell.internal_command(shell_command, internal_method_name, self, *args)
        end
      end
    end

    # General help for the table
    # class level so we can call it from anywhere
    def self.help
      <<-EOF
Help for table-reference commands.

You can either create a table via 'create' and then manipulate the table via commands like 'put', 'get', etc.
See the standard help information for how to use each of these commands.

However, as of 0.96, you can also get a reference to a table, on which you can invoke commands.
For instance, you can get create a table and keep around a reference to it via:

   hbase> t = create 't', 'cf'

Or, if you have already created the table, you can get a reference to it:

   hbase> t = get_table 't'

You can do things like call 'put' on the table:

  hbase> t.put 'r', 'cf:q', 'v'

which puts a row 'r' with column family 'cf', qualifier 'q' and value 'v' into table t.

To read the data out, you can scan the table:

  hbase> t.scan

which will read all the rows in table 't'.

Essentially, any command that takes a table name can also be done via table reference.
Other commands include things like: get, delete, deleteall,
get_all_columns, get_counter, count, incr. These functions, along with
the standard JRuby object methods are also available via tab completion.

For more information on how to use each of these commands, you can also just type:

   hbase> t.help 'scan'

which will output more information on how to use that command.

You can also do general admin actions directly on a table; things like enable, disable,
flush and drop just by typing:

   hbase> t.enable
   hbase> t.flush
   hbase> t.disable
   hbase> t.drop

Note that after dropping a table, your reference to it becomes useless and further usage
is undefined (and not recommended).
EOF
    end

    #---------------------------------------------------------------------------------------------

    # let external objects read the underlying table object
    attr_reader :table
    # let external objects read the table name
    attr_reader :name

    def initialize(table, shell)
      @table = table
      @name = @table.getName.getNameAsString
      @shell = shell
      @converters = {}
      @timestamp_format_epoch = table.getConfiguration.getBoolean(
          HConstants::SHELL_TIMESTAMP_FORMAT_EPOCH_KEY,
          HConstants::DEFAULT_SHELL_TIMESTAMP_FORMAT_EPOCH)
    end

    def close
      @table.close
    end

    # Note the below methods are prefixed with '_' to hide them from the average user, as
    # they will be much less likely to tab complete to the 'dangerous' internal method
    #----------------------------------------------------------------------------------------------

    # Put a cell 'value' at specified table/row/column
    def _put_internal(row, column, value, timestamp = nil, args = {})
      p = org.apache.hadoop.hbase.client.Put.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if args.any?
        attributes = args[ATTRIBUTES]
        set_attributes(p, attributes) if attributes
        visibility = args[VISIBILITY]
        set_cell_visibility(p, visibility) if visibility
        ttl = args[TTL]
        set_op_ttl(p, ttl) if ttl
      end
      # Case where attributes are specified without timestamp
      if timestamp.is_a?(Hash)
        timestamp.each do |k, v|
          if k == 'ATTRIBUTES'
            set_attributes(p, v)
          elsif k == 'VISIBILITY'
            set_cell_visibility(p, v)
          elsif k == 'TTL'
            set_op_ttl(p, v)
          end
        end
        timestamp = nil
      end
      if timestamp
        p.addColumn(family, qualifier, timestamp, value.to_s.to_java_bytes)
      else
        p.addColumn(family, qualifier, value.to_s.to_java_bytes)
      end
      @table.put(p)
    end

    #----------------------------------------------------------------------------------------------
    # Create a Delete mutation
    def _createdelete_internal(row, column = nil,
                               timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP,
                               args = {}, all_version = true)
      temptimestamp = timestamp
      if temptimestamp.is_a?(Hash)
        timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP
      end
      d = org.apache.hadoop.hbase.client.Delete.new(row.to_s.to_java_bytes, timestamp)
      if temptimestamp.is_a?(Hash)
        temptimestamp.each do |_k, v|
          if v.is_a?(String)
            set_cell_visibility(d, v) if v
          end
        end
      end
      if args.any?
        visibility = args[VISIBILITY]
        set_cell_visibility(d, visibility) if visibility
      end
      if column != ""
        if column && all_version
          family, qualifier = parse_column_name(column)
          if qualifier
            d.addColumns(family, qualifier, timestamp)
          else
            d.addFamily(family, timestamp)
          end
        elsif column && !all_version
          family, qualifier = parse_column_name(column)
          if qualifier
            d.addColumn(family, qualifier, timestamp)
          else
            d.addFamilyVersion(family, timestamp)
          end
        end
      end
      d
    end

    #----------------------------------------------------------------------------------------------
    # Delete rows using prefix
    def _deleterows_internal(row, column = nil,
                             timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP,
                             args = {}, all_version = true)
      cache = row['CACHE'] ? row['CACHE'] : 100
      prefix = row['ROWPREFIXFILTER']

      # create scan to get table names using prefix
      scan = org.apache.hadoop.hbase.client.Scan.new
      scan.setRowPrefixFilter(prefix.to_java_bytes)
      # Run the scanner to get all rowkeys
      scanner = @table.getScanner(scan)
      # Create a list to store all deletes
      list = java.util.ArrayList.new
      # Iterate results
      iter = scanner.iterator
      while iter.hasNext
        row = iter.next
        key = org.apache.hadoop.hbase.util.Bytes.toStringBinary(row.getRow)
        d = _createdelete_internal(key, column, timestamp, args, all_version)
        list.add(d)
        if list.size >= cache
          @table.delete(list)
          list.clear
        end
      end
      @table.delete(list)
    end

    #----------------------------------------------------------------------------------------------
    # Delete a cell
    def _delete_internal(row, column,
                         timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP,
                         args = {}, all_version = false)
      _deleteall_internal(row, column, timestamp, args, all_version)
    end

    #----------------------------------------------------------------------------------------------
    # Delete a row
    def _deleteall_internal(row, column = nil,
                            timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP,
                            args = {}, all_version = true)
      # delete operation doesn't need read permission. Retaining the read check for
      # meta table as a part of HBASE-5837.
      if is_meta_table?
        if row.is_a?(Hash) and row.key?('ROWPREFIXFILTER')
          raise ArgumentError, 'deleteall with ROWPREFIXFILTER in hbase:meta is not allowed.'
        else
          raise ArgumentError, 'Row Not Found' if _get_internal(row).nil?
        end
      end
      if row.is_a?(Hash)
        _deleterows_internal(row, column, timestamp, args, all_version)
      else
        d = _createdelete_internal(row, column, timestamp, args, all_version)
        @table.delete(d)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Increment a counter atomically
    # rubocop:disable Metrics/AbcSize, CyclomaticComplexity, MethodLength
    def _incr_internal(row, column, value = nil, args = {})
      value = 1 if value.is_a?(Hash)
      value ||= 1
      incr = org.apache.hadoop.hbase.client.Increment.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if args.any?
        attributes = args[ATTRIBUTES]
        visibility = args[VISIBILITY]
        set_attributes(incr, attributes) if attributes
        set_cell_visibility(incr, visibility) if visibility
        ttl = args[TTL]
        set_op_ttl(incr, ttl) if ttl
      end
      incr.addColumn(family, qualifier, value)
      result = @table.increment(incr)
      return nil if result.isEmpty

      # Fetch cell value
      cell = result.listCells[0]
      org.apache.hadoop.hbase.util.Bytes.toLong(cell.getValueArray,
                                                cell.getValueOffset, cell.getValueLength)
    end

    #----------------------------------------------------------------------------------------------
    # appends the value atomically
    def _append_internal(row, column, value, args = {})
      append = org.apache.hadoop.hbase.client.Append.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if args.any?
        attributes = args[ATTRIBUTES]
        visibility = args[VISIBILITY]
        set_attributes(append, attributes) if attributes
        set_cell_visibility(append, visibility) if visibility
        ttl = args[TTL]
        set_op_ttl(append, ttl) if ttl
      end
      append.add(family, qualifier, value.to_s.to_java_bytes)
      result = @table.append(append)
      return nil if result.isEmpty

      # Fetch cell value
      cell = result.listCells[0]
      org.apache.hadoop.hbase.util.Bytes.toStringBinary(cell.getValueArray,
                                                        cell.getValueOffset, cell.getValueLength)
    end
    # rubocop:enable Metrics/AbcSize, CyclomaticComplexity, MethodLength

    #----------------------------------------------------------------------------------------------
    # Count rows in a table
    def _count_internal(interval = 1000, scan = nil, cacheBlocks=false)
      raise(ArgumentError, 'Scan argument should be org.apache.hadoop.hbase.client.Scan') \
        unless scan.nil? || scan.is_a?(org.apache.hadoop.hbase.client.Scan)
      # We can safely set scanner caching with the first key only filter

      if scan.nil?
        scan = org.apache.hadoop.hbase.client.Scan.new
        scan.setCacheBlocks(cacheBlocks)
        scan.setCaching(10)
        scan.setFilter(org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter.new)
      else
        scan.setCacheBlocks(cacheBlocks)
        filter = scan.getFilter
        firstKeyOnlyFilter = org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter.new
        if filter.nil?
          scan.setFilter(firstKeyOnlyFilter)
        else
          firstKeyOnlyFilter.setReversed(filter.isReversed)
          scan.setFilter(org.apache.hadoop.hbase.filter.FilterList.new(filter, firstKeyOnlyFilter))
        end
      end

      # Run the scanner
      scanner = @table.getScanner(scan)
      count = 0
      iter = scanner.iterator

      # Iterate results
      while iter.hasNext
        row = iter.next
        count += 1
        next unless block_given? && count % interval == 0
        # Allow command modules to visualize counting process
        yield(count,
              org.apache.hadoop.hbase.util.Bytes.toStringBinary(row.getRow))
      end

      scanner.close
      # Return the counter
      count
    end

    #----------------------------------------------------------------------------------------------
    # Get from table
    def _get_internal(row, *args)
      get = org.apache.hadoop.hbase.client.Get.new(row.to_s.to_java_bytes)
      maxlength = -1
      count = 0
      @converters.clear

      # Normalize args
      args = args.first if args.first.is_a?(Hash)
      if args.is_a?(String) || args.is_a?(Array)
        columns = [args].flatten.compact
        args = { COLUMNS => columns }
      end

      #
      # Parse arguments
      #
      unless args.is_a?(Hash)
        raise ArgumentError, "Failed parse of #{args.inspect}, #{args.class}"
      end

      # Get maxlength parameter if passed
      maxlength = args.delete(MAXLENGTH) if args[MAXLENGTH]
      filter = args.delete(FILTER) if args[FILTER]
      attributes = args[ATTRIBUTES]
      authorizations = args[AUTHORIZATIONS]
      consistency = args.delete(CONSISTENCY) if args[CONSISTENCY]
      replicaId = args.delete(REGION_REPLICA_ID) if args[REGION_REPLICA_ID]
      converter = args.delete(FORMATTER) || nil
      converter_class = args.delete(FORMATTER_CLASS) || 'org.apache.hadoop.hbase.util.Bytes'
      unless args.empty?
        columns = args[COLUMN] || args[COLUMNS]
        vers = if args[VERSIONS]
                 args[VERSIONS]
               else
                 1
               end
        if columns
          # Normalize types, convert string to an array of strings
          columns = [columns] if columns.is_a?(String)

          # At this point it is either an array or some unsupported stuff
          unless columns.is_a?(Array)
            raise ArgumentError, "Failed parse column argument type #{args.inspect}, #{args.class}"
          end

          # Get each column name and add it to the filter
          columns.each do |column|
            family, qualifier = parse_column_name(column.to_s)
            if qualifier
              get.addColumn(family, qualifier)
            else
              get.addFamily(family)
            end
          end

          # Additional params
          get.setMaxVersions(vers)
          get.setTimeStamp(args[TIMESTAMP]) if args[TIMESTAMP]
          get.setTimeRange(args[TIMERANGE][0], args[TIMERANGE][1]) if args[TIMERANGE]
        else
          if attributes
            set_attributes(get, attributes)
          elsif authorizations
            set_authorizations(get, authorizations)
          else
            # May have passed TIMESTAMP and row only; wants all columns from ts.
            unless ts = args[TIMESTAMP] || tr = args[TIMERANGE]
              raise ArgumentError, "Failed parse of #{args.inspect}, #{args.class}"
            end
          end

          get.setMaxVersions(vers)
          # Set the timestamp/timerange
          get.setTimeStamp(ts.to_i) if args[TIMESTAMP]
          get.setTimeRange(args[TIMERANGE][0], args[TIMERANGE][1]) if args[TIMERANGE]
        end
        set_attributes(get, attributes) if attributes
        set_authorizations(get, authorizations) if authorizations
      end

      if filter.class == String
        get.setFilter(
          org.apache.hadoop.hbase.filter.ParseFilter.new.parseFilterString(filter.to_java_bytes)
        )
      else
        get.setFilter(filter)
      end

      get.setConsistency(org.apache.hadoop.hbase.client.Consistency.valueOf(consistency)) if consistency
      get.setReplicaId(replicaId) if replicaId

      # Call hbase for the results
      result = @table.get(get)
      return nil if result.isEmpty

      # Get stale info from results
      is_stale = result.isStale
      count += 1

      # Print out results.  Result can be Cell or RowResult.
      res = {}
      result.listCells.each do |c|
        # Get the family and qualifier of the cell without escaping non-printable characters. It is crucial that
        # column is constructed in this consistent way to that it can be used as a key.
        family_bytes =  org.apache.hadoop.hbase.util.Bytes.copy(c.getFamilyArray, c.getFamilyOffset, c.getFamilyLength)
        qualifier_bytes =  org.apache.hadoop.hbase.util.Bytes.copy(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength)
        column = "#{family_bytes}:#{qualifier_bytes}"

        value = to_string(column, c, maxlength, converter_class, converter)

        # Use the FORMATTER to determine how column is printed
        family = convert_bytes(family_bytes, converter_class, converter)
        qualifier = convert_bytes(qualifier_bytes, converter_class, converter)
        formatted_column = "#{family}:#{qualifier}"

        if block_given?
          yield(formatted_column, value)
        else
          res[formatted_column] = value
        end
      end

      # If block given, we've yielded all the results, otherwise just return them
      (block_given? ? [count, is_stale] : res)
    end

    #----------------------------------------------------------------------------------------------
    # Fetches and decodes a counter value from hbase
    def _get_counter_internal(row, column)
      family, qualifier = parse_column_name(column.to_s)
      # Format get request
      get = org.apache.hadoop.hbase.client.Get.new(row.to_s.to_java_bytes)
      get.addColumn(family, qualifier)
      get.setMaxVersions(1)

      # Call hbase
      result = @table.get(get)
      return nil if result.isEmpty

      # Fetch cell value
      cell = result.listCells[0]
      org.apache.hadoop.hbase.util.Bytes.toLong(cell.getValueArray,
                                                cell.getValueOffset, cell.getValueLength)
    end

    def _hash_to_scan(args)
      if args.any?
        enablemetrics = args['ALL_METRICS'].nil? ? false : args['ALL_METRICS']
        enablemetrics ||= !args['METRICS'].nil?
        filter = args['FILTER']
        startrow = args['STARTROW'] || ''
        stoprow = args['STOPROW']
        rowprefixfilter = args['ROWPREFIXFILTER']
        timestamp = args['TIMESTAMP']
        columns = args['COLUMNS'] || args['COLUMN'] || []
        # If CACHE_BLOCKS not set, then default 'true'.
        cache_blocks = args['CACHE_BLOCKS'].nil? ? true : args['CACHE_BLOCKS']
        cache = args['CACHE'] || 0
        reversed = args['REVERSED'] || false
        versions = args['VERSIONS'] || 1
        timerange = args[TIMERANGE]
        raw = args['RAW'] || false
        attributes = args[ATTRIBUTES]
        authorizations = args[AUTHORIZATIONS]
        consistency = args[CONSISTENCY]
        # Normalize column names
        columns = [columns] if columns.class == String
        limit = args['LIMIT'] || -1
        replica_id = args[REGION_REPLICA_ID]
        isolation_level = args[ISOLATION_LEVEL]
        read_type = args[READ_TYPE]
        allow_partial_results = args[ALLOW_PARTIAL_RESULTS].nil? ? false : args[ALLOW_PARTIAL_RESULTS]
        batch = args[BATCH] || -1
        max_result_size = args[MAX_RESULT_SIZE] || -1

        unless columns.is_a?(Array)
          raise ArgumentError, 'COLUMNS must be specified as a String or an Array'
        end

        scan = if stoprow
                 org.apache.hadoop.hbase.client.Scan.new(startrow.to_java_bytes, stoprow.to_java_bytes)
               else
                 org.apache.hadoop.hbase.client.Scan.new(startrow.to_java_bytes)
               end

        # This will overwrite any startrow/stoprow settings
        scan.setRowPrefixFilter(rowprefixfilter.to_java_bytes) if rowprefixfilter

        # Clear converters from last scan.
        @converters.clear

        columns.each do |c|
          family, qualifier = parse_column_name(c.to_s)
          if qualifier
            scan.addColumn(family, qualifier)
          else
            scan.addFamily(family)
          end
        end

        if filter.class == String
          scan.setFilter(
            org.apache.hadoop.hbase.filter.ParseFilter.new.parseFilterString(filter.to_java_bytes)
          )
        else
          scan.setFilter(filter)
        end

        scan.setScanMetricsEnabled(enablemetrics) if enablemetrics
        scan.setTimestamp(timestamp) if timestamp
        scan.setCacheBlocks(cache_blocks)
        scan.setReversed(reversed)
        scan.setCaching(cache) if cache > 0
        scan.setMaxVersions(versions) if versions > 1
        scan.setTimeRange(timerange[0], timerange[1]) if timerange
        scan.setRaw(raw)
        scan.setLimit(limit) if limit > 0
        set_attributes(scan, attributes) if attributes
        set_authorizations(scan, authorizations) if authorizations
        scan.setConsistency(org.apache.hadoop.hbase.client.Consistency.valueOf(consistency)) if consistency
        scan.setReplicaId(replica_id) if replica_id
        scan.setIsolationLevel(org.apache.hadoop.hbase.client.IsolationLevel.valueOf(isolation_level)) if isolation_level
        scan.setReadType(org.apache.hadoop.hbase.client::Scan::ReadType.valueOf(read_type)) if read_type
        scan.setAllowPartialResults(allow_partial_results) if allow_partial_results
        scan.setBatch(batch) if batch > 0
        scan.setMaxResultSize(max_result_size) if max_result_size > 0
      else
        scan = org.apache.hadoop.hbase.client.Scan.new
      end

      scan
    end

    def _get_scanner(args)
      @table.getScanner(_hash_to_scan(args))
    end

    #----------------------------------------------------------------------------------------------
    # Scans whole table or a range of keys and returns rows matching specific criteria
    def _scan_internal(args = {}, scan = nil)
      raise(ArgumentError, 'Args should be a Hash') unless args.is_a?(Hash)
      raise(ArgumentError, 'Scan argument should be org.apache.hadoop.hbase.client.Scan') \
        unless scan.nil? || scan.is_a?(org.apache.hadoop.hbase.client.Scan)

      maxlength = args.delete('MAXLENGTH') || -1
      converter = args.delete(FORMATTER) || nil
      converter_class = args.delete(FORMATTER_CLASS) || 'org.apache.hadoop.hbase.util.Bytes'
      count = 0
      res = {}

      # Start the scanner
      scan = scan.nil? ? _hash_to_scan(args) : scan
      scanner = @table.getScanner(scan)
      iter = scanner.iterator

      # Iterate results
      while iter.hasNext
        row = iter.next
        key = convert_bytes(row.getRow, nil, converter)
        is_stale |= row.isStale

        row.listCells.each do |c|
          # Get the family and qualifier of the cell without escaping non-printable characters. It is crucial that
          # column is constructed in this consistent way to that it can be used as a key.
          family_bytes =  org.apache.hadoop.hbase.util.Bytes.copy(c.getFamilyArray, c.getFamilyOffset, c.getFamilyLength)
          qualifier_bytes =  org.apache.hadoop.hbase.util.Bytes.copy(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength)
          column = "#{family_bytes}:#{qualifier_bytes}"

          cell = to_string(column, c, maxlength, converter_class, converter)

          # Use the FORMATTER to determine how column is printed
          family = convert_bytes(family_bytes, converter_class, converter)
          qualifier = convert_bytes(qualifier_bytes, converter_class, converter)
          formatted_column = "#{family}:#{qualifier}"

          if block_given?
            yield(key, "column=#{formatted_column}, #{cell}")
          else
            res[key] ||= {}
            res[key][formatted_column] = cell
          end
        end
        # One more row processed
        count += 1
      end

      scanner.close
      (block_given? ? [count, is_stale] : res)
    end

    # Apply OperationAttributes to puts/scans/gets
    def set_attributes(oprattr, attributes)
      raise(ArgumentError, 'Attributes must be a Hash type') unless attributes.is_a?(Hash)
      for k, v in attributes
        v = v.to_s unless v.nil?
        oprattr.setAttribute(k.to_s, v.to_java_bytes)
      end
    end

    def set_cell_permissions(op, permissions)
      raise(ArgumentError, 'Permissions must be a Hash type') unless permissions.is_a?(Hash)
      map = java.util.HashMap.new
      permissions.each do |user, perms|
        map.put(user.to_s, org.apache.hadoop.hbase.security.access.Permission.new(
                             perms.to_java_bytes
        ))
      end
      op.setACL(map)
    end

    def set_cell_visibility(oprattr, visibility)
      oprattr.setCellVisibility(
        org.apache.hadoop.hbase.security.visibility.CellVisibility.new(
          visibility.to_s
        )
      )
    end

    def set_authorizations(oprattr, authorizations)
      raise(ArgumentError, 'Authorizations must be a Array type') unless authorizations.is_a?(Array)
      auths = [authorizations].flatten.compact
      oprattr.setAuthorizations(
        org.apache.hadoop.hbase.security.visibility.Authorizations.new(
          auths.to_java(:string)
        )
      )
    end

    def set_op_ttl(op, ttl)
      op.setTTL(ttl.to_java(:long))
    end

    #----------------------------
    # Add general administration utilities to the shell
    # each of the names below adds this method name to the table
    # by callling the corresponding method in the shell
    # Add single method utilities to the current class
    # Generally used for admin functions which just have one name and take the table name
    def self.add_admin_utils(*args)
      args.each do |method|
        define_method method do |*method_args|
          @shell.command(method, @name, *method_args)
        end
      end
    end

    # Add the following admin utilities to the table
    add_admin_utils :enable, :disable, :flush, :drop, :describe, :snapshot

    #----------------------------
    # give the general help for the table
    # or the named command
    def help(command = nil)
      # if there is a command, get the per-command help from the shell
      if command
        begin
          return @shell.help_command(command)
        rescue NoMethodError
          puts "Command \'#{command}\' does not exist. Please see general table help."
          return nil
        end
      end
      @shell.help('table_help')
    end

    # Table to string
    def to_s
      cl = self.class
      "#{cl} - #{@name}"
    end

    # Standard ruby call to get the return value for an object
    # overriden here so we get sane semantics for printing a table on return
    def inspect
      to_s
    end

    #----------------------------------------------------------------------------------------
    # Helper methods

    # Returns a list of column names in the table
    def get_all_columns
      @table.table_descriptor.getFamilies.map do |family|
        "#{family.getNameAsString}:"
      end
    end

    # Checks if current table is one of the 'meta' tables
    def is_meta_table?
      org.apache.hadoop.hbase.TableName::META_TABLE_NAME.equals(@table.getName)
    end

    # Given a column specification in the format FAMILY[:QUALIFIER[:CONVERTER]]
    # 1. Save the converter for the given column
    # 2. Return a 2-element Array with [family, qualifier or nil], discarding the converter if provided
    #
    # @param [String] column specification
    def parse_column_name(column)
      spec = parse_column_format_spec(column)
      set_column_converter(spec.family, spec.qualifier, spec.converter) unless spec.converter.nil?
      [spec.family, spec.qualifier]
    end

    def toLocalDateTime(millis)
      if @timestamp_format_epoch
        return millis
      else
        instant = java.time.Instant.ofEpochMilli(millis)
        return java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.systemDefault()).toString
      end
    end

    # Make a String of the passed kv
    # Intercept cells whose format we know such as the info:regioninfo in hbase:meta
    def to_string(column, kv, maxlength = -1, converter_class = nil, converter = nil)
      if is_meta_table?
        if column == 'info:regioninfo' || column == 'info:splitA' || column == 'info:splitB' || \
            column.start_with?('info:merge')
          hri = org.apache.hadoop.hbase.HRegionInfo.parseFromOrNull(kv.getValueArray,
            kv.getValueOffset, kv.getValueLength)
          return format('timestamp=%s, value=%s', toLocalDateTime(kv.getTimestamp),
            hri.nil? ? '' : hri.toString)
        end
        if column == 'info:serverstartcode'
          if kv.getValueLength > 0
            str_val = org.apache.hadoop.hbase.util.Bytes.toLong(kv.getValueArray,
                                                                kv.getValueOffset, kv.getValueLength)
          else
            str_val = org.apache.hadoop.hbase.util.Bytes.toStringBinary(kv.getValueArray,
                                                                        kv.getValueOffset, kv.getValueLength)
          end
          return format('timestamp=%s, value=%s', toLocalDateTime(kv.getTimestamp), str_val)
        end
      end

      if org.apache.hadoop.hbase.CellUtil.isDelete(kv)
        val = "timestamp=#{toLocalDateTime(kv.getTimestamp)}, type=#{org.apache.hadoop.hbase.KeyValue::Type.codeToType(kv.getTypeByte)}"
      else
        val = "timestamp=#{toLocalDateTime(kv.getTimestamp)}, value=#{convert(column, kv, converter_class, converter)}"
      end
      maxlength != -1 ? val[0, maxlength] : val
    end

    def convert(column, kv, converter_class = 'org.apache.hadoop.hbase.util.Bytes', converter = 'toStringBinary')
      # use org.apache.hadoop.hbase.util.Bytes as the default class
      converter_class = 'org.apache.hadoop.hbase.util.Bytes' unless converter_class
      # use org.apache.hadoop.hbase.util.Bytes::toStringBinary as the default convertor
      converter = 'toStringBinary' unless converter
      if @converters.key?(column)
        # lookup the CONVERTER for certain column - "cf:qualifier"
        matches = /c\((.+)\)\.(.+)/.match(@converters[column])
        if matches.nil?
          # cannot match the pattern of 'c(className).functionname'
          # use the default klazz_name
          converter = @converters[column]
        else
          klazz_name = matches[1]
          converter = matches[2]
        end
      end
      # apply the converter
      convert_bytes(org.apache.hadoop.hbase.CellUtil.cloneValue(kv), klazz_name, converter)
    end

    def convert_bytes(bytes, converter_class = nil, converter_method = nil)
      # Avoid nil
      converter_class ||= 'org.apache.hadoop.hbase.util.Bytes'
      converter_method ||= 'toStringBinary'
      eval(converter_class).method(converter_method).call(bytes)
    end

    def convert_bytes_with_position(bytes, offset, len, converter_class, converter_method)
      # Avoid nil
      converter_class ||= 'org.apache.hadoop.hbase.util.Bytes'
      converter_method ||= 'toStringBinary'
      eval(converter_class).method(converter_method).call(bytes, offset, len)
    end

    # store the information designating what part of a column should be printed, and how
    ColumnFormatSpec = Struct.new(:family, :qualifier, :converter)

    ##
    # Parse the column specification for formatting used by shell commands like :scan
    #
    # Strings should be structured as follows:
    #   FAMILY:QUALIFIER[:CONVERTER]
    # Where:
    #   - FAMILY is the column family
    #   - QUALIFIER is the column qualifier. Non-printable characters should be left AS-IS and should NOT BE escaped.
    #   - CONVERTER is optional and is the name of a converter (like toLong) to apply
    #
    # @param [String] column
    # @return [ColumnFormatSpec] family, qualifier, and converter as Java bytes
    private def parse_column_format_spec(column)
      split = org.apache.hadoop.hbase.CellUtil.parseColumn(column.to_java_bytes)
      family = split[0]
      qualifier = nil
      converter = nil
      if split.length > 1
        parts = org.apache.hadoop.hbase.CellUtil.parseColumn(split[1])
        qualifier = parts[0]
        if parts.length > 1
          converter = parts[1]
        end
      end

      ColumnFormatSpec.new(family, qualifier, converter)
    end

    private def set_column_converter(family, qualifier, converter)
      @converters["#{String.from_java_bytes(family)}:#{String.from_java_bytes(qualifier)}"] = String.from_java_bytes(converter)
    end

    # if the column spec contains CONVERTER information, to get rid of :CONVERTER info from column pair.
    # 1. return back normal column pair as usual, i.e., "cf:qualifier[:CONVERTER]" to "cf" and "qualifier" only
    # 2. register the CONVERTER information based on column spec - "cf:qualifier"
    #
    # Deprecated for removal in 4.0.0
    def set_converter(column)
      family = String.from_java_bytes(column[0])
      parts = org.apache.hadoop.hbase.CellUtil.parseColumn(column[1])
      if parts.length > 1
        @converters["#{family}:#{String.from_java_bytes(parts[0])}"] = String.from_java_bytes(parts[1])
        column[1] = parts[0]
      end
    end
    extend Gem::Deprecate
    deprecate :set_converter, "4.0.0", nil, nil

    #----------------------------------------------------------------------------------------------
    # Get the split points for the table
    def _get_splits_internal
      locator = @table.getRegionLocator
      locator.getAllRegionLocations
             .select { |s| RegionReplicaUtil.isDefaultReplica(s.getRegion) }
             .map { |i| Bytes.toStringBinary(i.getRegionInfo.getStartKey) }
             .delete_if { |k| k == '' }
    ensure
      locator.close
    end
  end
  # rubocop:enable Metrics/ClassLength
end
