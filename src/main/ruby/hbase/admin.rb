#
# Copyright 2010 The Apache Software Foundation
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

java_import java.util.ArrayList
java_import java.util.concurrent.TimeUnit

java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.zookeeper.ZooKeeperMain
java_import org.apache.hadoop.hbase.HColumnDescriptor
java_import org.apache.hadoop.hbase.HTableDescriptor
java_import org.apache.hadoop.hbase.io.hfile.Compression
java_import org.apache.hadoop.hbase.regionserver.StoreFile
java_import org.apache.hadoop.hbase.util.Pair
java_import org.apache.hadoop.hbase.util.RegionSplitter
java_import org.apache.hadoop.hbase.HRegionInfo
java_import org.apache.zookeeper.ZooKeeper

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class Admin
    include HBaseConstants

    def initialize(configuration, formatter)
      @admin = HBaseAdmin.new(configuration)
      connection = @admin.getConnection()
      @conf = configuration
      @zk_wrapper = connection.getZooKeeperWrapper()
      zk = @zk_wrapper.getZooKeeper()
      @zk_main = ZooKeeperMain.new(zk)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in hbase
    def list
      @admin.listTables.map { |t| t.getNameAsString }
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region flush
    def flush(table_or_region_name)
      @admin.flush(table_or_region_name)
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region or column family compaction
    def compact(table_or_region_name, *args)
      if args.empty?
        @admin.compact(table_or_region_name)
      elsif args.length == 1
        # We are compacting a column family within a region.
        region_name = table_or_region_name
        column_family = args.first
        @admin.compact(region_name, column_family)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region major compaction
    def major_compact(table_or_region_name, *args)
      if args.empty?
        @admin.majorCompact(table_or_region_name)
      elsif args.length == 1
        # We are major compacting a column family within a region or table.
        column_family = args.first
        @admin.majorCompact(table_or_region_name, column_family)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region split
    def split(table_or_region_name, split_point)
      if split_point == nil
        @admin.split(table_or_region_name)
      else
        @admin.split(table_or_region_name, split_point)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Enables a table
    def enable(table_name)
      return if enabled?(table_name)
      @admin.enableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a table
    def disable(table_name)
      return unless enabled?(table_name)
      @admin.disableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop(table_name)
      raise ArgumentError, "Table #{table_name} does not exist.'" unless exists?(table_name)
      raise ArgumentError, "Table #{table_name} is enabled. Disable it first.'" if enabled?(table_name)

      @admin.deleteTable(table_name)
      flush(HConstants::META_TABLE_NAME)
      major_compact(HConstants::META_TABLE_NAME)
    end

    #----------------------------------------------------------------------------------------------
    # enable_loadbalancer
    def enable_loadbalancer
      @admin.enableLoadBalancer
      get_loadbalancer
    end

  #----------------------------------------------------------------------------------------------
    # disable_loadbalancer
    def disable_loadbalancer
      @admin.disableLoadBalancer
      get_loadbalancer
    end

  #----------------------------------------------------------------------------------------------
    # Shuts hbase down
    def get_loadbalancer
      print "LoadBalacner disabled: %s \n " % @admin.isLoadBalancerDisabled
    end

  #----------------------------------------------------------------------------------------------
    # Shuts hbase down
    def shutdown
      @admin.shutdown
    end

    #----------------------------------------------------------------------------------------------
    # Returns ZooKeeper status dump
    def zk_dump
      @zk_wrapper.dump
    end

    #----------------------------------------------------------------------------------------------
    # Creates a table
    def create(table_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Flatten params array
      args = args.flatten.compact

      # Fail if no column families defined
      raise(ArgumentError, "Table must have at least one column family") if args.empty?

      # Start defining the table
      htd = HTableDescriptor.new(table_name)

      # Parameters for pre-splitting the table
      num_regions = nil
      split_algo = nil

      # All args are columns, add them to the table definition
      # TODO: add table options support
      args.each do |arg|
        unless arg.kind_of?(String) || arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end
        if arg.kind_of?(String)
          # the arg is a string, default action is to add a column to the table
          htd.addFamily(hcd(arg, htd))
        else
          # arg is a hash.  3 possibilities:
          if (arg.has_key?(NUMREGIONS) or arg.has_key?(SPLITALGO))
            # (1) deprecated region pre-split API
            raise(ArgumentError, "Column family configuration should be specified in a separate clause") if arg.has_key?(NAME)
            raise(ArgumentError, "Number of regions must be specified") unless arg.has_key?(NUMREGIONS)
            raise(ArgumentError, "Split algorithm must be specified") unless arg.has_key?(SPLITALGO)
            raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
            num_regions = arg[NUMREGIONS]
            split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg[SPLITALGO])
          elsif (method = arg.delete(METHOD))
            # (2) table_attr modification
            raise(ArgumentError, "table_att is currently the only supported method") unless method == 'table_att'
            raise(ArgumentError, "NUMREGIONS & SPLITALGO must both be specified") unless arg.has_key?(NUMREGIONS) == arg.has_key?(split_algo)
            htd.setMaxFileSize(JLong.valueOf(arg[MAX_FILESIZE])) if arg[MAX_FILESIZE]
            htd.setReadOnly(JBoolean.valueOf(arg[READONLY])) if arg[READONLY]
            htd.setMemStoreFlushSize(JLong.valueOf(arg[MEMSTORE_FLUSHSIZE])) if arg[MEMSTORE_FLUSHSIZE]
            htd.setDeferredLogFlush(JBoolean.valueOf(arg[DEFERRED_LOG_FLUSH])) if arg[DEFERRED_LOG_FLUSH]
            htd.setWALDisabled(JBoolean.valueOf(arg[DISABLE_WAL])) if arg[DISABLE_WAL]
            if arg[NUMREGIONS]
              raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
              num_regions = arg[NUMREGIONS]
              split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg[SPLITALGO])
            end
            if arg[CONFIG]
              raise(ArgumentError, "#{CONFIG} must be a Hash type") unless arg.kind_of?(Hash)
              for k,v in arg[CONFIG]
                v = v.to_s unless v.nil?
                htd.setValue(k, v)
              end
            end
          else
            # (3) column family spec
            htd.addFamily(hcd(arg, htd))
          end
        end
      end
      if num_regions.nil?
        # Perform the create table call
        @admin.createTable(htd)
      else
        # Compute the splits for the predefined number of regions
        splits = split_algo.split(JInteger.valueOf(num_regions))

        # Perform the create table call
        @admin.createTable(htd, splits)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Closes a region
    def close_region(region_name, server = nil)
      @admin.closeRegion(region_name, server ? [server].to_java : nil)
    end

    #----------------------------------------------------------------------------------------------
    # Moves a region
    def move_region(region_name, server)
      @admin.moveRegion(region_name, server)
    end

    #----------------------------------------------------------------------------------------------
    # Enables a region
    def enable_region(region_name)
      online(region_name, false)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a region
    def disable_region(region_name)
      online(region_name, true)
    end

    #----------------------------------------------------------------------------------------------
    # Returns table's structure description
    def describe(table_name)
      tables = @admin.listTables.to_a
      tables << HTableDescriptor::META_TABLEDESC
      tables << HTableDescriptor::ROOT_TABLEDESC

      tables.each do |t|
        # Found the table
        return t.to_s if t.getNameAsString == table_name
      end

      raise(ArgumentError, "Failed to find table named #{table_name}")
    end

    #----------------------------------------------------------------------------------------------
    # Truncates table (deletes all records by recreating the table)
    def truncate(table_name)
      h_table = HTable.new(@conf, table_name)
      table_description = h_table.getTableDescriptor()
      yield 'Disabling table...' if block_given?
      disable(table_name)

      yield 'Dropping table...' if block_given?
      drop(table_name)

      yield 'Creating table...' if block_given?
      @admin.createTable(table_description)
    end

    #----------------------------------------------------------------------------------------------
    # Check the status of alter command (number of regions reopened)
       def alter_status(table_name)
         # Table name should be a string
         raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

         # Table should exist
         raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

         status = Pair.new()
         begin
           status = @admin.getAlterStatus(table_name.to_java_bytes)
           if status.getSecond() != 0
             puts "#{status.getSecond() - status.getFirst()}/#{status.getSecond()} regions updated."
           else
             puts "All regions updated."
           end
           sleep 1
         end while status != nil && status.getFirst() != 0
         puts "Done."
       end    
       
    #----------------------------------------------------------------------------------------------
    # Change table structure or table options
    def alter(table_name, wait = true, *args)
      # Table name should be a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Table should exist
      raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

      # Table should be disabled
      # raise(ArgumentError, "Table #{table_name} is enabled. Disable it first before altering.") if enabled?(table_name)

      # There should be at least one argument
      raise(ArgumentError, "There should be at least one argument but the table name") if args.empty?

      # Get table descriptor
      htd = @admin.getTableDescriptor(table_name.to_java_bytes)

      # Process all args
      columnsToAdd = ArrayList.new()
      columnsToMod = ArrayList.new()
      columnsToDel = ArrayList.new()

      waitInterval = @conf.getInt(HConstants::MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS,
                                         HConstants::DEFAULT_MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS);
      numConcurrentRegionsClosed =  @conf.getInt(HConstants::MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE,
                                         HConstants::DEFAULT_MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE);

      args.each do |arg|
        # Normalize args to support column name only alter specs
        arg = { NAME => arg } if arg.kind_of?(String)

        # Normalize args to support shortcut delete syntax
        arg = { METHOD => 'delete', NAME => arg['delete'] } if arg['delete']

        # Now set regionCloseWaitInterval if specified
        if arg[WAIT_INTERVAL]
          waitInterval = arg[WAIT_INTERVAL]
        end

        # Now set the NumConcurrentCloseRegions if specified
        if arg[NUM_CONCURRENT_CLOSE]
          numConcurrentRegionsClosed = arg[NUM_CONCURRENT_CLOSE]
        end

        # No method parameter, try to use the args as a column definition
        unless method = arg.delete(METHOD)
          descriptor = hcd(arg, htd)
          column_name = descriptor.getNameAsString

          # If column already exist, then try to alter it. Create otherwise.
          if htd.hasFamily(column_name.to_java_bytes)
            columnsToMod.add(Pair.new(column_name, descriptor))
          else
            columnsToAdd.add(descriptor)
          end
          next
        end

        # Delete column family
        if method == "delete"
          raise(ArgumentError, "NAME parameter missing for delete method") unless arg[NAME]
          columnsToDel.add(arg[NAME])
          next
        end

        # Change table attributes
        if method == "table_att"
          # Table should be disabled
          raise(ArgumentError, "Table #{table_name} is enabled. Disable it first before altering.") if enabled?(table_name)          
          htd.setMaxFileSize(JLong.valueOf(arg[MAX_FILESIZE])) if arg[MAX_FILESIZE]
          htd.setReadOnly(JBoolean.valueOf(arg[READONLY])) if arg[READONLY]
          htd.setMemStoreFlushSize(JLong.valueOf(arg[MEMSTORE_FLUSHSIZE])) if arg[MEMSTORE_FLUSHSIZE]
          htd.setDeferredLogFlush(JBoolean.valueOf(arg[DEFERRED_LOG_FLUSH])) if arg[DEFERRED_LOG_FLUSH]
          if arg[CONFIG]
            raise(ArgumentError, "#{CONFIG} must be a Hash type") unless arg.kind_of?(Hash)
            for k,v in arg[CONFIG]
              v = v.to_s unless v.nil?
              htd.setValue(k, v)
            end
          end
          @admin.modifyTable(table_name.to_java_bytes, htd)
          next
        end

        # Unknown method
        raise ArgumentError, "Unknown method: #{method}"
      end

      # now batch process alter requests
      @admin.alterTable(table_name, columnsToAdd, columnsToMod, columnsToDel, waitInterval, numConcurrentRegionsClosed)
      if wait == true
        puts "Updating all regions with the new schema..."
        alter_status(table_name)
      end            
    end

    def status(format)
      status = @admin.getClusterStatus()
      if format == "detailed"
        puts("version %s" % [ status.getHBaseVersion() ])
        # Put regions in transition first because usually empty
        puts("%d regionsInTransition" % status.getRegionsInTransition().size())
        for k, v in status.getRegionsInTransition()
          puts("    %s" % [v])
        end
        puts("%d live servers" % [ status.getServers() ])
        for server in status.getServerInfo()
          puts("    %s:%d %d" % \
            [ server.getServerAddress().getHostname(),  \
              server.getServerAddress().getPort(), server.getStartCode() ])
          puts("        %s" % [ server.getLoad().toString() ])
          for region in server.getLoad().getRegionsLoad()
            puts("        %s" % [ region.getNameAsString() ])
            puts("            %s" % [ region.toString() ])
          end
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
      elsif format == "simple"
        load = 0
        regions = 0
        puts("%d live servers" % [ status.getServers() ])
        for server in status.getServerInfo()
          puts("    %s:%d %d" % \
            [ server.getServerAddress().getHostname(),  \
              server.getServerAddress().getPort(), server.getStartCode() ])
          puts("        %s" % [ server.getLoad().toString() ])
          load += server.getLoad().getNumberOfRequests()
          regions += server.getLoad().getNumberOfRegions()
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
        puts("Aggregate load: %d, regions: %d" % [ load , regions ] )
      else
        puts "#{status.getServers} servers, #{status.getDeadServers} dead, #{'%.4f' % status.getAverageLoad} average load"
      end
    end

    #----------------------------------------------------------------------------------------------
    #
    # Helper methods
    #

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Is table enabled
    def enabled?(table_name)
      @admin.isTableEnabled(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Return a new HColumnDescriptor made of passed args
    def hcd(arg, htd)
      # String arg, single parameter constructor
      return HColumnDescriptor.new(arg) if arg.kind_of?(String)

      raise(ArgumentError, "Column family #{arg} must have a name") unless name = arg[NAME]

      family = htd.getFamily(name.to_java_bytes)
      # create it if it's a new family
      family ||= HColumnDescriptor.new(name.to_java_bytes)

      family.setBlockCacheEnabled(JBoolean.valueOf(arg[HColumnDescriptor::BLOCKCACHE])) if arg.include?(HColumnDescriptor::BLOCKCACHE)
      family.setBloomFilterType(StoreFile::BloomType.valueOf(arg[HColumnDescriptor::BLOOMFILTER])) if arg.include?(HColumnDescriptor::BLOOMFILTER)
      family.setBloomFilterErrorRate(JFloat.valueOf(arg[HColumnDescriptor::BLOOMFILTER_ERRORRATE])) if arg.include?(HColumnDescriptor::BLOOMFILTER_ERRORRATE)
      family.setScope(JInteger.valueOf(arg[REPLICATION_SCOPE])) if arg.include?(HColumnDescriptor::REPLICATION_SCOPE)
      family.setInMemory(JBoolean.valueOf(arg[IN_MEMORY])) if arg.include?(HColumnDescriptor::IN_MEMORY)
      family.setTimeToLive(JInteger.valueOf(arg[HColumnDescriptor::TTL])) if arg.include?(HColumnDescriptor::TTL)
      family.setFlashBackQueryLimit(JInteger.valueOf(arg[HColumnDescriptor::FLASHBACK_QUERY_LIMIT])) if arg.include?(HColumnDescriptor::FLASHBACK_QUERY_LIMIT)
      family.setCompressionType(Compression::Algorithm.valueOf(arg[HColumnDescriptor::COMPRESSION])) if arg.include?(HColumnDescriptor::COMPRESSION)
      family.setDataBlockEncoding(org.apache.hadoop.hbase.io.encoding.DataBlockEncoding.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING)
      family.setEncodeOnDisk(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::ENCODE_ON_DISK])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::ENCODE_ON_DISK)
      family.setBlocksize(JInteger.valueOf(arg[HColumnDescriptor::BLOCKSIZE])) if arg.include?(HColumnDescriptor::BLOCKSIZE)
      family.setMaxVersions(JInteger.valueOf(arg[VERSIONS])) if arg.include?(HColumnDescriptor::VERSIONS)
      if arg[CONFIG]
        raise(ArgumentError, "#{CONFIG} must be a Hash type") unless arg.kind_of?(Hash)
        for k,v in arg[CONFIG]
          v = v.to_s unless v.nil?
          family.setValue(k, v)
        end
      end

      return family
    end

    #----------------------------------------------------------------------------------------------
    # Enables/disables a region by name
    def online(region_name, on_off)
      # Open meta table
      meta = HTable.new(@conf, HConstants::META_TABLE_NAME)

      # Read region info
      # FIXME: fail gracefully if can't find the region
      region_bytes = Bytes.toBytes(region_name)
      g = Get.new(region_bytes)
      g.addColumn(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)
      hri_bytes = meta.get(g).value

      # Change region status
      hri = Writables.getWritable(hri_bytes, HRegionInfo.new)
      hri.setOffline(on_off)

      # Write it back
      put = Put.new(region_bytes)
      put.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
      meta.put(put)
    end
    #----------------------------------------------------------------------------------------------
    # Invoke a ZooKeeper maintenance command
    def zk(args)
      line = args.join(' ')
      line = 'help' if line.empty?
      @zk_main.executeLine(line)
    end
  end
end
