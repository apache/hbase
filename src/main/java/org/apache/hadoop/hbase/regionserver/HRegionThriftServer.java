/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.thrift.ThriftServerRunner;
import org.apache.hadoop.hbase.thrift.ThriftUtilities;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.thrift.TException;

/**
 * HRegionThriftServer - this class starts up a Thrift server in the same
 * JVM where the RegionServer is running. It inherits most of the
 * functionality from the standard ThriftServer. This is good because
 * we can maintain compatibility with applications that use the
 * standard Thrift interface. For performance reasons, we can override
 * methods to directly invoke calls into the HRegionServer and avoid the hop.
 * <p>
 * This can be enabled with <i>hbase.regionserver.export.thrift</i> set to true.
 */
public class HRegionThriftServer extends HasThread {

  public static final Log LOG = LogFactory.getLog(HRegionThriftServer.class);

  private final HRegionServer rs;
  private final ThriftServerRunner serverRunner;

  /**
   * Create an instance of the glue object that connects the
   * RegionServer with the standard ThriftServer implementation
   */
  HRegionThriftServer(HRegionServer regionServer, Configuration conf)
      throws IOException {
    super("Region Thrift Server");
    this.rs = regionServer;
    this.serverRunner = new ThriftServerRunner(conf, HConstants.RS_THRIFT_PREFIX,
        new HBaseHandlerRegion(conf));
  }

  /**
   * Stop ThriftServer
   */
  void shutdown() {
    serverRunner.shutdown();
  }

  @Override
  public void run() {
    serverRunner.run();
  }

  /**
   * Inherit the Handler from the standard ThriftServer. This allows us
   * to use the default implementation for most calls. We override certain calls
   * for performance reasons
   */
  private class HBaseHandlerRegion extends ThriftServerRunner.HBaseHandler
      implements Hbase.Iface {

    /**
     * Whether requests should be redirected to other RegionServers if the
     * specified region is not hosted by this RegionServer.
     */
    private boolean redirect;

    HBaseHandlerRegion(final Configuration conf) throws IOException {
      super(conf);
      initialize(conf);
    }

    /**
     * Read and initialize config parameters
     */
    private void initialize(Configuration conf) {
      this.redirect = conf.getBoolean("hbase.regionserver.thrift.redirect",
          false);
    }

    /**
     * Do increments. Shortcircuit to get better performance.
     */
    @Override
    public long atomicIncrement(byte[] tableName, byte [] row, byte [] family,
                                byte [] qualifier, long amount)
      throws IOError, IllegalArgument, TException {
      try {
        HTable table = getTable(tableName);
        HRegionLocation location = table.getRegionLocation(row);
        byte[] regionName = location.getRegionInfo().getRegionName();

        return rs.incrementColumnValue(regionName, row, family,
                                       qualifier, amount, true);
      } catch (NotServingRegionException e) {
        if (!redirect) {
          throw new IOError(e.getMessage(), 0);
        }
        LOG.info("ThriftServer redirecting atomicIncrement");
        return super.atomicIncrement(tableName, row, family,
                                     qualifier, amount);
      } catch (IOException e) {
        throw new IOError(e.getMessage(), 0);
      }
    }

    /**
     * Get a record. Shortcircuit to get better performance.
     */
    @Override
    public List<TRowResult> getRowWithColumnsTs(ByteBuffer tableName, ByteBuffer row,
                                                List<ByteBuffer> columns,
                                                long timestamp)
      throws IOError {
      try {
        HTable table = getTable(tableName);
        byte[] rowBytes = Bytes.getBytes(row);
        HRegionLocation location = table.getRegionLocation(rowBytes);
        byte[] regionName = location.getRegionInfo().getRegionName();

        if (columns == null) {
          Get get = new Get(rowBytes);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = rs.get(regionName, get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        byte[][] columnArr = columns.toArray(new byte[columns.size()][]);
        Get get = new Get(rowBytes);
        for (byte[] column : columnArr) {
          byte[][] famAndQf = KeyValue.parseColumn(column);
          if (famAndQf.length == 1) {
            get.addFamily(famAndQf[0]);
          } else {
            get.addColumn(famAndQf[0], famAndQf[1]);
          }
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = rs.get(regionName, get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (NotServingRegionException e) {
        if (!redirect) {
          throw new IOError(e.getMessage(), 0);
        }
        LOG.info("ThriftServer redirecting getRowWithColumnsTs");
        return super.getRowWithColumnsTs(tableName, row, columns, timestamp);
      } catch (IOException e) {
        throw new IOError(e.getMessage(), 0);
      }
    }

    @Override
    public List<TRowResult> getRowWithColumnPrefix(ByteBuffer tableName,
        ByteBuffer row, ByteBuffer prefix) throws IOError {
      return (getRowWithColumnPrefixTs(tableName, row, prefix,
          HConstants.LATEST_TIMESTAMP));
    }

    @Override
    public List<TRowResult> getRowWithColumnPrefixTs(ByteBuffer tableName,
        ByteBuffer row, ByteBuffer prefix, long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        byte[] rowBytes = Bytes.getBytes(row);
        if (prefix == null) {
          Get get = new Get(rowBytes);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(rowBytes);
        byte[][] famAndPrefix = KeyValue.parseColumn(Bytes.getBytes(prefix));
        if (famAndPrefix.length == 2) {
          get.addFamily(famAndPrefix[0]);
          get.setFilter(new ColumnPrefixFilter(famAndPrefix[1]));
        } else {
          get.setFilter(new ColumnPrefixFilter(famAndPrefix[0]));
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw new IOError(e.getMessage(), 0);
      }
    }
  }
}
