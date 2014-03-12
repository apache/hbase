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

import static org.apache.hadoop.hbase.thrift.ThriftServerRunner.convertIOException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.thrift.ThriftServerRunner;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
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
          throw new IOError(e.getMessage(), 0, e.getClass().getName());
        }
        LOG.info("ThriftServer redirecting atomicIncrement");
        return super.atomicIncrement(tableName, row, family,
                                     qualifier, amount);
      } catch (IOException e) {
        throw new IOError(e.getMessage(), 0, e.getClass().getName());
      }
    }

    /**
     * Process a get request. If the region name is set, using the shortcircuit optimization.
     */
    @Override
    protected Result processGet(ByteBuffer tableName, ByteBuffer regionName, Get get)
        throws IOException, IOError {
      if (Bytes.isNonEmpty(regionName)) {
        metrics.incDirectCalls();
        return rs.get(Bytes.getBytes(regionName), get);
      } else {
        metrics.incIndirectCalls();
        return super.processGet(tableName, regionName, get);
      }
    }

    /**
     * Process a put request. If the region name is set, using the shortcircuit optimization.
     */
    @Override
    protected void processPut(ByteBuffer tableName, ByteBuffer regionName, Put put)
        throws IOException, IOError {
      if (Bytes.isNonEmpty(regionName)) {
        metrics.incDirectCalls();
        rs.put(Bytes.getBytes(regionName), put);
      } else {
        metrics.incIndirectCalls();
        super.processPut(tableName, regionName, put);
      }
    }

    /**
     * Process a delete request. If the region name is set, using the shortcircuit optimization.
     */
    @Override
    protected void processDelete(ByteBuffer tableName, ByteBuffer regionName, Delete delete)
        throws IOException, IOError {
      if (Bytes.isNonEmpty(regionName)) {
        metrics.incDirectCalls();
        rs.delete(Bytes.getBytes(regionName), delete);
      } else {
        metrics.incIndirectCalls();
        super.processDelete(tableName, regionName, delete);
      }
    }

    /**
     * Process the multiGet requests. If the region name is set, using the shortcircuit 
     * optimization
     */
    @Override
    protected Result[] processMultiGet(ByteBuffer tableName, ByteBuffer regionName, List<Get> gets)
        throws IOException, IOError {
      if (Bytes.isNonEmpty(regionName)) {
        metrics.incDirectCalls();
        return rs.get(Bytes.getBytes(regionName), gets);
      } else {
        metrics.incIndirectCalls();
        return super.processMultiGet(tableName, regionName, gets);
      }
    }

    /**
     * Process the multiPut requests. If the region name is set, using the shortcircuit
     * optimization
     */
    @Override
    protected void processMultiPut(ByteBuffer tableName, ByteBuffer regionName, List<Put> puts)
        throws IOException, IOError {
      if (Bytes.isNonEmpty(regionName)) {
        metrics.incDirectCalls();
        rs.put(Bytes.getBytes(regionName), puts);
      } else {
        metrics.incIndirectCalls();
        super.processMultiPut(tableName, regionName, puts);
      }
    }

    /**
     * Process a delete request. If the region name is set, using the shortcircuit optimization.
     */
    @Override
    protected void processMultiDelete(ByteBuffer tableName, ByteBuffer regionName,
        List<Delete> deletes) throws IOException, IOError {
      if (Bytes.isNonEmpty(regionName)) {
        metrics.incDirectCalls();
        rs.delete(Bytes.getBytes(regionName), deletes);
      } else {
        metrics.incIndirectCalls();
        super.processMultiDelete(tableName, regionName, deletes);
      }
    }

    @Override
    public Map<ByteBuffer, Long> getLastFlushTimes() throws TException {
      Map<ByteBuffer, Long> regionToFlushTime = new HashMap<ByteBuffer, Long>();
      for (HRegion region: rs.getOnlineRegions()) {
        regionToFlushTime.put(ByteBuffer.wrap(region.getRegionName()),
                              region.getMinFlushTimeForAllStores());
      }
      return regionToFlushTime;
    }

    @Override
    public long getCurrentTimeMillis() throws TException {
      return rs.getCurrentTimeMillis();
    }

    @Override
    public void flushRegion(ByteBuffer regionName, long ifOlderThanTS)
        throws TException, IOError {
      try {
        rs.flushRegion(Bytes.getBytes(regionName), ifOlderThanTS);
      } catch (IOException ex) {
        throw convertIOException(ex);
      }
    }
  }
}
