/**
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

import static org.apache.hadoop.hbase.thrift.ThriftServerRunner.HBaseHandler.toBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.thrift.ThriftServerRunner;
import org.apache.hadoop.hbase.thrift.ThriftUtilities;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;

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
public class HRegionThriftServer extends Thread {

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
    this.serverRunner =
        new ThriftServerRunner(conf, new HBaseHandlerRegion(conf));
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
   * Inherit the Handler from the standard ThriftServerRunner. This allows us
   * to use the default implementation for most calls. We override certain calls
   * for performance reasons
   */
  private class HBaseHandlerRegion extends ThriftServerRunner.HBaseHandler {

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

    // TODO: Override more methods to short-circuit for performance

    /**
     * Get a record. Short-circuit to get better performance.
     */
    @Override
    public List<TRowResult> getRowWithColumnsTs(ByteBuffer tableName,
                                                ByteBuffer rowb,
                                                List<ByteBuffer> columns,
                                                long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        byte[] row = toBytes(rowb);
        HTable table = getTable(toBytes(tableName));
        HRegionLocation location = table.getRegionLocation(row, false);
        byte[] regionName = location.getRegionInfo().getRegionName();

        if (columns == null) {
          Get get = new Get(row);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = rs.get(regionName, get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(row);
        for(ByteBuffer column : columns) {
          byte [][] famAndQf = KeyValue.parseColumn(toBytes(column));
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
          LOG.warn(e.getMessage(), e);
          throw new IOError(e.getMessage());
        }
        LOG.debug("ThriftServer redirecting getRowWithColumnsTs");
        return super.getRowWithColumnsTs(tableName, rowb, columns, timestamp,
                                         attributes);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }
  }
}
