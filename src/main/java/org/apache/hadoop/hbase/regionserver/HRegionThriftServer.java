/**
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
import java.util.List;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.TBoundedThreadPoolServer;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift.ThriftServer;
import org.apache.hadoop.hbase.thrift.ThriftUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * ThriftServer - this class starts up a Thrift server in the same
 * JVM where the RegionServer is running. It inherits most of the
 * functionality from the standard ThriftServer. This is good because
 * we can maintain compatibility with applications that use the
 * standard Thrift interface. For performance reasons, we override
 * methods to directly invoke calls into the HRegionServer.
 */
public class HRegionThriftServer extends Thread {

  public static final Log LOG = LogFactory.getLog(HRegionThriftServer.class);
  public static final int DEFAULT_LISTEN_PORT = 9091;

  private HRegionServer rs;
  private Configuration conf;

  private int port;
  private boolean nonblocking;
  private String bindIpAddress;
  private String transport;
  private String protocol;
  volatile private TServer tserver;
  private boolean redirect; // redirect to appropriate Regionserver

  /**
   * Create an instance of the glue object that connects the
   * RegionServer with the standard ThriftServer implementation
   */
  HRegionThriftServer(HRegionServer regionServer, Configuration conf) {
    this.rs = regionServer;
    this.conf = conf;
  }

  /**
   * Inherit the Handler from the standard ThriftServer. This allows us
   * to use the default implementation for most calls. We override certain calls
   * for performance reasons
   */
  private class HBaseHandlerRegion extends ThriftServer.HBaseHandler {

    HBaseHandlerRegion(final Configuration conf) throws IOException {
      super(conf);
      initialize(conf);
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
          throw new IOError(e.getMessage());
        }
        LOG.info("ThriftServer redirecting atomicIncrement");
        return super.atomicIncrement(tableName, row, family,
                                     qualifier, amount);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    /**
     * Get a record. Shortcircuit to get better performance.
     */
    @Override
    public List<TRowResult> getRowWithColumnsTs(byte[] tableName, byte[] row,
                                                List<byte[]> columns,
                                                long timestamp)
      throws IOError {
      try {
        HTable table = getTable(tableName);
        HRegionLocation location = table.getRegionLocation(row);
        byte[] regionName = location.getRegionInfo().getRegionName();

        if (columns == null) {
          Get get = new Get(row);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = rs.get(regionName, get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        byte[][] columnArr = columns.toArray(new byte[columns.size()][]);
        Get get = new Get(row);
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
          throw new IOError(e.getMessage());
        }
        LOG.info("ThriftServer redirecting getRowWithColumnsTs");
        return super.getRowWithColumnsTs(tableName, row, columns, timestamp);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    @Override
    public List<TRowResult> getRowWithColumnPrefix(byte[] tableName,
        byte[] row, byte[] prefix) throws IOError {
      return (getRowWithColumnPrefixTs(tableName, row, prefix,
          HConstants.LATEST_TIMESTAMP));
    }

    @Override
    public List<TRowResult> getRowWithColumnPrefixTs(byte[] tableName,
        byte[] row, byte[] prefix, long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        if (prefix == null) {
          Get get = new Get(row);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(row);
        byte[][] famAndPrefix = KeyValue.parseColumn(prefix);
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
        throw new IOError(e.getMessage());
      }
    }

  }

  /**
   * Read and initialize config parameters
   */
  private void initialize(Configuration conf) {
    this.port = conf.getInt("hbase.regionserver.thrift.port",
                            DEFAULT_LISTEN_PORT);
    this.bindIpAddress = conf.get("hbase.regionserver.thrift.ipaddress");
    this.protocol = conf.get("hbase.regionserver.thrift.protocol");
    this.transport = conf.get("hbase.regionserver.thrift.transport");
    this.nonblocking = conf.getBoolean("hbase.regionserver.thrift.nonblocking",
                                       false);
    this.redirect = conf.getBoolean("hbase.regionserver.thrift.redirect",
                                       false);
  }

  /**
   * Stop ThriftServer
   */
  void shutdown() {
    if (tserver != null) {
      tserver.stop();
      tserver = null;
    }
  }

  public void run() {
    try {
      HBaseHandlerRegion handler = new HBaseHandlerRegion(this.conf);
      Hbase.Processor processor = new Hbase.Processor(handler);

      TProtocolFactory protocolFactory;
      if (this.protocol != null && this.protocol.equals("compact")) {
        protocolFactory = new TCompactProtocol.Factory();
      } else {
        protocolFactory = new TBinaryProtocol.Factory();
      }

      if (this.nonblocking) {
        LOG.info("starting HRegionServer Nonblocking Thrift server on " +
                 this.port);
        LOG.info("HRegionServer Nonblocking Thrift server does not " +
                 "support address binding.");
        TNonblockingServerTransport serverTransport =
          new TNonblockingServerSocket(this.port);
        TFramedTransport.Factory transportFactory =
          new TFramedTransport.Factory();

        tserver = new TNonblockingServer(processor, serverTransport,
                                        transportFactory, protocolFactory);
      } else {
        InetAddress listenAddress = null;
        if (this.bindIpAddress != null) {
          listenAddress = InetAddress.getByName(this.bindIpAddress);
        } else {
          listenAddress = InetAddress.getLocalHost();
        }
        TServerTransport serverTransport = new TServerSocket(
           new InetSocketAddress(listenAddress, port));

        TTransportFactory transportFactory;
        if (this.transport != null && this.transport.equals("framed")) {
          transportFactory = new TFramedTransport.Factory();
        } else {
          transportFactory = new TTransportFactory();
        }

        TBoundedThreadPoolServer.Options serverOptions =
            new TBoundedThreadPoolServer.Options(conf);

        LOG.info("starting " + ThriftServer.THREAD_POOL_SERVER_CLASS.getSimpleName() + " on "
            + listenAddress + ":" + Integer.toString(port)
            + "; minimum number of worker threads="
            + serverOptions.minWorkerThreads
            + ", maximum number of worker threads="
            + serverOptions.maxWorkerThreads + ", queued requests="
            + serverOptions.maxQueuedRequests);
        ThriftMetrics metrics = new ThriftMetrics(port, conf);
        tserver = new TBoundedThreadPoolServer(processor, serverTransport,
                                       transportFactory, protocolFactory, serverOptions, metrics);
      }
      tserver.serve();
    } catch (Exception e) {
      LOG.warn("Unable to start HRegionServerThrift interface.", e);
    }
  }
}
