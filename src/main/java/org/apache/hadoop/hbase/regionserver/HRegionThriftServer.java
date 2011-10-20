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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.thrift.ThriftServer;
import org.apache.hadoop.hbase.thrift.ThriftUtilities;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

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
  public static final int DEFAULT_LISTEN_PORT = 9090;

  private HRegionServer rs;
  private Configuration conf;

  private int port;
  private boolean nonblocking;
  private String bindIpAddress;
  private String transport;
  private String protocol;
  volatile private TServer tserver;

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

    // TODO: Override more methods to short-circuit for performance

    /**
     * Get a record. Short-circuit to get better performance.
     */
    @Override
    public List<TRowResult> getRowWithColumnsTs(ByteBuffer tableName,
                                                ByteBuffer rowb,
                                                List<ByteBuffer> columns,
                                                long timestamp)
      throws IOError {
      try {
        byte [] row = rowb.array();
        HTable table = getTable(tableName.array());
        HRegionLocation location = table.getRegionLocation(row);
        byte[] regionName = location.getRegionInfo().getEncodedNameAsBytes();

        if (columns == null) {
          Get get = new Get(row);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = rs.get(regionName, get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        ByteBuffer[] columnArr = columns.toArray(
                                   new ByteBuffer[columns.size()]);
        Get get = new Get(row);
        for(ByteBuffer column : columnArr) {
          byte [][] famAndQf = KeyValue.parseColumn(column.array());
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
        LOG.info("ThriftServer redirecting getRowWithColumnsTs");
        return super.getRowWithColumnsTs(tableName, rowb, columns, timestamp);
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

  @Override
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
        TNonblockingServerTransport serverTransport =
          new TNonblockingServerSocket(this.port);
        TFramedTransport.Factory transportFactory =
          new TFramedTransport.Factory();

        TNonblockingServer.Args serverArgs =
          new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        LOG.info("starting HRegionServer Nonblocking Thrift server on " +
            this.port);
        LOG.info("HRegionServer Nonblocking Thrift server does not " +
            "support address binding.");
        tserver = new TNonblockingServer(serverArgs);
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

        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.protocolFactory(protocolFactory);
        serverArgs.transportFactory(transportFactory);
        LOG.info("starting HRegionServer ThreadPool Thrift server on " +
                 listenAddress + ":" + this.port);
        tserver = new TThreadPoolServer(serverArgs);
      }
      tserver.serve();
    } catch (Exception e) {
      LOG.warn("Unable to start HRegionServerThrift interface.", e);
    }
  }
}
