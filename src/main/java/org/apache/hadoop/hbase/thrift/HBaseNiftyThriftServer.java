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

package org.apache.hadoop.hbase.thrift;

import com.facebook.nifty.codec.DefaultThriftFrameCodecFactory;
import com.facebook.nifty.codec.ThriftFrameCodecFactory;
import com.facebook.nifty.core.NiftyTimer;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.nifty.header.codec.HeaderThriftCodecFactory;
import com.facebook.nifty.header.protocol.TDuplexHeaderProtocolFactory;
import com.facebook.nifty.header.protocol.TFacebookCompactProtocol;
import com.facebook.nifty.processor.NiftyProcessor;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.ipc.thrift.ThriftCallStatsReporter;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HBaseNiftyThriftServer {
  public static final ImmutableMap<String, TDuplexProtocolFactory> SWIFT_PROTOCOL_FACTORIES = ImmutableMap
      .of("binary", TDuplexProtocolFactory
          .fromSingleFactory(new TBinaryProtocol.Factory()), "compact",
          TDuplexProtocolFactory
              .fromSingleFactory(new TCompactProtocol.Factory()), "header",
          new TDuplexHeaderProtocolFactory(), "fcompact",
          TDuplexProtocolFactory
              .fromSingleFactory(new TFacebookCompactProtocol.Factory()));

  public static final ImmutableMap<String, ThriftFrameCodecFactory> SWIFT_FRAME_CODEC_FACTORIES = ImmutableMap
      .<String, ThriftFrameCodecFactory> of("buffered",
          new DefaultThriftFrameCodecFactory(), "framed",
          new DefaultThriftFrameCodecFactory(), "header",
          new HeaderThriftCodecFactory());
  // The port at which the server needs to run
  private int port;
  private static final Log LOG = LogFactory.getLog(HBaseNiftyThriftServer.class);
  private ThriftServer server;
  private HBaseRpcMetrics rpcMetrics;
  private List<ThriftEventHandler> thriftClientStatsReporterList =
    new ArrayList<>();
  private boolean useHeaderProtocol;
  public static final ThriftCodecManager THRIFT_CODEC_MANAGER =
    new ThriftCodecManager();

  public HBaseRpcMetrics getRpcMetrics() {
    return rpcMetrics;
  }

  public HBaseNiftyThriftServer(Configuration conf, ThriftClientInterface instance, int port) {
    // / TODO:manukranthk verify whether the port is being used
    this.port = port;
    this.useHeaderProtocol = conf.getBoolean(HConstants.USE_HEADER_PROTOCOL,
      HConstants.DEFAULT_USE_HEADER_PROTOCOL);
    this.rpcMetrics = new HBaseRpcMetrics(
        HBaseRPC.Server.classNameBase(instance.getClass().getName()),
        new Integer(port).toString());
    // The event handler which pushes down the per-call metrics.
    ThriftEventHandler reporter = new ThriftCallStatsReporter(conf, rpcMetrics,
        instance);
    thriftClientStatsReporterList.add(reporter);

    NiftyProcessor processor = new ThriftServiceProcessor(
      THRIFT_CODEC_MANAGER, thriftClientStatsReporterList, instance);

    ThriftServerConfig serverConfig = new ThriftServerConfig()
      .setPort(port)
      .setMaxFrameSize(
        new DataSize(conf.getInt(HConstants.SWIFT_MAX_FRAME_SIZE_BYTES,
          HConstants.SWIFT_MAX_FRAME_SIZE_BYTES_DEFAULT), Unit.BYTE))
      .setConnectionLimit(
        conf.getInt(HConstants.SWIFT_CONNECTION_LIMIT,
          HConstants.SWIFT_CONNECTION_LIMIT_DEFAULT))
      .setIoThreadCount(
        conf.getInt(HConstants.SWIFT_IO_THREADS,
          HConstants.SWIFT_IO_THREADS_DEFAULT))
      .setWorkerThreads(
        conf.getInt(HConstants.SWIFT_WORKER_THREADS,
          HConstants.SWIFT_WORKER_THREADS_DEFAULT))
      .setIdleConnectionTimeout(
            new Duration(conf.getInt(
                HConstants.SWIFT_CONNECTION_IDLE_MAX_MINUTES,
                HConstants.SWIFT_CONNECTION_IDLE_MAX_MINUTES_DEFAULT),
                TimeUnit.MINUTES));

    if (useHeaderProtocol) {
      server = new ThriftServer(processor, serverConfig.setProtocolName(
          "header").setTransportName("header"), new NiftyTimer("thrift"),
          SWIFT_FRAME_CODEC_FACTORIES, SWIFT_PROTOCOL_FACTORIES);
    } else {
      server = new ThriftServer(processor, serverConfig.setProtocolName(
          "fcompact").setTransportName("framed"), new NiftyTimer("thrift"),
          SWIFT_FRAME_CODEC_FACTORIES, SWIFT_PROTOCOL_FACTORIES);
    }
    server.start();
    LOG.debug("Starting the thrift server. Listening on port " + this.port);
  }

  public int getPort() {
    return this.server.getPort();
  }

  public void stop() {
    server.close();
  }
}
