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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AbstractRpcServices;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactionService;

@InterfaceAudience.Private
public class CSRpcServices extends AbstractRpcServices
    implements CompactionService.BlockingInterface {
  protected static final Logger LOG = LoggerFactory.getLogger(CSRpcServices.class);

  private final HCompactionServer compactionServer;

  // Request counter.
  final LongAdder requestCount = new LongAdder();
  /** RPC scheduler to use for the compaction server. */
  public static final String COMPACTION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
      "hbase.compaction.server.rpc.scheduler.factory.class";
  /**
   * @return immutable list of blocking services and the security info classes that this server
   *         supports
   */
  protected List<RpcServer.BlockingServiceAndInterface> getServices(final Configuration conf) {
    // now return empty, compaction server do not receive rpc request
    List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>();
    bssi.add(new RpcServer.BlockingServiceAndInterface(
        CompactionService.newReflectiveBlockingService(this),
        CompactionService.BlockingInterface.class));
    return new ImmutableList.Builder<RpcServer.BlockingServiceAndInterface>().addAll(bssi).build();
  }

  void start() {
    rpcServer.start();
  }

  protected Class<?> getRpcSchedulerFactoryClass(Configuration conf) {
    return conf.getClass(COMPACTION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  CSRpcServices(final HCompactionServer cs) throws IOException {
    super(cs);
    compactionServer = cs;
  }


  /**
   * Request compaction on the compaction server.
   * @param controller the RPC controller
   * @param request the compaction request
   */
  @Override
  public CompactResponse requestCompaction(RpcController controller,
      CompactionProtos.CompactRequest request) {
    requestCount.increment();
    LOG.info("Receive compaction request from {}", ProtobufUtil.toString(request));
    compactionServer.compactionThreadManager.requestCompaction();
    return CompactionProtos.CompactResponse.newBuilder().build();
  }

}
