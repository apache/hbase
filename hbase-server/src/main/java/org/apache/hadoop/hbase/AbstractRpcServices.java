/*
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
package org.apache.hadoop.hbase;

import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@InterfaceAudience.Private
public abstract class AbstractRpcServices implements HBaseRPCErrorHandler, PriorityFunction {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractRpcServices.class);
  protected RpcServerInterface rpcServer;
  protected InetSocketAddress isa;
  @Override
  public int getPriority(RPCProtos.RequestHeader header, Message param, User user) {
    return 0;
  }

  @Override
  public long getDeadline(RPCProtos.RequestHeader header, Message param) {
    return 0;
  }

  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getIsa() {
    return isa;
  }

  @Override
  public boolean checkOOME(final Throwable e) {
    return exitIfOOME(e);
  }

  public static boolean exitIfOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains("java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.error(HBaseMarkers.FATAL, "Run out of memory; "
            + AbstractRpcServices.class.getSimpleName() + " will abort itself immediately",
          e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }
  protected void stop(){
    rpcServer.stop();
  }

}
