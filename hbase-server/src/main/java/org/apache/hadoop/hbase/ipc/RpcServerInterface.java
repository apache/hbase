/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.authorize.PolicyProvider;

import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import com.google.common.base.Function;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
public interface RpcServerInterface {
  // TODO: Needs cleanup.  Why a 'start', and then a 'startThreads' and an 'openServer'?

  void setSocketSendBufSize(int size);

  void start();

  void stop();

  void join() throws InterruptedException;

  InetSocketAddress getListenerAddress();

  Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
    Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
  throws IOException, ServiceException;

  void setErrorHandler(HBaseRPCErrorHandler handler);

  void openServer();

  void startThreads();

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  MetricsHBaseServer getMetrics();

  void setQosFunction(Function<Pair<RequestHeader, Message>, Integer> newFunc);

  /**
   * Refresh autentication manager policy.
   * @param pp
   */
  @VisibleForTesting
  void refreshAuthManager(PolicyProvider pp);
}
