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

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.authorize.PolicyProvider;

@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public interface RpcServerInterface {
  void start();
  boolean isStarted();

  void stop();
  void join() throws InterruptedException;

  void setSocketSendBufSize(int size);
  InetSocketAddress getListenerAddress();

  /**
   * @deprecated As of release 1.3, this will be removed in HBase 3.0
   */
  @Deprecated
  Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
    Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
  throws IOException, ServiceException;

  Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md, Message param,
      CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status, long startTime,
      int timeout) throws IOException, ServiceException;

  void setErrorHandler(HBaseRPCErrorHandler handler);
  HBaseRPCErrorHandler getErrorHandler();

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  MetricsHBaseServer getMetrics();

  /**
   * Add/subtract from the current size of all outstanding calls.  Called on setup of a call to add
   * call total size and then again at end of a call to remove the call size.
   * @param diff Change (plus or minus)
   */
  void addCallSize(long diff);

  /**
   * It was removed in HBASE-24174 and added back in HBASE-25285 for compactibility concern.
   * NOTICE: implementations should call {@link #refreshAuthManager(Configuration, PolicyProvider)}
   * for correctness.
   * @param pp PolicyProvider
   */
  @InterfaceAudience.Private
  @Deprecated
  void refreshAuthManager(PolicyProvider pp);

  /**
   * Refresh authentication manager policy.
   * @param conf configuration for refresh
   * @param pp
   */
  @InterfaceAudience.Private
  void refreshAuthManager(Configuration conf, PolicyProvider pp);

  RpcScheduler getScheduler();

  void setRsRpcServices(RSRpcServices rsRpcServices);

  /**
   * Set Online SlowLog Provider
   *
   * @param namedQueueRecorder instance of {@link NamedQueueRecorder}
   */
  void setNamedQueueRecorder(final NamedQueueRecorder namedQueueRecorder);

}
