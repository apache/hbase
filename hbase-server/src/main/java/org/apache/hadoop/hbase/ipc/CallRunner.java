package org.apache.hadoop.hbase.ipc;
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
import java.nio.channels.ClosedChannelException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.RpcServer.Call;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.TraceScope;

import com.google.protobuf.Message;

/**
 * The request processing logic, which is usually executed in thread pools provided by an
 * {@link RpcScheduler}.  Call {@link #run()} to actually execute the contained
 * {@link RpcServer.Call}
 */
@InterfaceAudience.Private
public class CallRunner {
  private Call call;
  private RpcServerInterface rpcServer;
  private MonitoredRPCHandler status;
  private UserProvider userProvider;

  /**
   * On construction, adds the size of this call to the running count of outstanding call sizes.
   * Presumption is that we are put on a queue while we wait on an executor to run us.  During this
   * time we occupy heap.
   * @param call The call to run.
   * @param rpcServer
   */
  // The constructor is shutdown so only RpcServer in this class can make one of these.
  CallRunner(final RpcServerInterface rpcServer, final Call call, UserProvider userProvider) {
    this.call = call;
    this.rpcServer = rpcServer;
    // Add size of the call to queue size.
    this.rpcServer.addCallSize(call.getSize());
    this.status = getStatus();
    this.userProvider = userProvider;
  }

  public Call getCall() {
    return call;
  }

  /**
   * Cleanup after ourselves... let go of references.
   */
  private void cleanup() {
    this.call = null;
    this.rpcServer = null;
    this.status = null;
    this.userProvider = null;
  }

  public void run() {
    try {
      if (!call.connection.channel.isOpen()) {
        if (RpcServer.LOG.isDebugEnabled()) {
          RpcServer.LOG.debug(Thread.currentThread().getName() + ": skipped " + call);
        }
        return;
      }
      this.status.setStatus("Setting up call");
      this.status.setConnection(call.connection.getHostAddress(), call.connection.getRemotePort());
      if (RpcServer.LOG.isDebugEnabled()) {
        UserGroupInformation remoteUser = call.connection.user;
        RpcServer.LOG.debug(call.toShortString() + " executing as " +
            ((remoteUser == null) ? "NULL principal" : remoteUser.getUserName()));
      }
      Throwable errorThrowable = null;
      String error = null;
      Pair<Message, CellScanner> resultPair = null;
      RpcServer.CurCall.set(call);
      TraceScope traceScope = null;
      try {
        if (!this.rpcServer.isStarted()) {
          throw new ServerNotRunningYetException("Server is not running yet");
        }
        if (call.tinfo != null) {
          traceScope = Trace.startSpan(call.toTraceString(), call.tinfo);
        }
        RequestContext.set(userProvider.create(call.connection.user), RpcServer.getRemoteIp(),
          call.connection.service);
        // make the call
        resultPair = this.rpcServer.call(call.service, call.md, call.param, call.cellScanner,
          call.timestamp, this.status);
      } catch (Throwable e) {
        RpcServer.LOG.debug(Thread.currentThread().getName() + ": " + call.toShortString(), e);
        errorThrowable = e;
        error = StringUtils.stringifyException(e);
      } finally {
        if (traceScope != null) {
          traceScope.close();
        }
        // Must always clear the request context to avoid leaking
        // credentials between requests.
        RequestContext.clear();
      }
      RpcServer.CurCall.set(null);
      // Set the response for undelayed calls and delayed calls with
      // undelayed responses.
      if (!call.isDelayed() || !call.isReturnValueDelayed()) {
        Message param = resultPair != null ? resultPair.getFirst() : null;
        CellScanner cells = resultPair != null ? resultPair.getSecond() : null;
        call.setResponse(param, cells, errorThrowable, error);
      }
      call.sendResponseIfReady();
      this.status.markComplete("Sent response");
      this.status.pause("Waiting for a call");
    } catch (OutOfMemoryError e) {
      if (this.rpcServer.getErrorHandler() != null) {
        if (this.rpcServer.getErrorHandler().checkOOME(e)) {
          RpcServer.LOG.info(Thread.currentThread().getName() + ": exiting on OutOfMemoryError");
          return;
        }
      } else {
        // rethrow if no handler
        throw e;
      }
    } catch (ClosedChannelException cce) {
      RpcServer.LOG.warn(Thread.currentThread().getName() + ": caught a ClosedChannelException, " +
          "this means that the server was processing a " +
          "request but the client went away. The error message was: " +
          cce.getMessage());
    } catch (Exception e) {
      RpcServer.LOG.warn(Thread.currentThread().getName()
          + ": caught: " + StringUtils.stringifyException(e));
    } finally {
      // regardless if succesful or not we need to reset the callQueueSize
      this.rpcServer.addCallSize(call.getSize() * -1);
      cleanup();
    }
  }

  MonitoredRPCHandler getStatus() {
    // It is ugly the way we park status up in RpcServer.  Let it be for now.  TODO.
    MonitoredRPCHandler status = RpcServer.MONITORED_RPC.get();
    if (status != null) {
      return status;
    }
    status = TaskMonitor.get().createRPCStatus(Thread.currentThread().getName());
    status.pause("Waiting for a call");
    RpcServer.MONITORED_RPC.set(status);
    return status;
  }
}
