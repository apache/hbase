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
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.RpcServer.Call;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.protobuf.Message;

/**
 * The request processing logic, which is usually executed in thread pools provided by an
 * {@link RpcScheduler}.  Call {@link #run()} to actually execute the contained
 * RpcServer.Call
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class CallRunner {
  private static final Log LOG = LogFactory.getLog(CallRunner.class);

  private static final CallDroppedException CALL_DROPPED_EXCEPTION
    = new CallDroppedException();

  private Call call;
  private RpcServerInterface rpcServer;
  private MonitoredRPCHandler status;
  private volatile boolean sucessful;

  /**
   * On construction, adds the size of this call to the running count of outstanding call sizes.
   * Presumption is that we are put on a queue while we wait on an executor to run us.  During this
   * time we occupy heap.
   */
  // The constructor is shutdown so only RpcServer in this class can make one of these.
  CallRunner(final RpcServerInterface rpcServer, final Call call) {
    this.call = call;
    this.rpcServer = rpcServer;
    // Add size of the call to queue size.
    if (call != null && rpcServer != null) {
      this.rpcServer.addCallSize(call.getSize());
    }
  }

  public Call getCall() {
    return call;
  }

  public void setStatus(MonitoredRPCHandler status) {
    this.status = status;
  }

  /**
   * Cleanup after ourselves... let go of references.
   */
  private void cleanup() {
    this.call = null;
    this.rpcServer = null;
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
      if (RpcServer.LOG.isTraceEnabled()) {
        UserGroupInformation remoteUser = call.connection.ugi;
        RpcServer.LOG.trace(call.toShortString() + " executing as " +
            ((remoteUser == null) ? "NULL principal" : remoteUser.getUserName()));
      }
      Throwable errorThrowable = null;
      String error = null;
      Pair<Message, CellScanner> resultPair = null;
      RpcServer.CurCall.set(call);
      TraceScope traceScope = null;
      try {
        if (!this.rpcServer.isStarted()) {
          InetSocketAddress address = rpcServer.getListenerAddress();
          throw new ServerNotRunningYetException("Server " +
              (address != null ? address : "(channel closed)") + " is not running yet");
        }
        if (call.tinfo != null) {
          traceScope = Trace.startSpan(call.toTraceString(), call.tinfo);
        }
        // make the call
        resultPair = this.rpcServer.call(call.service, call.md, call.param, call.cellScanner,
          call.timestamp, this.status, call.timeout);
      } catch (Throwable e) {
        RpcServer.LOG.debug(Thread.currentThread().getName() + ": " + call.toShortString(), e);
        errorThrowable = e;
        error = StringUtils.stringifyException(e);
        if (e instanceof Error) {
          throw (Error)e;
        }
      } finally {
        if (traceScope != null) {
          traceScope.close();
        }
        RpcServer.CurCall.set(null);
        if (resultPair != null) {
          this.rpcServer.addCallSize(call.getSize() * -1);
          sucessful = true;
        }
      }
      // Set the response
      Message param = resultPair != null ? resultPair.getFirst() : null;
      CellScanner cells = resultPair != null ? resultPair.getSecond() : null;
      call.setResponse(param, cells, errorThrowable, error);
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
      InetSocketAddress address = rpcServer.getListenerAddress();
      RpcServer.LOG.warn(Thread.currentThread().getName() + ": caught a ClosedChannelException, " +
          "this means that the server " + (address != null ? address : "(channel closed)") +
          " was processing a request but the client went away. The error message was: " +
          cce.getMessage());
    } catch (Exception e) {
      RpcServer.LOG.warn(Thread.currentThread().getName()
          + ": caught: " + StringUtils.stringifyException(e));
    } finally {
      if (!sucessful) {
        this.rpcServer.addCallSize(call.getSize() * -1);
      }
      cleanup();
    }
  }

  /**
   * When we want to drop this call because of server is overloaded.
   */
  public void drop() {
    try {
      if (!call.connection.channel.isOpen()) {
        if (RpcServer.LOG.isDebugEnabled()) {
          RpcServer.LOG.debug(Thread.currentThread().getName() + ": skipped " + call);
        }
        return;
      }

      // Set the response
      InetSocketAddress address = rpcServer.getListenerAddress();
      call.setResponse(null, null, CALL_DROPPED_EXCEPTION, "Call dropped, server "
        + (address != null ? address : "(channel closed)") + " is overloaded, please retry.");
      call.sendResponseIfReady();
    } catch (ClosedChannelException cce) {
      InetSocketAddress address = rpcServer.getListenerAddress();
      RpcServer.LOG.warn(Thread.currentThread().getName() + ": caught a ClosedChannelException, " +
        "this means that the server " + (address != null ? address : "(channel closed)") +
        " was processing a request but the client went away. The error message was: " +
        cce.getMessage());
    } catch (Exception e) {
      RpcServer.LOG.warn(Thread.currentThread().getName()
        + ": caught: " + StringUtils.stringifyException(e));
    } finally {
      if (!sucessful) {
        this.rpcServer.addCallSize(call.getSize() * -1);
      }
      cleanup();
    }
  }
}
