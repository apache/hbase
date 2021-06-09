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
package org.apache.hadoop.hbase.ipc;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;

import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.TraceScope;

/**
 * The request processing logic, which is usually executed in thread pools provided by an
 * {@link RpcScheduler}.  Call {@link #run()} to actually execute the contained
 * RpcServer.Call
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class CallRunner {

  private static final CallDroppedException CALL_DROPPED_EXCEPTION
    = new CallDroppedException();

  private RpcCall call;
  private RpcServerInterface rpcServer;
  private MonitoredRPCHandler status;
  private volatile boolean sucessful;

  /**
   * On construction, adds the size of this call to the running count of outstanding call sizes.
   * Presumption is that we are put on a queue while we wait on an executor to run us.  During this
   * time we occupy heap.
   */
  // The constructor is shutdown so only RpcServer in this class can make one of these.
  CallRunner(final RpcServerInterface rpcServer, final RpcCall call) {
    this.call = call;
    this.rpcServer = rpcServer;
    // Add size of the call to queue size.
    if (call != null && rpcServer != null) {
      this.rpcServer.addCallSize(call.getSize());
    }
  }

  public RpcCall getRpcCall() {
    return call;
  }

  /**
   * Keep for backward compatibility.
   * @deprecated As of release 2.0, this will be removed in HBase 3.0
   */
  @Deprecated
  public ServerCall<?> getCall() {
    return (ServerCall<?>) call;
  }

  public void setStatus(MonitoredRPCHandler status) {
    this.status = status;
  }

  /**
   * Cleanup after ourselves... let go of references.
   */
  private void cleanup() {
    this.call.cleanup();
    this.call = null;
    this.rpcServer = null;
  }

  public void run() {
    try {
      if (call.disconnectSince() >= 0) {
        if (RpcServer.LOG.isDebugEnabled()) {
          RpcServer.LOG.debug(Thread.currentThread().getName() + ": skipped " + call);
        }
        return;
      }
      call.setStartTime(System.currentTimeMillis());
      if (call.getStartTime() > call.getDeadline()) {
        RpcServer.LOG.warn("Dropping timed out call: " + call);
        return;
      }
      this.status.setStatus("Setting up call");
      this.status.setConnection(call.getRemoteAddress().getHostAddress(), call.getRemotePort());
      if (RpcServer.LOG.isTraceEnabled()) {
        Optional<User> remoteUser = call.getRequestUser();
        RpcServer.LOG.trace(call.toShortString() + " executing as " +
            (remoteUser.isPresent() ? remoteUser.get().getName() : "NULL principal"));
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
        String serviceName =
            call.getService() != null ? call.getService().getDescriptorForType().getName() : "";
        String methodName = (call.getMethod() != null) ? call.getMethod().getName() : "";
        String traceString = serviceName + "." + methodName;
        traceScope = TraceUtil.createTrace(traceString);
        // make the call
        resultPair = this.rpcServer.call(call, this.status);
      } catch (TimeoutIOException e){
        RpcServer.LOG.warn("Can not complete this request in time, drop it: " + call);
        return;
      } catch (Throwable e) {
        if (e instanceof ServerNotRunningYetException) {
          // If ServerNotRunningYetException, don't spew stack trace.
          if (RpcServer.LOG.isTraceEnabled()) {
            RpcServer.LOG.trace(call.toShortString(), e);
          }
        } else {
          // Don't dump full exception.. just String version
          RpcServer.LOG.debug(call.toShortString() + ", exception=" + e);
        }
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
      this.status.markComplete("To send response");
      // return back the RPC request read BB we can do here. It is done by now.
      call.cleanup();
      // Set the response
      Message param = resultPair != null ? resultPair.getFirst() : null;
      CellScanner cells = resultPair != null ? resultPair.getSecond() : null;
      call.setResponse(param, cells, errorThrowable, error);
      call.sendResponseIfReady();
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

      if (this.status.isRPCRunning()) {
        this.status.markComplete("Call error");
      }
      this.status.pause("Waiting for a call");
      cleanup();
    }
  }

  /**
   * When we want to drop this call because of server is overloaded.
   */
  public void drop() {
    try {
      if (call.disconnectSince() >= 0) {
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
