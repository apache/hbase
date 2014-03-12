/**
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.ipc.thrift;

import com.facebook.nifty.core.NiftyRequestContext;
import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.header.transport.THeaderTransport;
import com.facebook.swift.service.ThriftEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.ThriftHRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.thrift.transport.TTransport;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the handler class which has callbacks for different stages of the
 * RPC call processing, pre-read, post-read, pre-write, post-Write etc.
 *
 * We will use it to calculate the processing time of calls.
 *
 * This class is similar to the ThriftServiceStatsHandler class, however, it
 * maintains an internal storage of aggregated stats, and we cannot generate
 * warnings based on the processing time, response size etc. because the
 * PerCallMethodStats class is private.
 */
public class ThriftCallStatsReporter extends ThriftEventHandler {
  static final Log LOG = LogFactory.getLog(ThriftCallStatsReporter.class);

  HBaseRpcMetrics rpcMetrics;

  private static final String WARN_RESPONSE_TIME =
    "hbase.ipc.warn.response.time";
  private static final String WARN_RESPONSE_SIZE =
    "hbase.ipc.warn.response.size";

  /** Default value for above params */
  private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  private final int warnResponseTime;
  private final int warnResponseSize;

  Object serverInstance;

  private boolean useHeaderProtocol;

  public ThriftCallStatsReporter(Configuration conf,
                                 HBaseRpcMetrics rpcMetrics,
                                 Object serverInstance) {
    this.rpcMetrics = rpcMetrics;
    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME,
      DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE,
      DEFAULT_WARN_RESPONSE_SIZE);
    this.serverInstance = serverInstance;
    this.useHeaderProtocol = conf.getBoolean(HConstants.USE_HEADER_PROTOCOL,
      HConstants.DEFAULT_USE_HEADER_PROTOCOL);
  }

  /**
   * This class keeps track of the RPC call's stats, i.e. the name of the
   * method, when we started processing it, when we were done, etc.
   */
  public static class ThriftCallStats {
    private RequestContext requestContext;
    private String methodName;
    private long preReadTimeMs;
    private long postWriteTimeMs;
    private Object[] callArgs;
    private long requestSize;
    private long responseSize;
    private boolean success = true;

    ThriftCallStats(String methodName, RequestContext requestContext) {
      // Swift encodes the method name as <ClassName/>.<MethodName/>
      // However, we only want the method name.
      String[] splits = methodName.split("\\.");
      this.methodName = splits[splits.length - 1];
      this.requestContext = requestContext;
    }

    public String getMethodName() {
      return this.methodName;
    }

    public RequestContext getRequestContext() {
      return this.requestContext;
    }

    public void setPreReadTimeMs(long preReadTimeMs) {
      this.preReadTimeMs = preReadTimeMs;
    }

    public long getPreReadTimeMs() {
      return this.preReadTimeMs;
    }

    public void setPostWriteTimeMs(long postWriteTimeMs) {
      this.postWriteTimeMs = postWriteTimeMs;
    }

    public long getPostWriteTimeMs() {
      return this.postWriteTimeMs;
    }

    public void setCallArgs(Object[] callArgs) {
      this.callArgs = callArgs;
    }

    public Object[] getCallArgs() {
      return this.callArgs;
    }

    public long getRequestSize() {
      return this.requestSize;
    }

    public long getResponseSize() {
      return this.responseSize;
    }

    public long getProcessingTimeMs() {
      return Math.max(this.postWriteTimeMs - this.preReadTimeMs, 0);
    }

    public long getStartTimeMs() {
      return getPreReadTimeMs();
    }

    // TODO @gauravm
    public long getQueueTimeMs() {
      return 0;
    }

    public String getClientAddress() {
      return this.requestContext.getRemoteAddress().toString();
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }

    public boolean getSuccess() {
      return this.success;
    }

    public void setRequestSize(int requestSize) {
      this.requestSize = requestSize;
    }

    public void setResponseSize(int responseSize) {
      this.responseSize = responseSize;
    }

    @Override
    public String toString() {
      return "ThriftCallStats [requestContext=" + requestContext
          + ", methodName=" + methodName + ", preReadTimeMs=" + preReadTimeMs
          + ", postWriteTimeMs=" + postWriteTimeMs + ", callArgs="
          + Arrays.toString(callArgs) + ", requestSize=" + requestSize
          + ", responseSize=" + responseSize + ", success=" + success + "]";
    }
  }

  /**
   * This method creates a new ThriftCallStats object for every RPC call.
   * @param methodName
   * @param requestContext
   * @return
   */
  @Override
  public Object getContext(String methodName, RequestContext requestContext) {
    return new ThriftCallStats(methodName, requestContext);
  }

  /**
   *
   * @param context
   * @param methodName
   */
  @Override
  public void preRead(Object context, String methodName) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    ctx.setPreReadTimeMs(EnvironmentEdgeManager.currentTimeMillis());
  }

  /**
   * Extract info from header
   */
  @Override
  public void postRead(Object context, String methodName, Object[] args) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    ctx.setCallArgs(args);
    ctx.setRequestSize(getBytesRead(ctx));
    if (useHeaderProtocol) {
      if (ctx.requestContext instanceof NiftyRequestContext) {
        Call call = getCallInfoFromClient(ctx.requestContext);
        if (HRegionServer.enableServerSideProfilingForAllCalls.get()
            || (call != null && call.isShouldProfile())) {
          // it is possible that call is null - if profiling is enabled only on
          // serverside
          if (call == null) {
            call = new Call(HBaseRPCOptions.DEFAULT);
          }
          call.setShouldProfile(true);
          call.setProfilingData(new ProfilingData());
        } else if (call != null) {
          // call is not null but profiling is not enabled, so set profiling
          // data to null
          call.setProfilingData(null);
        }
        HRegionServer.callContext.set(call);
      } else {
        LOG.error("Request Context was not THeaderTransport, server cannot read headers");
      }
    }
  }

  /**
   * Send header info to the client (used for profiling data) TODO: (adela)
   * Check if the header needs to be sent every time or only when profiling is
   * required
   *
   * @param context
   * @param methodName
   * @param result
   */
  @Override
  public void preWrite(Object context, String methodName, Object result) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    ctx.setPostWriteTimeMs(System.currentTimeMillis());
    ctx.setResponseSize(getBytesWritten(ctx));
    if (useHeaderProtocol) {
      try {
        Call call = HRegionServer.callContext.get();
        HRegionServer.callContext.remove();
        if (call!= null && call.isShouldProfile()) {
          sendCallInfoToClient(call, ctx.requestContext);
        }
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Exception during sending header happened");
      }
    }
  }

  /**
   *
   * @param context
   * @param methodName
   * @param result
   */
  @Override
  public void postWrite(Object context, String methodName, Object result) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    ctx.setPostWriteTimeMs(System.currentTimeMillis());
    ctx.setResponseSize(getBytesWritten(ctx));
  }

  private int getBytesRead(ThriftCallStats ctx) {
    if (!(ctx.requestContext instanceof NiftyRequestContext)) {
      return 0;
    }
    NiftyRequestContext requestContext = (NiftyRequestContext) ctx.requestContext;
    return requestContext.getNiftyTransport().getReadByteCount();
  }

  public void preReadException(Object context, String methodName, Throwable t) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    ctx.setSuccess(false);
    LOG.error("RPC call " + methodName + " failed before read.");
    t.printStackTrace();
  }

  public void postReadException(Object context, String methodName, Throwable t) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    ctx.setSuccess(false);
    LOG.error("RPC call " + methodName + " failed after read.");
    t.printStackTrace();
  }

  public void done(Object context, String methodName) {
    ThriftCallStats ctx = (ThriftCallStats) context;
    if (ctx.getSuccess()) {
      int processingTime = (int) (ctx.getProcessingTimeMs());
      rpcMetrics.rpcProcessingTime.inc(processingTime);
      rpcMetrics.inc(ctx.getMethodName(), processingTime);

      long responseSize = ctx.getResponseSize();
      boolean tooSlow = (processingTime > warnResponseTime
        && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize
        && warnResponseSize > -1);
      if (tooSlow || tooLarge) {
        try {
          logResponse(ctx, (tooLarge ? "TooLarge" : "TooSlow"));
        } catch (IOException e) {
          LOG.info("(operation" + (tooLarge ? "TooLarge" : "TooSlow") + "): " +
                   ctx.getMethodName() + ". Error while logging: " + e);
        }
        if (tooSlow) {
          // increment global slow RPC response counter
          rpcMetrics.inc("slowResponse.", processingTime);
          HRegion.incrNumericPersistentMetric("slowResponse.all.cumulative", 1);
          HRegion.incrNumericPersistentMetric("slowResponse."
            + ctx.getMethodName() + ".cumulative", 1);
        }
        if (tooLarge) {
          // increment global slow RPC response counter
          rpcMetrics.inc("largeResponse.kb.", (int)(responseSize/1024));
          HRegion.incrNumericPersistentMetric("largeResponse.all.cumulative", 1);
          HRegion.incrNumericPersistentMetric("largeResponse."
            + ctx.getMethodName() + ".cumulative", 1);
        }
      }
      if (processingTime > 1000) {
        // we use a hard-coded one second period so that we can clearly
        // indicate the time period we're warning about in the name of the
        // metric itself
        rpcMetrics.inc(ctx.getMethodName() + ".aboveOneSec.",
          processingTime);
        HRegion.incrNumericPersistentMetric(ctx.getMethodName() +
          ".aboveOneSec.cumulative", 1);
      }
    }
  }
  /**
   * Write to the log if an operation is too slow or too large.
   *
   * @param ctx Context for the thrift call
   * @param tag "tooSlow", or "tooLarge"?
   * @throws IOException
   */
  private void logResponse(ThriftCallStats ctx, String tag)
    throws IOException {
    Object params[] = ctx.getCallArgs();
    // for JSON encoding
    ObjectMapper mapper = new ObjectMapper();
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<String, Object>();
    responseInfo.put("starttimems", ctx.getStartTimeMs());
    responseInfo.put("processingtimems", ctx.getProcessingTimeMs());
    responseInfo.put("queuetimems", ctx.getQueueTimeMs());
    responseInfo.put("responsesize", ctx.getResponseSize());
    responseInfo.put("client", ctx.getClientAddress());
    responseInfo.put("class", serverInstance.getClass().getSimpleName());
    responseInfo.put("method", ctx.getMethodName());

    if (params.length == 2 && serverInstance instanceof ThriftHRegionServer &&
      params[0] instanceof byte[] &&
      params[1] instanceof Operation) {
      // if the slow process is a query, we want to log its table as well
      // as its own fingerprint
      byte [] tableName =
        HRegionInfo.parseRegionName((byte[]) params[0])[0];
      responseInfo.put("table", Bytes.toStringBinary(tableName));
      // annotate the response map with operation details
      responseInfo.putAll(((Operation) params[1]).toMap());
      // report to the log file
      LOG.warn("(operation" + tag + "): " +
        mapper.writeValueAsString(responseInfo));
    } else if (params.length == 1 && serverInstance instanceof ThriftHRegionServer &&
      params[0] instanceof Operation) {
      // annotate the response map with operation details
      responseInfo.putAll(((Operation) params[0]).toMap());
      // report to the log file
      LOG.warn("(operation" + tag + "): " +
        mapper.writeValueAsString(responseInfo));
    } else {
      // can't get JSON details, so just report call.toString() along with
      // a more generic tag.
      // responseInfo.put("call", call.toString());
      LOG.warn("(response" + tag + "): " +
        mapper.writeValueAsString(responseInfo));
    }
  }

  private int getBytesWritten(ThriftCallStats ctx) {
    if (!(ctx.requestContext instanceof NiftyRequestContext)) {
      // Standard TTransport interface doesn't give us a way to determine how many bytes
      // were read
      return 0;
    }

    NiftyRequestContext requestContext = (NiftyRequestContext) ctx.requestContext;
    return requestContext.getNiftyTransport().getWrittenByteCount();
  }

  /**
   * Send information about the call to the client
   *
   * @param call
   * @param requestContext
   * @throws Exception
   */
  private void sendCallInfoToClient(Call call, RequestContext requestContext)
      throws Exception {
    if (call != null) {
      TTransport inputTransport = requestContext.getInputProtocol()
          .getTransport();
      TTransport outputTransport = requestContext.getOutputProtocol()
          .getTransport();
      if (outputTransport instanceof THeaderTransport) {
        THeaderTransport headerTransport = (THeaderTransport) inputTransport;
        String dataString = Bytes
            .writeThriftBytesAndGetString(call, Call.class);
        headerTransport.setHeader(HConstants.THRIFT_HEADER_FROM_SERVER, dataString);
      } else {
        LOG.error("input transport was not THeaderTransport, client cannot read headers");
      }
    }
  }

  /**
   * Construct the Call object from the information that client gave us
   *
   * @param requestContext
   * @return
   */
  private Call getCallInfoFromClient(RequestContext requestContext) {
    TTransport inputTransport = requestContext.getInputProtocol()
        .getTransport();
    if (inputTransport instanceof THeaderTransport) {
      THeaderTransport headerTransport = (THeaderTransport) inputTransport;
      String dataString = headerTransport.getReadHeaders().get(
          HConstants.THRIFT_HEADER_FROM_CLIENT);
      if (dataString != null) {
        byte[] dataBytes = Bytes.hexToBytes(dataString);
        try {
          Call call = Bytes.readThriftBytes(dataBytes, Call.class);
          return call;
        } catch (Exception e) {
          e.printStackTrace();
          LOG.error("Deserialization of the call header didn't succeed.");
        }
      }
    } else {
      LOG.error("Input transport is not instance of THeaderTransport, but "
          + inputTransport.getClass().getName());
    }
    return null;
  }
}
