/**
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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RSRpcServices.RegionScannersCloseCallBack;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handler to process Get Action
 */
@InterfaceAudience.Private
public class GetActionHandler extends EventHandler {
  private final GetContext context;
  private final CountDownLatch latch;
  private IOException sizeIOE;
  private Result result;
  private long processTime;

  public GetActionHandler(GetContext context, CountDownLatch latch) {
    super(null, EventType.RS_PARALLEL_GET);
    this.context = context;
    this.latch = latch;
  }

  @Override
  public void process() throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    try {
      if (context.isResponseSizeAboveQuota()) {
        sizeIOE = new MultiActionResultTooLarge("Max result size exceeded"
            + context.curCall.getResponseCellSize());
        return;
      }
      if (context.curCall != null) {
        result = context.service.get(context.get, ((HRegion) context.region),
            context.closeCallBack, context.curCall);
        context.curCall.addResultSize(result);
      } else {
        result = context.region.get(context.get);
      }
    } finally {
      latch.countDown();
      processTime = EnvironmentEdgeManager.currentTime() - before;
    }
  }

  public Result getResult() {
    return this.result;
  }

  public IOException getSizeIOException() {
    return sizeIOE;
  }

  public int getActionIndex() {
    return context.index;
  }

  public RpcCallContext getCurCall() {
    return context.curCall;
  }

  public Region getRegion() {
    return context.region;
  }

  public long getProcessTime() {
    return this.processTime;
  }

  /**
   * Context for Get Action's
   */
  public static class GetContext {
    private final RSRpcServices service;
    private final Region region;
    private final Get get;
    private final RpcCallContext curCall;
    private final RegionScannersCloseCallBack closeCallBack;
    private final long maxQuotaResultSize;
    private final int index;

    public GetContext(RSRpcServices service, Region region, Get get,
        RpcCallContext curCall, RegionScannersCloseCallBack closeCallBack,
        long maxQuotaResultSize, int index) {
      this.service = service;
      this.region = region;
      this.get = get;
      this.curCall = curCall;
      if (curCall != null) {
        curCall.incrementGetsNumber();
      }
      this.closeCallBack = closeCallBack;
      this.maxQuotaResultSize = maxQuotaResultSize;
      this.index = index;
    }

    /*
     * Consider the response size only, since the batch size limit has already been handled
     * at the begining of RSRpcServices#doNonAtomicRegionMutation.
     */
    boolean isResponseSizeAboveQuota() {
      return service.isRpcCallAboveQuota(curCall, maxQuotaResultSize, false);
    }

    public int getActionIndex() {
      return index;
    }
  }
}
