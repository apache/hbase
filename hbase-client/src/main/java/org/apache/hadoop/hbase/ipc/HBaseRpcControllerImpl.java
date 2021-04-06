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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Get instances via {@link RpcControllerFactory} on client-side.
 * @see RpcControllerFactory
 */
@InterfaceAudience.Private
public class HBaseRpcControllerImpl implements HBaseRpcController {
  /**
   * The time, in ms before the call should expire.
   */
  private Integer callTimeout;

  private boolean done = false;

  private boolean cancelled = false;

  private final List<RpcCallback<Object>> cancellationCbs = new ArrayList<>();

  private IOException exception;

  /**
   * Rpc target Region's RegionInfo we are going against. May be null.
   * @see #hasRegionInfo()
   */
  private RegionInfo regionInfo;

  /**
   * Priority to set on this request. Set it here in controller so available composing the request.
   * This is the ordained way of setting priorities going forward. We will be undoing the old
   * annotation-based mechanism.
   */
  private int priority = HConstants.PRIORITY_UNSET;

  /**
   * They are optionally set on construction, cleared after we make the call, and then optionally
   * set on response with the result. We use this lowest common denominator access to Cells because
   * sometimes the scanner is backed by a List of Cells and other times, it is backed by an encoded
   * block that implements CellScanner.
   */
  private CellScanner cellScanner;

  public HBaseRpcControllerImpl() {
    this(null, (CellScanner) null);
  }

  /**
   * Used server-side. Clients should go via {@link RpcControllerFactory}
   */
  public HBaseRpcControllerImpl(final CellScanner cellScanner) {
    this(null, cellScanner);
  }

  HBaseRpcControllerImpl(RegionInfo regionInfo, final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
    this.regionInfo = regionInfo;
  }

  HBaseRpcControllerImpl(RegionInfo regionInfo, final List<CellScannable> cellIterables) {
    this.cellScanner = cellIterables == null ? null : CellUtil.createCellScanner(cellIterables);
    this.regionInfo = null;
  }

  @Override
  public boolean hasRegionInfo() {
    return this.regionInfo != null;
  }

  @Override
  public RegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  /**
   * @return One-shot cell scanner (you cannot back it up and restart)
   */
  @Override
  public CellScanner cellScanner() {
    return cellScanner;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "The only possible race method is startCancel")
  @Override
  public void setCellScanner(final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  @Override
  public void setPriority(int priority) {
    this.priority = Math.max(this.priority, priority);

  }

  @Override
  public void setPriority(final TableName tn) {
    setPriority(
      tn != null && tn.isSystemTable() ? HConstants.SYSTEMTABLE_QOS : HConstants.NORMAL_QOS);
  }

  @Override
  public int getPriority() {
    return priority < 0 ? HConstants.NORMAL_QOS : priority;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "The only possible race method is startCancel")
  @Override
  public void reset() {
    priority = 0;
    cellScanner = null;
    exception = null;
    callTimeout = null;
    regionInfo = null;
    // In the implementations of some callable with replicas, rpc calls are executed in a executor
    // and we could cancel the operation from outside which means there could be a race between
    // reset and startCancel. Although I think the race should be handled by the callable since the
    // reset may clear the cancel state...
    synchronized (this) {
      done = false;
      cancelled = false;
      cancellationCbs.clear();
    }
  }

  @Override
  public int getCallTimeout() {
    return callTimeout != null? callTimeout: 0;
  }

  @Override
  public void setCallTimeout(int callTimeout) {
    this.callTimeout = callTimeout;
  }

  @Override
  public boolean hasCallTimeout() {
    return callTimeout != null;
  }

  @Override
  public synchronized String errorText() {
    if (!done || exception == null) {
      return null;
    }
    return exception.getMessage();
  }

  @Override
  public synchronized boolean failed() {
    return done && this.exception != null;
  }

  @Override
  public synchronized boolean isCanceled() {
    return cancelled;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> callback) {
    synchronized (this) {
      if (done) {
        return;
      }
      if (!cancelled) {
        cancellationCbs.add(callback);
        return;
      }
    }
    // run it directly as we have already been cancelled.
    callback.run(null);
  }

  @Override
  public synchronized void setFailed(String reason) {
    if (done) {
      return;
    }
    done = true;
    exception = new IOException(reason);
  }

  @Override
  public synchronized void setFailed(IOException e) {
    if (done) {
      return;
    }
    done = true;
    exception = e;
  }

  @Override
  public synchronized IOException getFailed() {
    return done ? exception : null;
  }

  @Override
  public synchronized void setDone(CellScanner cellScanner) {
    if (done) {
      return;
    }
    done = true;
    this.cellScanner = cellScanner;
  }

  @Override
  public void startCancel() {
    // As said above in the comment of reset, the cancellationCbs maybe cleared by reset, so we need
    // to copy it.
    List<RpcCallback<Object>> cbs;
    synchronized (this) {
      if (done) {
        return;
      }
      done = true;
      cancelled = true;
      cbs = new ArrayList<>(cancellationCbs);
    }
    for (RpcCallback<?> cb : cbs) {
      cb.run(null);
    }
  }

  @Override
  public synchronized void notifyOnCancel(RpcCallback<Object> callback, CancellationCallback action)
      throws IOException {
    if (cancelled) {
      action.run(true);
    } else {
      cancellationCbs.add(callback);
      action.run(false);
    }
  }
}
