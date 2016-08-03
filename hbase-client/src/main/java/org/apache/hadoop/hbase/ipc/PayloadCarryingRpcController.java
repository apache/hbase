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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

/**
 * Optionally carries Cells across the proxy/service interface down into ipc. On its
 * way out it optionally carries a set of result Cell data. We stick the Cells here when we want
 * to avoid having to protobuf them (for performance reasons). This class is used ferrying data
 * across the proxy/protobuf service chasm. Also does call timeout. Used by client and server
 * ipc'ing.
 */
@InterfaceAudience.Private
public class PayloadCarryingRpcController implements RpcController, CellScannable {
  /**
   * The time, in ms before the call should expire.
   */
  protected volatile Integer callTimeout;
  protected volatile boolean cancelled = false;
  protected final AtomicReference<RpcCallback<Object>> cancellationCb = new AtomicReference<>(null);
  protected final AtomicReference<RpcCallback<IOException>> failureCb = new AtomicReference<>(null);
  private IOException exception;

  public static final int PRIORITY_UNSET = -1;
  /**
   * Priority to set on this request.  Set it here in controller so available composing the
   * request.  This is the ordained way of setting priorities going forward.  We will be
   * undoing the old annotation-based mechanism.
   */
  private int priority = PRIORITY_UNSET;

  /**
   * They are optionally set on construction, cleared after we make the call, and then optionally
   * set on response with the result. We use this lowest common denominator access to Cells because
   * sometimes the scanner is backed by a List of Cells and other times, it is backed by an
   * encoded block that implements CellScanner.
   */
  private CellScanner cellScanner;

  public PayloadCarryingRpcController() {
    this((CellScanner)null);
  }

  public PayloadCarryingRpcController(final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  public PayloadCarryingRpcController(final List<CellScannable> cellIterables) {
    this.cellScanner = cellIterables == null? null: CellUtil.createCellScanner(cellIterables);
  }

  /**
   * @return One-shot cell scanner (you cannot back it up and restart)
   */
  @Override
  public CellScanner cellScanner() {
    return cellScanner;
  }

  public void setCellScanner(final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  /**
   * @param priority Priority for this request; should fall roughly in the range
   * {@link HConstants#NORMAL_QOS} to {@link HConstants#HIGH_QOS}
   */
  public void setPriority(int priority) {
    this.priority = priority;
  }

  /**
   * @param tn Set priority based off the table we are going against.
   */
  public void setPriority(final TableName tn) {
    this.priority =
        (tn != null && tn.isSystemTable())? HConstants.SYSTEMTABLE_QOS: HConstants.NORMAL_QOS;
  }

  /**
   * @param regionName RegionName. If hbase:meta, we'll set high priority.
   */
  public void setPriority(final byte [] regionName) {
    if (isMetaRegion(regionName)) {
      setPriority(TableName.META_TABLE_NAME);
    }
  }

  private static boolean isMetaRegion(final byte[] regionName) {
    return Bytes.equals(regionName, HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
        || Bytes.equals(regionName, HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes());
  }

  /**
   * @return The priority of this request
   */
  public int getPriority() {
    return priority;
  }

  @Override
  public void reset() {
    priority = 0;
    cellScanner = null;
    exception = null;
    cancelled = false;
    failureCb.set(null);
    cancellationCb.set(null);
    callTimeout = null;
  }

  public int getCallTimeout() {
    if (callTimeout != null) {
      return callTimeout;
    } else {
      return 0;
    }
  }

  public void setCallTimeout(int callTimeout) {
    this.callTimeout = callTimeout;
  }

  public boolean hasCallTimeout(){
    return callTimeout != null;
  }

  @Override
  public String errorText() {
    if (exception != null) {
      return exception.getMessage();
    } else {
      return null;
    }
  }

  /**
   * For use in async rpc clients
   * @return true if failed
   */
  @Override
  public boolean failed() {
    return this.exception != null;
  }

  @Override
  public boolean isCanceled() {
    return cancelled;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> cancellationCb) {
    this.cancellationCb.set(cancellationCb);
    if (this.cancelled) {
      cancellationCb.run(null);
    }
  }

  /**
   * Notify a callback on error.
   * For use in async rpc clients
   *
   * @param failureCb the callback to call on error
   */
  public void notifyOnFail(RpcCallback<IOException> failureCb) {
    this.failureCb.set(failureCb);
    if (this.exception != null) {
      failureCb.run(this.exception);
    }
  }

  @Override
  public void setFailed(String reason) {
    this.exception = new IOException(reason);
    if (this.failureCb.get() != null) {
      this.failureCb.get().run(this.exception);
    }
  }

  /**
   * Set failed with an exception to pass on.
   * For use in async rpc clients
   *
   * @param e exception to set with
   */
  public void setFailed(IOException e) {
    this.exception = e;
    if (this.failureCb.get() != null) {
      this.failureCb.get().run(this.exception);
    }
  }

  @Override
  public void startCancel() {
    cancelled = true;
    if (cancellationCb.get() != null) {
      cancellationCb.get().run(null);
    }
  }
}