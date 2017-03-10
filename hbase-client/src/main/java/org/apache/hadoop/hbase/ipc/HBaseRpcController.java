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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import java.io.IOException;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Optionally carries Cells across the proxy/service interface down into ipc. On its way out it
 * optionally carries a set of result Cell data. We stick the Cells here when we want to avoid
 * having to protobuf them (for performance reasons). This class is used ferrying data across the
 * proxy/protobuf service chasm. Also does call timeout. Used by client and server ipc'ing.
 */
@InterfaceAudience.Private
public interface HBaseRpcController extends RpcController, CellScannable {

  static final int PRIORITY_UNSET = -1;

  /**
   * Only used to send cells to rpc server, the returned cells should be set by
   * {@link #setDone(CellScanner)}.
   */
  void setCellScanner(CellScanner cellScanner);

  /**
   * @param priority Priority for this request; should fall roughly in the range
   *          {@link HConstants#NORMAL_QOS} to {@link HConstants#HIGH_QOS}
   */
  void setPriority(int priority);

  /**
   * @param tn Set priority based off the table we are going against.
   */
  void setPriority(final TableName tn);

  /**
   * @return The priority of this request
   */
  int getPriority();

  int getCallTimeout();

  void setCallTimeout(int callTimeout);

  boolean hasCallTimeout();

  /**
   * Set failed with an exception to pass on. For use in async rpc clients
   * @param e exception to set with
   */
  void setFailed(IOException e);

  /**
   * Return the failed exception, null if not failed.
   */
  IOException getFailed();

  /**
   * <b>IMPORTANT:</b> always call this method if the call finished without any exception to tell
   * the {@code HBaseRpcController} that we are done.
   */
  void setDone(CellScanner cellScanner);

  /**
   * A little different from the basic RpcController:
   * <ol>
   * <li>You can register multiple callbacks to an {@code HBaseRpcController}.</li>
   * <li>The callback will not be called if the rpc call is finished without any cancellation.</li>
   * <li>You can call me at client side also.</li>
   * </ol>
   */
  @Override
  void notifyOnCancel(RpcCallback<Object> callback);

  interface CancellationCallback {
    void run(boolean cancelled) throws IOException;
  }

  /**
   * If not cancelled, add the callback to cancellation callback list. And then execute the action
   * with the cancellation state as a parameter. The implementation should guarantee that the
   * cancellation state does not change during this call.
   */
  void notifyOnCancel(RpcCallback<Object> callback, CancellationCallback action) throws IOException;
}