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

import java.io.IOException;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Simple delegating controller for use with the {@link RpcControllerFactory} to help override
 * standard behavior of a {@link HBaseRpcController}.
 */
@InterfaceAudience.Private
public class DelegatingHBaseRpcController implements HBaseRpcController {

  private final HBaseRpcController delegate;

  public DelegatingHBaseRpcController(HBaseRpcController delegate) {
    this.delegate = delegate;
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public boolean failed() {
    return delegate.failed();
  }

  @Override
  public String errorText() {
    return delegate.errorText();
  }

  @Override
  public void startCancel() {
    delegate.startCancel();
  }

  @Override
  public void setFailed(String reason) {
    delegate.setFailed(reason);
  }

  @Override
  public boolean isCanceled() {
    return delegate.isCanceled();
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> callback) {
    delegate.notifyOnCancel(callback);
  }

  @Override
  public CellScanner cellScanner() {
    return delegate.cellScanner();
  }

  @Override
  public void setCellScanner(CellScanner cellScanner) {
    delegate.setCellScanner(cellScanner);
  }

  @Override
  public void setPriority(int priority) {
    delegate.setPriority(priority);
  }

  @Override
  public void setPriority(TableName tn) {
    delegate.setPriority(tn);
  }

  @Override
  public int getPriority() {
    return delegate.getPriority();
  }

  @Override
  public int getCallTimeout() {
    return delegate.getCallTimeout();
  }

  @Override
  public void setCallTimeout(int callTimeout) {
    delegate.setCallTimeout(callTimeout);
  }

  @Override
  public boolean hasCallTimeout() {
    return delegate.hasCallTimeout();
  }

  @Override
  public void setDeadline(long deadline) {
    delegate.setDeadline(deadline);
  }

  @Override
  public long getDeadline() {
    return delegate.getDeadline();
  }

  @Override
  public void setFailed(IOException e) {
    delegate.setFailed(e);
  }

  @Override
  public IOException getFailed() {
    return delegate.getFailed();
  }

  @Override
  public void setDone(CellScanner cellScanner) {
    delegate.setDone(cellScanner);
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> callback, CancellationCallback action)
      throws IOException {
    delegate.notifyOnCancel(callback, action);
  }
}
