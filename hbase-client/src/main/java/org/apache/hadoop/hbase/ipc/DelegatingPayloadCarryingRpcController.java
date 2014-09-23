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

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;

/**
 * Simple delegating controller for use with the {@link RpcControllerFactory} to help override
 * standard behavior of a {@link PayloadCarryingRpcController}.
 */
public class DelegatingPayloadCarryingRpcController extends PayloadCarryingRpcController {
  private PayloadCarryingRpcController delegate;

  public DelegatingPayloadCarryingRpcController(PayloadCarryingRpcController delegate) {
    this.delegate = delegate;
  }

  @Override
  public CellScanner cellScanner() {
    return delegate.cellScanner();
  }

  @Override
  public void setCellScanner(final CellScanner cellScanner) {
    delegate.setCellScanner(cellScanner);
  }

  @Override
  public void setPriority(int priority) {
    delegate.setPriority(priority);
  }

  @Override
  public void setPriority(final TableName tn) {
    delegate.setPriority(tn);
  }

  @Override
  public int getPriority() {
    return delegate.getPriority();
  }
}
