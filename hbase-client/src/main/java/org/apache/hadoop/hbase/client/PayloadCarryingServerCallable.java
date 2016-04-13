/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

import java.io.IOException;

/**
 * This class is used to unify HTable calls with AsyncProcess Framework.
 * HTable can use AsyncProcess directly though this class.
 */
@InterfaceAudience.Private
public abstract class PayloadCarryingServerCallable<T>
    extends RegionServerCallable<T> {
  protected PayloadCarryingRpcController controller;

  public PayloadCarryingServerCallable(HConnection connection, TableName tableName, byte[] row,
    RpcControllerFactory rpcControllerFactory) {
    super(connection, tableName, row);
    this.controller = rpcControllerFactory.newController();
  }

  public void cancel() {
    controller.startCancel();
  }

  public boolean isCancelled() {
    return controller.isCanceled();
  }
}
