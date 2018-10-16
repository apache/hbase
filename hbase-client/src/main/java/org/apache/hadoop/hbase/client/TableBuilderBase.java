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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for all table builders.
 */
@InterfaceAudience.Private
abstract class TableBuilderBase implements TableBuilder {

  protected TableName tableName;

  protected int operationTimeout;

  protected int rpcTimeout;

  protected int readRpcTimeout;

  protected int writeRpcTimeout;

  TableBuilderBase(TableName tableName, ConnectionConfiguration connConf) {
    if (tableName == null) {
      throw new IllegalArgumentException("Given table name is null");
    }
    this.tableName = tableName;
    this.operationTimeout = tableName.isSystemTable() ? connConf.getMetaOperationTimeout()
        : connConf.getOperationTimeout();
    this.rpcTimeout = connConf.getRpcTimeout();
    this.readRpcTimeout = connConf.getReadRpcTimeout();
    this.writeRpcTimeout = connConf.getWriteRpcTimeout();
  }

  @Override
  public TableBuilderBase setOperationTimeout(int timeout) {
    this.operationTimeout = timeout;
    return this;
  }

  @Override
  public TableBuilderBase setRpcTimeout(int timeout) {
    this.rpcTimeout = timeout;
    return this;
  }

  @Override
  public TableBuilderBase setReadRpcTimeout(int timeout) {
    this.readRpcTimeout = timeout;
    return this;
  }

  @Override
  public TableBuilderBase setWriteRpcTimeout(int timeout) {
    this.writeRpcTimeout = timeout;
    return this;
  }
}
