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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Wraps a {@link AsyncConnection} to make it can't be closed.
 */
@InterfaceAudience.Private
public class SharedAsyncConnection implements AsyncConnection {

  private final AsyncConnection conn;

  public SharedAsyncConnection(AsyncConnection conn) {
    this.conn = conn;
  }

  @Override
  public boolean isClosed() {
    return conn.isClosed();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Shared connection");
  }

  @Override
  public Configuration getConfiguration() {
    return conn.getConfiguration();
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return conn.getRegionLocator(tableName);
  }

  @Override
  public void clearRegionLocationCache() {
    conn.clearRegionLocationCache();
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    return conn.getTableBuilder(tableName);
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(TableName tableName,
      ExecutorService pool) {
    return conn.getTableBuilder(tableName, pool);
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    return conn.getAdminBuilder();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService pool) {
    return conn.getAdminBuilder(pool);
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    return conn.getBufferedMutatorBuilder(tableName);
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName,
      ExecutorService pool) {
    return conn.getBufferedMutatorBuilder(tableName, pool);
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    return conn.getHbck();
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    return conn.getHbck(masterServer);
  }

  @Override
  public Connection toConnection() {
    return new SharedConnection(conn.toConnection());
  }

}
