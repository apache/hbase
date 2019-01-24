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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Wraps a Connection to make it can't be closed or aborted.
 */
@InterfaceAudience.Private
public class SharedConnection implements Connection {

  private final Connection conn;

  public SharedConnection(Connection conn) {
    this.conn = conn;
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Shared connection");
  }

  @Override
  public boolean isClosed() {
    return this.conn.isClosed();
  }

  @Override
  public void abort(String why, Throwable e) {
    throw new UnsupportedOperationException("Shared connection");
  }

  @Override
  public boolean isAborted() {
    return this.conn.isAborted();
  }

  @Override
  public Configuration getConfiguration() {
    return this.conn.getConfiguration();
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return this.conn.getBufferedMutator(tableName);
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    return this.conn.getBufferedMutator(params);
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return this.conn.getRegionLocator(tableName);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return this.conn.getAdmin();
  }

  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
    return this.conn.getTableBuilder(tableName, pool);
  }

  @Override
  public void clearRegionLocationCache() {
    conn.clearRegionLocationCache();
  }

  @Override
  public Hbck getHbck() throws IOException {
    return conn.getHbck();
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    return conn.getHbck(masterServer);
  }
}
