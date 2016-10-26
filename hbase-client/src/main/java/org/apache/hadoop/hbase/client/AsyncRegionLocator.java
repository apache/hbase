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

import static org.apache.hadoop.hbase.client.ConnectionUtils.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * TODO: reimplement using aync connection when the scan logic is ready. The current implementation
 * is based on the blocking client.
 */
@InterfaceAudience.Private
class AsyncRegionLocator implements Closeable {

  private final ConnectionImplementation conn;

  AsyncRegionLocator(Configuration conf) throws IOException {
    conn = (ConnectionImplementation) ConnectionFactory.createConnection(conf);
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
      boolean reload) {
    CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
    try {
      future.complete(conn.getRegionLocation(tableName, row, reload));
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  CompletableFuture<HRegionLocation> getPreviousRegionLocation(TableName tableName,
      byte[] startRowOfCurrentRegion, boolean reload) {
    CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
    byte[] toLocateRow = createClosestRowBefore(startRowOfCurrentRegion);
    try {
      for (;;) {
        HRegionLocation loc = conn.getRegionLocation(tableName, toLocateRow, reload);
        byte[] endKey = loc.getRegionInfo().getEndKey();
        if (Bytes.equals(startRowOfCurrentRegion, endKey)) {
          future.complete(loc);
          break;
        }
        toLocateRow = endKey;
      }
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  void updateCachedLocations(TableName tableName, byte[] regionName, byte[] row, Object exception,
      ServerName source) {
    conn.updateCachedLocations(tableName, regionName, row, exception, source);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(conn);
  }
}
