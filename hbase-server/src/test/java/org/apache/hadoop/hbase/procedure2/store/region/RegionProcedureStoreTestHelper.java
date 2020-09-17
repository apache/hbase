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
package org.apache.hadoop.hbase.procedure2.store.region;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureLoader;

final class RegionProcedureStoreTestHelper {

  private RegionProcedureStoreTestHelper() {
  }

  static Server mockServer(Configuration conf) {
    Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(conf);
    when(server.getServerName())
      .thenReturn(ServerName.valueOf("localhost", 12345, System.currentTimeMillis()));
    return server;
  }

  static RegionProcedureStore createStore(Server server, MasterRegion region,
    ProcedureLoader loader) throws IOException {
    RegionProcedureStore store = new RegionProcedureStore(server, region, new LeaseRecovery() {

      @Override
      public void recoverFileLease(FileSystem fs, Path path) throws IOException {
      }
    });
    store.start(1);
    store.recoverLease();
    store.load(loader);
    return store;
  }
}
