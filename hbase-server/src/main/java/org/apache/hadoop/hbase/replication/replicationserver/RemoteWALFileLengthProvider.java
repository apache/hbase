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
package org.apache.hadoop.hbase.replication.replicationserver;

import java.io.IOException;
import java.util.OptionalLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetLogFileSizeIfBeingWrittenRequest;

/**
 * Used by ReplicationServer while Replication offload enabled.
 * On ReplicationServer, we need to know the length of the wal being writing from RegionServer that
 * holds the wal. So achieve that through RPC call.
 */
@InterfaceAudience.Private
public class RemoteWALFileLengthProvider implements WALFileLengthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteWALFileLengthProvider.class);

  private AsyncClusterConnection conn;

  private ServerName rs;

  public RemoteWALFileLengthProvider(AsyncClusterConnection conn, ServerName rs) {
    this.conn = conn;
    this.rs = rs;
  }

  @Override
  public OptionalLong getLogFileSizeIfBeingWritten(Path path) throws IOException {
    AsyncRegionServerAdmin rsAdmin = conn.getRegionServerAdmin(rs);
    GetLogFileSizeIfBeingWrittenRequest request =
      GetLogFileSizeIfBeingWrittenRequest.newBuilder().setWalPath(path.toString()).build();
    try {
      AdminProtos.GetLogFileSizeIfBeingWrittenResponse response =
        FutureUtils.get(rsAdmin.getLogFileSizeIfBeingWritten(request));
      if (response.getIsBeingWritten()) {
        return OptionalLong.of(response.getLength());
      } else {
        return OptionalLong.empty();
      }
    } catch (IOException e) {
      LOG.warn("Exceptionally get the length of wal {} from RS {}", path.getName(), rs);
      throw e;
    }
  }
}
