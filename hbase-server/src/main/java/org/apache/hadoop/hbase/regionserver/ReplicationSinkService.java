/*
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;

/**
 * A sink for a replication stream has to expose this service.
 * This service allows an application to hook into the
 * regionserver and behave as a replication sink.
 */
@InterfaceAudience.Private
public interface ReplicationSinkService extends ReplicationService {
  /**
   * Carry on the list of log entries down to the sink
   * @param entries list of WALEntries to replicate
   * @param cells Cells that the WALEntries refer to (if cells is non-null)
   * @param replicationClusterId Id which will uniquely identify source cluster FS client
   *          configurations in the replication configuration directory
   * @param sourceBaseNamespaceDirPath Path that point to the source cluster base namespace
   *          directory required for replicating hfiles
   * @param sourceHFileArchiveDirPath Path that point to the source cluster hfile archive directory
   * @throws IOException
   */
  void replicateLogEntries(List<WALEntry> entries, CellScanner cells, String replicationClusterId,
      String sourceBaseNamespaceDirPath, String sourceHFileArchiveDirPath) throws IOException;
}
