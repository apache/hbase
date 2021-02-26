/**
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
package org.apache.hadoop.hbase.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The Write Ahead Log (WAL) stores all durable edits to the HRegion. This interface provides the
 * entry point for all WAL implementors.
 * <p>
 * See {@link FSHLogProvider} for an example implementation. A single WALProvider will be used for
 * retrieving multiple WALs in a particular region server and must be threadsafe.
 */
@InterfaceAudience.Private
public interface WALProvider {

  /**
   * Set up the provider to create wals. will only be called once per instance.
   * @param factory factory that made us may not be null
   * @param conf may not be null
   * @param providerId differentiate between providers from one factory. may be null
   */
  void init(WALFactory factory, Configuration conf, String providerId, Abortable abortable)
      throws IOException;

  /**
   * @param region the region which we want to get a WAL for it. Could be null.
   * @return a WAL for writing entries for the given region.
   */
  WAL getWAL(RegionInfo region) throws IOException;

  /**
   * @return the List of WALs that are used by this server
   */
  List<WAL> getWALs();

  /**
   * persist outstanding WALs to storage and stop accepting new appends. This method serves as
   * shorthand for sending a sync to every WAL provided by a given implementation. Those WALs will
   * also stop accepting new writes.
   */
  void shutdown() throws IOException;

  /**
   * shutdown utstanding WALs and clean up any persisted state. Call this method only when you will
   * not need to replay any of the edits to the WALs from this provider. After this call completes,
   * the underlying resources should have been reclaimed.
   */
  void close() throws IOException;

  interface WriterBase extends Closeable {
    long getLength();
    /**
     * NOTE: We add this method for {@link WALFileLengthProvider} used for replication,
     * considering the case if we use {@link AsyncFSWAL},we write to 3 DNs concurrently,
     * according to the visibility guarantee of HDFS, the data will be available immediately
     * when arriving at DN since all the DNs will be considered as the last one in pipeline.
     * This means replication may read uncommitted data and replicate it to the remote cluster
     * and cause data inconsistency.
     * The method {@link WriterBase#getLength} may return length which just in hdfs client
     * buffer and not successfully synced to HDFS, so we use this method to return the length
     * successfully synced to HDFS and replication thread could only read writing WAL file
     * limited by this length.
     * see also HBASE-14004 and this document for more details:
     * https://docs.google.com/document/d/11AyWtGhItQs6vsLRIx32PwTxmBY3libXwGXI25obVEY/edit#
     * @return byteSize successfully synced to underlying filesystem.
     */
    long getSyncedLength();
  }

  // Writers are used internally. Users outside of the WAL should be relying on the
  // interface provided by WAL.
  interface Writer extends WriterBase {
    void sync(boolean forceSync) throws IOException;

    void append(WAL.Entry entry) throws IOException;
  }

  interface AsyncWriter extends WriterBase {
    CompletableFuture<Long> sync(boolean forceSync);

    void append(WAL.Entry entry);
  }

  /**
   * Get number of the log files this provider is managing
   */
  long getNumLogFiles();

  /**
   * Get size of the log files this provider is managing
   */
  long getLogFileSize();

  /**
   * Add a {@link WALActionsListener}.
   * <p>
   * Notice that you must call this method before calling {@link #getWAL(RegionInfo)} as this method
   * will not effect the {@link WAL} which has already been created. And as long as we can only it
   * when initialization, it is not thread safe.
   */
  void addWALActionsListener(WALActionsListener listener);

  default WALFileLengthProvider getWALFileLengthProvider() {
    return path -> getWALs().stream().map(w -> w.getLogFileSizeIfBeingWritten(path))
        .filter(o -> o.isPresent()).findAny().orElse(OptionalLong.empty());
  }
}
