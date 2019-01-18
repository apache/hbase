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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream;
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
  void init(WALFactory factory, Configuration conf, String providerId) throws IOException;

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
  }

  // Writers are used internally. Users outside of the WAL should be relying on the
  // interface provided by WAL.
  interface Writer extends WriterBase {
    void sync(boolean forceSync) throws IOException;

    void append(WAL.Entry entry) throws IOException;
  }

  interface AsyncWriter extends WriterBase {
    CompletableFuture<Long> sync();

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

  /**
   * Streaming implementation to retrieve WAL entries from given set of Wals. This class is given a
   * queue of WAL
   * @param logQueue Queue of wals
   * @param conf configuration
   * @param startPosition start position for the first wal in the queue
   * @param serverName name of the server
   * @param metrics metric source
   * @return WALEntryStream
   * @throws IOException IOException
   */
  WALEntryStream getWalStream(PriorityBlockingQueue<WALIdentity> logQueue, Configuration conf,
      long startPosition, ServerName serverName, MetricsSource metrics) throws IOException;

  /**
   * Create wal identity wrapper for wal Name
   * @param serverName regionserver name
   * @param walName Name of the wal
   * @param isArchive where it is archived
   * @return WALIdentity
   */
  WALIdentity createWalIdentity(ServerName serverName, String walName, boolean isArchive);

  /**
   * Get the updated walId if it is moved after the server is dead
   * @param wal original walId
   * @param server server which is dead
   * @param deadRegionServers list of dead region servers
   * @return updated walId after relocation
   * @throws IOException IOException
   */
  WALIdentity locateWalId(WALIdentity wal, Server server,
      List<ServerName> deadRegionServers) throws IOException;
}
