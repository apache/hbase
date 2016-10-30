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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;

/**
 * The Write Ahead Log (WAL) stores all durable edits to the HRegion.
 * This interface provides the entry point for all WAL implementors.
 * <p>
 * See {@link FSHLogProvider} for an example implementation.
 *
 * A single WALProvider will be used for retrieving multiple WALs in a particular region server
 * and must be threadsafe.
 */
@InterfaceAudience.Private
public interface WALProvider {

  /**
   * Set up the provider to create wals.
   * will only be called once per instance.
   * @param factory factory that made us may not be null
   * @param conf may not be null
   * @param listeners may be null
   * @param providerId differentiate between providers from one factory. may be null
   */
  void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, final String providerId) throws IOException;

  /**
   * @param identifier may not be null. contents will not be altered.
   * @param namespace could be null, and will use default namespace if null
   * @return a WAL for writing entries for the given region.
   */
  WAL getWAL(final byte[] identifier, byte[] namespace) throws IOException;

  /** @return the List of WALs that are used by this server
   */
  List<WAL> getWALs() throws IOException;

  /**
   * persist outstanding WALs to storage and stop accepting new appends.
   * This method serves as shorthand for sending a sync to every WAL provided by a given
   * implementation. Those WALs will also stop accepting new writes.
   */
  void shutdown() throws IOException;

  /**
   * shutdown utstanding WALs and clean up any persisted state.
   * Call this method only when you will not need to replay any of the edits to the WALs from
   * this provider. After this call completes, the underlying resources should have been reclaimed.
   */
  void close() throws IOException;

  // Writers are used internally. Users outside of the WAL should be relying on the
  // interface provided by WAL.
  interface Writer extends Closeable {
    void sync() throws IOException;
    void append(WAL.Entry entry) throws IOException;
    long getLength() throws IOException;
  }

  interface AsyncWriter extends Closeable {
    CompletableFuture<Long> sync();
    void append(WAL.Entry entry);
    long getLength();
  }

  /**
   * Get number of the log files this provider is managing
   */
  long getNumLogFiles();

  /**
   * Get size of the log files this provider is managing
   */
  long getLogFileSize();

}
