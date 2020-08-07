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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsReplicationGlobalSourceSource extends MetricsReplicationSourceSource {

  public static final String SOURCE_WAL_READER_EDITS_BUFFER = "source.walReaderEditsBufferUsage";

  /**
   * Sets the total usage of memory used by edits in memory read from WALs. The memory represented
   * by this usage measure is across peers/sources. For example, we may batch the same WAL edits
   * multiple times for the sake of replicating them to multiple peers..
   * @param usage The memory used by edits in bytes
   */
  void setWALReaderEditsBufferBytes(long usage);

  /**
   * Returns the size, in bytes, of edits held in memory to be replicated across all peers.
   */
  long getWALReaderEditsBufferBytes();
}
