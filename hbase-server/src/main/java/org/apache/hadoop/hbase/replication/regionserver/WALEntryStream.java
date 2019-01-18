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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALIdentity;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link WALIdentity}, and
 * continually iterates through all the WAL {@link Entry} in the queue. When it's done reading from
 * a Wal, it dequeues it and starts reading from the next.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WALEntryStream extends WAL.Reader {
  /**
   * @return the {@link WALIdentity} of the current WAL
   */
  public WALIdentity getCurrentWalIdentity();

  /**
   * @return true if there is another WAL {@link Entry}
   */
  public boolean hasNext() throws IOException;

  /**
   * Returns the next WAL entry in this stream but does not advance.
   */
  public Entry peek() throws IOException;
}
